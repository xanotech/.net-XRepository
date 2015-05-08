using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Web;
using XTools;

namespace XRepository {
    using IRecord = IDictionary<string, object>;
    using Record = Dictionary<string, object>;

    public class NRepository : RepositoryBase {

        protected const BindingFlags CaseInsensitiveBinding =
            BindingFlags.IgnoreCase | BindingFlags.Instance | BindingFlags.Public;

        private static Cache<string, TypeInfoCache> infoCache = new Cache<string, TypeInfoCache>(s => new TypeInfoCache());

        protected static Cache<Type, Mirror> mirrorCache = new Cache<Type, Mirror>(t => new Mirror(t));



        /// <summary>
        ///   Used exclusively when retrieving objects from the database for the purpose of
        ///   setting references values (SetReferences) and only exists for the current thread.
        /// </summary>
        private IDictionary<Type, IDictionary<object, object>> idObjectMap;

        private IDictionary<Type, IDictionary<object, object>> joinObjectMap;




        public NRepository(Func<IDbConnection> openConnection) {
            CreationStack = new StackTrace(true).ToString();
            OpenConnection = openConnection;
            IsReferenceAssignmentActive = true;
            IsUsingLikeForEquals = false;
        } // end constructor



        public NRepository(string connectionStringName) :
            this(() => { return DataTool.OpenConnection(connectionStringName); }) {
            ConnectionString = connectionStringName;
        } // end constructor



        private object CastToTypedEnumerable(IEnumerable enumerable, Type type) {
            var mirror = mirrorCache[typeof(Enumerable)];
            var castMethod = mirror.GetMethod("Cast", new[] {typeof(IEnumerable)});
            castMethod = castMethod.MakeGenericMethod(new[] {type});
            return castMethod.Invoke(null, new object[] {enumerable});
        } // end method



        public long Count<T>() where T : new() {
            return Count<T>((IEnumerable<Criterion>)null);
        } // end method



        public long Count<T>(IEnumerable<Criterion> criteria) where T : new() {
            if (criteria != null) {
                var type = typeof(T);
                foreach (var criterion in criteria)
                    criterion.Name = GetMappedColumn(type, criterion.Name);
            } // end if
            try {
                var tableNames = GetTableNames(typeof(T));
                return Count(tableNames, criteria);
            } finally {
                Executor.Dispose();
            } // end try-finally
        } // end method



        public long Count<T>(object criteria) where T : new() {
            if (criteria == null || criteria.GetType().IsBasic()) try {
                var type = typeof(T);
                var idProperty = GetIdProperty(type);
                if (idProperty == null) {
                    var value = "" + criteria;
                    if (type == typeof(string) || type == typeof(DateTime) || type == typeof(DateTime?))
                        value = '"' + value + '"';
                    else if (type == typeof(char) || type == typeof(char?))
                        value = "'" + value + "'";
                    throw new DataException("Repository.Count<T>(" + value + ") method cannot be used for " +
                        type.FullName + " because does not have a single column primary key or " +
                        "it doesn't have a property that corresponds to the primary key.");
                } // end if
                return Count<T>(new Criterion(idProperty.Name, "=", criteria));
            } finally {
                Executor.Dispose();
            } // end try-finally

            var criterion = criteria as Criterion;
            if (criterion != null)
                criteria = new[] { criterion };

            var enumerable = criteria as IEnumerable<Criterion> ?? Criterion.Create(criteria);
            return Count<T>(enumerable);
        } // end method



        public long Count<T>(params Criterion[] criteria) where T : new() {
            return Count<T>((IEnumerable<Criterion>)criteria);
        } // end method



        protected virtual long Count(IEnumerable<string> tableNames, IEnumerable<Criterion> criteria) {
            try {
                if (IsUsingLikeForEquals)
                    criteria = SwitchEqualsToLike(criteria);
                InvokeCountInterceptors(tableNames, criteria);
                return Executor.Count(tableNames, criteria);
            } finally {
                Executor.Dispose();
            } // end try-finally
        } // end method



        public static NRepository Create<T>(string connectionString) where T : IDbConnection, new() {
            var repo = new NRepository(() => { return DataTool.OpenConnection<T>(connectionString); });
            repo.ConnectionString = connectionString;
            return repo;
        } // end method



        public static NRepository Create(Func<IDbConnection> openConnection) {
            return new NRepository(openConnection);
        } // end method



        public static NRepository Create(string connectionStringName) {
            return new NRepository(connectionStringName);
        } // end method



        protected IRecord CreateDatabaseRecord(object obj) {
            var type = obj.GetType();
            var tableNames = GetTableNames(type);
            var record = GetValues(obj, tableNames);
            record["_tableNames"] = tableNames;
            return record;
        } // end method



        private object CreateLazyLoadEnumerable(Type type, Criterion criterion, object referencingObject) {
            var lazyLoadType = typeof(LazyLoadEnumerable<>);
            var mirror = mirrorCache[lazyLoadType];
            var lazyLoadGenericType = mirror.MakeGenericType(type);
            var instance = Activator.CreateInstance(lazyLoadGenericType);

            mirror = mirrorCache[lazyLoadGenericType];
            var property = mirror.GetProperty("Criterion");
            property.SetValue(instance, criterion, null);

            property = mirror.GetProperty("ReferencingObject");
            property.SetValue(instance, referencingObject, null);

            property = mirror.GetProperty("Repository");
            property.SetValue(instance, this, null);

            return instance;
        } // end method



        private T CreateObject<T>(IRecord record) where T : new() {
            T obj = new T();
            var type = typeof(T);
            var mirror = mirrorCache[type];
            foreach (var column in record.Keys) {
                var propertyName = GetMappedProperty(type, FormatColumnName(column));
                var prop = mirror.GetProperty(propertyName, CaseInsensitiveBinding);
                if (prop == null)
                    continue;

                var val = record[column];
                if (val != null) {
                    var propMirror = mirrorCache[prop.PropertyType];
                    if (!propMirror.IsAssignableFrom(val.GetType()))
                        val = SystemTool.SmartConvert(val, prop.PropertyType);
                } // end if
                prop.SetValue(obj, val, null);
            } // end for
            return obj;
        } // end method



        private IEnumerable<T> CreateObjects<T>(IEnumerable<IRecord> records,
            CursorData cursorData) where T : new() {
            var objs = new List<T>();
            var recordNum = 0;
            var recordStart = cursorData.skip ?? 0;
            var recordStop = cursorData.limit != null ? recordStart + cursorData.limit : null;

            foreach (var record in records) {
                // Create objects if the pagingMechanism is not Programatic or
                // (if it is) and recordNum is on or after recordStart and
                // before recordStop.  recordStop is null if cursor.limit
                // is null and in that case, use recordNum + 1 so that the record
                // always results in CreateObject (since cursor.limit wasn't specified).
                if (cursorData.pagingMechanism != DatabaseExecutor.PagingMechanism.Programmatic ||
                    recordNum >= recordStart &&
                    recordNum < (recordStop ?? (recordNum + 1)))
                    objs.Add(CreateObject<T>(record));
                    
                recordNum++;

                // Stop iterating if the pagingMechanism is null or Programmatic,
                // recordStop is defined, and recordNum is on or after recordStop.
                if ((cursorData.pagingMechanism == null ||
                    cursorData.pagingMechanism == DatabaseExecutor.PagingMechanism.Programmatic) &&
                    recordStop != null && recordNum >= recordStop)
                    break;
            } // end while
            return objs;
        } // end method



        private Reference CreateReference(PropertyInfo property) {
            // If a property is an Array, if it is "basic", or if
            // it's ready only, then it can't be a reference property.
            if (property.PropertyType.IsArray ||
                property.PropertyType.IsBasic() ||
                property.GetSetMethod() == null)
                return null;

            string prefix;
            Type referencedType, primaryType, foreignType;
            var isMultiple = true;
            if (property.PropertyType.IsGenericType &&
                property.PropertyType.GetGenericTypeDefinition() == typeof(IEnumerable<>)) {
                referencedType = property.PropertyType.GetGenericArguments()[0];
                if (referencedType.IsBasic())
                    return null;

                primaryType = property.DeclaringType;
                foreignType = referencedType;
                prefix = primaryType.Name;
            } else {
                isMultiple = false;
                referencedType = property.PropertyType;
                primaryType = property.PropertyType;
                foreignType = property.DeclaringType;
                prefix = property.Name;
            } // end if

            // Validate the referencedType and make sure that at least
            // one of the tableNames (if it has any) has a SchemaTable
            // demonstrating that it one or more database tables.
            var tableNames = GetTableNames(referencedType);
            if (!tableNames.Any())
                return null;
            //if (tableNames.All(tn => GetSchemaTable(tn) == null))
            //    return null;

            var keyProperty = FindReferenceKeyProperty(prefix, primaryType, foreignType);
            if (keyProperty == null)
                return null;

            var reference = new Reference();
            reference.IsMultiple = isMultiple;
            reference.KeyProperty = keyProperty;
            reference.ValueProperty = property;
            reference.ReferencedType = referencedType;
            return reference;
        } // end method



        protected virtual IEnumerable<T> Fetch<T>(Cursor<T> cursor) where T : new() {
            var type = typeof(T);

            // Clone cursor data and change sort columns to mapped database columns
            var cursorData = cursor.CursorData.Clone();
            var newSort = new Dictionary<string, int>();
            if (cursorData.sort != null)
                foreach (var key in cursorData.sort.Keys)
                    newSort[GetMappedColumn(type, key)] = cursorData.sort[key];
            cursorData.sort = newSort;

            var tableNames = GetTableNames(typeof(T));
            InvokeFindInterceptors(tableNames, cursorData.criteria);
            var objects = new BlockingCollection<IRecord>();
            Executor.Fetch(tableNames, cursorData, objects);
            return CreateObjects<T>(objects, cursorData);
        } // end method



        protected IEnumerable<T> Fetch<T>(Cursor<T> cursor, IEnumerable[] joinObjects)
            where T : new() {
            bool wasNull = idObjectMap == null;
            if (wasNull)
                idObjectMap = new Dictionary<Type, IDictionary<object, object>>();

            try {
                if (IsUsingLikeForEquals)
                    cursor.CursorData.criteria = SwitchEqualsToLike(cursor.CursorData.criteria);

                var objs = Fetch(cursor);

                foreach (var obj in objs) {
                    var id = GetId(obj);
                    if (id != null)
                        MapObject(id, obj, idObjectMap);
                } // end foreach

                if (joinObjectMap == null)
                    InitObjectMaps(joinObjects);
                SetReferences(objs);

                return objs;
            } finally {
                if (wasNull) {
                    idObjectMap = null;
                    joinObjectMap = null;
                } // end if

                Executor.Dispose();
            } // end try-finally
        } // end method



        public Cursor<T> Find<T>() where T : new() {
            return Find<T>((IEnumerable<Criterion>)null);
        } // end method



        public Cursor<T> Find<T>(IEnumerable<Criterion> criteria) where T : new() {
            if (criteria != null) {
                var type = typeof(T);
                foreach (var criterion in criteria)
                    criterion.Name = GetMappedColumn(type, criterion.Name);
            } // end if
            return new Cursor<T>(criteria, Fetch, this);
        } // end method



        public Cursor<T> Find<T>(object criteria) where T : new() {
            if (criteria == null || criteria.GetType().IsBasic()) try {
                var type = typeof(T);
                var idProperty = GetIdProperty(type);
                if (idProperty == null) {
                    var value = "" + criteria;
                    if (type == typeof(string) || type == typeof(DateTime) || type == typeof(DateTime?))
                        value = '"' + value + '"';
                    else if (type == typeof(char) || type == typeof(char?))
                        value = "'" + value + "'";
                    throw new DataException("Repository.Find<T>(" + value + ") method cannot be used for " +
                        type.FullName + " because does not have a single column primary key or " +
                        "it doesn't have a property that corresponds to the primary key.");
                } // end if
                return Find<T>(new Criterion(idProperty.Name, "=", criteria));
            } finally {
                Executor.Dispose();
            } // end try-finally

            var criterion = criteria as Criterion;
            if (criterion != null)
                criteria = new[] {criterion};

            var enumerable = criteria as IEnumerable<Criterion> ?? Criterion.Create(criteria);
            return Find<T>(enumerable);
        } // end method



        public Cursor<T> Find<T>(params Criterion[] criteria) where T : new() {
            return Find<T>((IEnumerable<Criterion>)criteria);
        } // end method



        private PropertyInfo FindIdProperty(Type type) {
            var keys = GetPrimaryKeys(type);
            if (keys.Count() != 1)
                return null;

            var mirror = mirrorCache[type];
            var propertyName = GetMappedProperty(type, keys.FirstOrDefault());
            return mirror.GetProperty(propertyName, CaseInsensitiveBinding);
        } // end method



        public T FindOne<T>() where T : new() {
            return Find<T>().Limit(1).FirstOrDefault();
        } // end method



        public T FindOne<T>(IEnumerable<Criterion> criteria) where T : new() {
            return Find<T>(criteria).Limit(1).FirstOrDefault();
        } // end method



        public T FindOne<T>(object criteria) where T : new() {
            return Find<T>(criteria).Limit(1).FirstOrDefault();
        } // end method



        public T FindOne<T>(params Criterion[] criteria) where T : new() {
            return Find<T>(criteria).Limit(1).FirstOrDefault();
        } // end method



        private void FindReferenceIds(IEnumerable objs, IEnumerable<Reference> references) {
            if (!IsReferenceAssignmentActive)
                return;

            var idsToFind = new Dictionary<Type, IList<object>>();
            foreach (var obj in objs)
            foreach (var reference in references)
            if (reference.IsSingle) {
                var id = reference.KeyProperty.GetValue(obj, null);
                if (id == null)
                    continue;

                if (!idObjectMap.ContainsKey(reference.ReferencedType))
                    idObjectMap[reference.ReferencedType] = new Dictionary<object, object>();

                if (!idObjectMap[reference.ReferencedType].ContainsKey(id)) {
                    if (!idsToFind.ContainsKey(reference.ReferencedType))
                        idsToFind[reference.ReferencedType] = new List<object>();
                    if (!idsToFind[reference.ReferencedType].Contains(id))
                        idsToFind[reference.ReferencedType].Add(id);
                } // end if
            } // end if

            foreach (var type in idsToFind.Keys) {
                var idProperty = GetIdProperty(type);
                if (idProperty == null)
                    continue;

                var criterion = new Criterion(idProperty.Name, "=", idsToFind[type]);
                var results = ReflectedFind(type, new[] {criterion});
                // Retrieves results (which loads them into idObjectMap)
                (results as IEnumerable).GetEnumerator();
            } // end foreach
        } // end method



        private PropertyInfo FindReferenceKeyProperty(string prefix, Type primaryType, Type foreignType) {
            var keys = GetPrimaryKeys(primaryType);
            if (keys.Count() != 1)
                return null;

            var tableNames = GetTableNames(primaryType);
            var keyName = keys.First();
            foreach (var name in tableNames)
                keyName = keyName.RemoveIgnoreCase(name);
            keyName = prefix + keyName;

            var mirror = mirrorCache[foreignType];
            var keyProp = mirror.GetProperty(keyName, CaseInsensitiveBinding);
            if (keyProp == null) {
                keyName = keys.First();
                keys = GetPrimaryKeys(foreignType);
                if (!(keys.Count() == 1 && keys.First().Is(keyName)))
                    keyProp = mirror.GetProperty(keyName, CaseInsensitiveBinding);
            } // end if
            return keyProp;
        } // end method



        private IEnumerable<Reference> FindReferences(Type type) {
            var references = new List<Reference>();
            var mirror = mirrorCache[type];
            var properties = mirror.GetProperties();
            foreach (var property in properties) {
                var reference = CreateReference(property);
                if (reference != null)
                    references.Add(reference);
            } // end foreach
            return references;
        } // end method



        private IEnumerable<string> FindTableNames(Type type) {
            var tableNameList = new List<string>();
            while (type != typeof(object)) {
                var tableDef = Executor.GetTableDefinition(type.Name);
                if (tableDef != null)
                    tableNameList.Add(tableDef.FullName);
                type = type.BaseType;
            } // end while
            tableNameList.Reverse();
            return tableNameList;
        } // end method



        private static string FormatColumnName(string name) {
            // Extract the name from the reader's schema.  Fields with
            // the same name in multiple tables selected (usually the
            // primary key) will be preceded by the table name and a ".".
            // For instance, if Employee extends from Person, the names
            // "Person.Id" and "Employee.Id" will be column names.
            // Strip the preceding table name and ".".
            var index = name.LastIndexOf('.');
            if (index > -1)
                name = name.Substring(index + 1);
            return name;
        } // end method



        private object GetId(object obj) {
            PropertyInfo idProperty;
            return GetId(obj, out idProperty);
        } // end method



        private object GetId(object obj, out PropertyInfo idProperty) {
            idProperty = GetIdProperty(obj.GetType());
            if (idProperty == null)
                return null;
            return idProperty.GetValue(obj, null);
        } // end method



        public PropertyInfo GetIdProperty(Type type) {
            if (type == null)
                throw new ArgumentNullException("type", "The type parameter was null.");

            var info = infoCache[ConnectionString];
            return info.idPropertyCache.GetValue(type, FindIdProperty);
        } // end method



        private long? GetIntId(object obj) {
            return GetId(obj) as long?;
        } // end method



        private long? GetIntId(object obj, out PropertyInfo idProperty) {
            var id = GetId(obj, out idProperty) as long?;
            if (idProperty != null &&
                !idProperty.PropertyType.IsInteger())
                idProperty = null;
            return id;
        } // end method



        protected string GetMappedColumn(Type type, string propertyName) {
            var info = infoCache[ConnectionString];
            while (type != typeof(object)) {
                foreach (var mapping in info.columnMapCache[type])
                    if (mapping.Value == propertyName)
                        return mapping.Key;
                type = type.BaseType;
            } // end while
            return propertyName;
        } // end method



        protected string GetMappedProperty(Type type, string column) {
            var info = infoCache[ConnectionString];
            while (type != typeof(object)) {
                if (info.columnMapCache[type].ContainsKey(column))
                    return info.columnMapCache[type][column];
                type = type.BaseType;
            } // end while
            return column;
        } // end method



        public virtual IEnumerable<string> GetPrimaryKeys(Type type) {
            if (type == null)
                throw new ArgumentNullException("type", "The type parameter was null.");

            var tableNames = GetTableNames(type);
            if (!tableNames.Any())
                return new string[0];

            var keys = Executor.GetPrimaryKeys(tableNames.Last());
            keys = keys.Select(k => GetMappedProperty(type, k));
            return keys;
        } // end method



        internal IEnumerable<Reference> GetReferences(Type type) {
            if (type == null)
                throw new ArgumentNullException("type", "The type parameter was null.");

            var info = infoCache[ConnectionString];
            return info.referencesCache.GetValue(type, FindReferences);
        } // end method



        public IEnumerable<string> GetTableNames(Type type, bool isSilent = false) {
            if (type == null)
                throw new ArgumentNullException("type", "The type parameter was null.");

            var info = infoCache[ConnectionString];
            var tableNames = info.tableNamesCache.GetValue(type, FindTableNames);
            if (!tableNames.Any() && !isSilent)
                throw new DataException("There are no tables associated with \"" + type.FullName + "\".");
            return tableNames;
        } // end method



        protected IRecord GetValues(object obj, IEnumerable<string> tableNames) {
            var type = obj.GetType();
            var mirror = mirrorCache[type];
            var values = new Record(StringComparer.OrdinalIgnoreCase);
            foreach (var tableName in tableNames) {
                var columns = Executor.GetColumns(tableName);
                foreach (var column in columns) {
                    if (values.ContainsKey(column))
                        continue;

                    var propertyName = GetMappedProperty(type, column);
                    var prop = mirror.GetProperty(propertyName, CaseInsensitiveBinding);
                    if (prop != null)
                        values[column] = prop.GetValue(obj, null);
                } // end for
            } // end foreach
            return values;
        } // end method



        private void InitObjectMaps(IEnumerable[] joinObjects) {
            if (joinObjectMap != null)
                return;

            joinObjectMap = new Dictionary<Type, IDictionary<object, object>>();

            if (joinObjects == null)
                return;

            foreach (var joinObjEnum in joinObjects)
            foreach (var obj in joinObjEnum) {
                var id = GetId(obj);
                if (id != null)
                    MapObject(id, obj, idObjectMap, joinObjectMap);
            } // end foreach
        } // end method



        protected bool IsIdNeeded(object obj, string baseTableName) {
            if (Sequencer == null)
                return false;

            PropertyInfo idProperty;
            var id = GetIntId(obj, out idProperty);

            return idProperty != null &&
                (id == null || (id == 0 && !idProperty.PropertyType.IsNullable()));
        } // end method



        public bool IsReferenceAssignmentActive { get; set; }



        public bool IsUsingLikeForEquals { get; set; }



        private bool IsValidColumn(Type type, string columnName) {
            var tableNames = GetTableNames(type);
            foreach (var tableName in tableNames) {
                var columns = Executor.GetColumns(tableName);
                foreach (var column in columns)
                    if (column.Is(columnName))
                        return true;
            } // end foreach
            return false;
        } // end method



        private void MapObject(object id, object obj,
            params IDictionary<Type, IDictionary<object, object>>[] maps) {
            var type = obj.GetType();
            if (!GetTableNames(type).Any())
                return;

            while (type != typeof(object)) {
                foreach (var map in maps) {
                    if (!map.ContainsKey(type))
                        map[type] = new Dictionary<object, object>();
                    map[type][id] = obj;
                } // end foreach
                type = type.BaseType;
            } // end while
        } // end method



        public void MapColumn<T>(string propertyName, string columnName) {
            MapColumn(typeof(T), propertyName, columnName);
        } // end method



        public void MapColumn(Type type, string propertyName, string columnName) {
            if (type == null)
                throw new ArgumentNullException("type", "The type parameter was null.");
            if (propertyName == null)
                throw new ArgumentNullException("propertyName", "The propertyName parameter was null.");
            if (columnName == null)
                throw new ArgumentNullException("columnName", "The columnName parameter was null.");

            var mirror = mirrorCache[type];
            var prop = mirror.GetProperty(propertyName, CaseInsensitiveBinding);
            if (prop == null)
                throw new MissingMemberException(type.FullName, propertyName);

            if (!IsValidColumn(type, columnName))
                throw new DataException("The columnName \"" + columnName + "\" does not exist.");
            
            var info = infoCache[ConnectionString];
            info.columnMapCache[type][columnName] = propertyName;
        } // end method



        public void MapTable<T>(string tableName) {
            MapTable(typeof(T), tableName);
        } // end method



        public void MapTable(Type type, string tableName) {
            if (type == null)
                throw new ArgumentNullException("type", "The type parameter was null.");
            if (tableName == null)
                throw new ArgumentNullException("tableName", "The tableName parameter was null.");

            var tableDef = Executor.GetTableDefinition(tableName);
            if (tableDef == null)
                throw new DataException("The table \"" + tableName + "\" is not a valid table.");

            var info = infoCache[ConnectionString];
            var tableNames = new List<string>();
            var baseType = type.BaseType;
            if (baseType != typeof(object))
                tableNames.AddRange(GetTableNames(baseType, true));
            tableNames.Add(tableName);
            info.tableNamesCache.PutValue(type, tableNames);
        } // end method



        public int MaxParameters {
            get {
                var dbExec = Executor as DatabaseExecutor;
                if (dbExec == null)
                    return default(int);

                return dbExec.MaxParameters;
            } // end get
            set {
                var dbExec = Executor as DatabaseExecutor;
                if (dbExec != null && value != default(int))
                    dbExec.MaxParameters = value;
            } // end set
        } // end property



        private object ReflectedFind(Type type, object criteria) {
            var mirror = mirrorCache[GetType()];
            var method = mirror.GetMethod("Find", new[] {typeof(object)});
            method = method.MakeGenericMethod(type);
            return method.Invoke(this, new[] {criteria});
        } // end method



        public void Remove(object obj) {
            var enumerable = obj as IEnumerable;
            if (enumerable == null)
                enumerable = new[] {obj};
            RemoveRange(enumerable);
        } // end method



        protected virtual void RemoveRange(IEnumerable enumerable) {
            try {
                var records = new BlockingCollection<IRecord>();
                foreach (var obj in enumerable) {
                    var record = CreateDatabaseRecord(obj);
                    var tableNames = record["_tableNames"] as IEnumerable<string>;
                    InvokeRemoveInterceptors(tableNames, record);
                    records.Add(record);
                } // end foreach
                Executor.Remove(records);
            } finally {
                Executor.Dispose();
            } // end try-finally
        } // end method



        public void Save(object obj) {
            var enumerable = obj as IEnumerable;
            if (enumerable == null)
                enumerable = new[] { obj };
            SaveRange(enumerable);
        } // end method



        protected virtual void SaveRange(IEnumerable enumerable) {
            try {
                var count = enumerable.Count();
                var baseTableNameIndex = new string[count];
                var isIdNeededIndex = new bool[count];
                var idsNeededByTableName = new Dictionary<string, int>();
                var index = 0;
                foreach (var obj in enumerable) {
                    var baseTableName = GetTableNames(obj.GetType()).First();
                    baseTableNameIndex[index] = baseTableName;

                    var isIdNeeded = IsIdNeeded(obj, baseTableName);
                    isIdNeededIndex[index] = isIdNeeded;
                    if (isIdNeeded) {
                        if (!idsNeededByTableName.ContainsKey(baseTableName))
                            idsNeededByTableName[baseTableName] = 0;
                        idsNeededByTableName[baseTableName]++;
                    } // end if
                
                    index++;
                } // end foreach

                var idMap = new Dictionary<string, long>();
                foreach (var tableName in idsNeededByTableName.Keys)
                    idMap[tableName] = Sequencer.GetNextValues(tableName,
                        Executor.GetPrimaryKeys(tableName).First(), idsNeededByTableName[tableName]);

                var records = new BlockingCollection<IRecord>();
                index = 0;
                foreach (var obj in enumerable) {
                    if (isIdNeededIndex[index]) {
                        var baseTableName = baseTableNameIndex[index];
                        var id = idMap[baseTableName];
                        var idProperty = GetIdProperty(obj.GetType());
                        idProperty.SetValue(obj, id, null);
                        idMap[baseTableName]++;
                    } // end if

                    var record = CreateDatabaseRecord(obj);
                    var tableNames = record["_tableNames"] as IEnumerable<string>;
                    InvokeSaveInterceptors(tableNames, record);
                    records.Add(record);
                    index++;
                } // end foreach

                Executor.Save(records);
            } finally {
                Executor.Dispose();
            } // end try-finally
        } // end method



        private void SetMultipleReferences(IEnumerable objs, IEnumerable<Reference> references) {
            // This funky collection holds enumerables (ILists) keyed by Type and KeyProperty value == id
            var joinEnumerables = new Dictionary<Type, IDictionary<object, IList<object>>>();

            foreach (var obj in objs)
            foreach (var reference in references)
            if (reference.IsMultiple) {
                var id = GetId(obj);
                if (id == null)
                    continue;

                if (joinObjectMap.ContainsKey(reference.ReferencedType)) {
                    if (!joinEnumerables.ContainsKey(reference.ReferencedType))
                        foreach (var joinObj in joinObjectMap[reference.ReferencedType].Values) {
                            var keyValue = reference.KeyProperty.GetValue(joinObj, null);
                            if (!joinEnumerables.ContainsKey(reference.ReferencedType))
                                joinEnumerables[reference.ReferencedType] = new Dictionary<object, IList<object>>();
                            if (!joinEnumerables[reference.ReferencedType].ContainsKey(keyValue))
                                joinEnumerables[reference.ReferencedType][keyValue] = new List<object>();
                            joinEnumerables[reference.ReferencedType][keyValue].Add(joinObj);
                        } // end foreach

                    if (!joinEnumerables[reference.ReferencedType].ContainsKey(id))
                        joinEnumerables[reference.ReferencedType][id] = new List<object>();

                    var enumerable = CastToTypedEnumerable(joinEnumerables[reference.ReferencedType][id], reference.ReferencedType);
                    reference.ValueProperty.SetValue(obj, enumerable, null);
                } else if (IsReferenceAssignmentActive) {
                    Criterion criterion = new Criterion(reference.KeyProperty.Name, "=", id);
                    var enumerable = CreateLazyLoadEnumerable(reference.ReferencedType, criterion, obj);
                    reference.ValueProperty.SetValue(obj, enumerable, null);
                } // end if-else
            } // end if
        } // end method



        private void SetSingleReferences(IEnumerable objs, IEnumerable<Reference> references) {
            foreach (var obj in objs)
            foreach (var reference in references)
            if (reference.IsSingle) {
                var id = reference.KeyProperty.GetValue(obj, null);
                if (id == null)
                    continue;

                if (idObjectMap[reference.ReferencedType].ContainsKey(id)) {
                    object referencedObj = idObjectMap[reference.ReferencedType][id];
                    reference.ValueProperty.SetValue(obj, referencedObj, null);
                } // end if
            } // end for-each
        } // end method



        private void SetReferences(IEnumerable objs) {
            var type = objs.GetType().GetGenericArguments()[0];
            var references = GetReferences(type);
            SetMultipleReferences(objs, references);
            FindReferenceIds(objs, references);
            SetSingleReferences(objs, references);
        } // end method



        private static IEnumerable<Criterion> SwitchEqualsToLike(IEnumerable<Criterion> criteria) {
            if (criteria == null)
                return null;

            var switchedCriteria = new List<Criterion>();
            foreach (var criterion in criteria) {
                var switchedCriterion = new Criterion(criterion.Name,
                    criterion.Operation, criterion.Value);
                if (switchedCriterion.Operation == Criterion.OperationType.EqualTo)
                    switchedCriterion.Operation = Criterion.OperationType.Like;
                if (switchedCriterion.Operation == Criterion.OperationType.NotEqualTo)
                    switchedCriterion.Operation = Criterion.OperationType.NotLike;
                switchedCriteria.Add(switchedCriterion);
            } // end foreach
            return switchedCriteria;
        } // end method

    } // end class
} // end namespace