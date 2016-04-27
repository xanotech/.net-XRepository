using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.Linq;
using System.Reflection;
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
            long? count = null;
            ProcessBasicCriteria<T>(criteria, "Count", crit => {
                count = Count<T>(crit);
            });
            if (count != null)
                return count.Value;

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
            foreach (var record in records)
                objs.Add(CreateObject<T>(record));
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

            // Clone cursor data (so the original remains untouched)
            var cursorData = cursor.CursorData.Clone();

            // This next block of code populates columns based
            // on the properties of T and their mapped column name.
            var columns = new HashSet<string>();
            var tableNames = GetTableNames(type);
            var allColumns = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            allColumns.UnionWith(tableNames.SelectMany(tn => Executor.GetColumns(tn)));
            var mirror = mirrorCache[type];
            foreach (var property in mirror.GetProperties()) {
                if (!property.PropertyType.IsBasic())
                    continue;

                var column = GetMappedColumn(type, property.Name);
                if (allColumns.Contains(column))
                    columns.Add(column);
            } // end foreach
            cursorData.columns = columns;

            // Change sort columns to mapped database columns
            var sort = new Dictionary<string, int>();
            if (cursorData.sort != null)
                foreach (var key in cursorData.sort.Keys)
                    sort[GetMappedColumn(type, key)] = cursorData.sort[key];
            cursorData.sort = sort;

            InvokeFindInterceptors(tableNames, cursorData.criteria);
            var records = new BlockingCollection<IRecord>();
            Executor.Fetch(tableNames, cursorData, records);
            InvokeFindCompleteInterceptors(tableNames, records);
            return CreateObjects<T>(records, cursorData);
        } // end method



        protected IEnumerable<T> Fetch<T>(Cursor<T> cursor, IEnumerable<IEnumerable> joinObjects)
            where T : new() {
            bool wasNull = idObjectMap == null;
            if (wasNull) {
                idObjectMap = new Dictionary<Type, IDictionary<object, object>>();
                joinObjectMap = new Dictionary<Type, IDictionary<object, object>>();
            } // end if

            try {
                if (IsUsingLikeForEquals)
                    cursor.CursorData.criteria = SwitchEqualsToLike(cursor.CursorData.criteria);

                var objects = Fetch(cursor);

                MapObjects(objects);

                joinObjects = FetchStringJoins<T>(objects, joinObjects);
                MapJoinObjects(joinObjects);
                SetReferences(objects);

                return objects;
            } finally {
                if (wasNull) {
                    idObjectMap = null;
                    joinObjectMap = null;
                } // end if

                Executor.Dispose();
            } // end try-finally
        } // end method



        private IEnumerable<IEnumerable> FetchStringJoins<T>(IEnumerable<T> objects,
            IEnumerable<IEnumerable> joinObjects) {
            if (joinObjects == null)
                return null;

            var references = GetReferences(typeof(T));

            // Initialize newJoinObjects as all non-string joinObjects.
            var newJoinObjects = joinObjects.Where(jo => (jo as string) == null).ToList();

            // Holds all populated joins that are strings.
            var joinStrings = joinObjects.Select(jo => jo as string)
                .Where(js => !string.IsNullOrEmpty(js)).OrderBy(js => js);
            
            // Holds all "primary" properties mentioned by join strings.
            // Join strings may take the form "PrimaryProperty.SecondarProperty...".
            // The keys of propertyMap are the primary properties.  The values
            // are the joins without the primary portion and will be passed
            // to the find operation for the primary property.
            var propertyMap = new Dictionary<string, IList<string>>();
            foreach (var str in joinStrings) {
                var split = str.Split('.');
                var property = split.First();
                if (!propertyMap.ContainsKey(property))
                    propertyMap[property] = new List<string>();
                var remainder = str.Substring(property.Length);
                
                // If the remainder (what is left after removing the primary property)
                // has any value, remove the first character (which should be a '.').
                if (!string.IsNullOrEmpty(remainder))
                    remainder = remainder.Substring(1);

                // If the remainder still has a value after removing the '.',
                // then its a valid secondary+ property.
                if (!string.IsNullOrEmpty(remainder))
                    propertyMap[property].Add(remainder);
            } // end foreach

            foreach (var property in propertyMap.Keys) {
                var reference = references.FirstOrDefault(r => r.ValueProperty.Name == property);
                if (reference == null)
                    continue;

                var joinsForFind = new List<IEnumerable>(newJoinObjects);
                joinsForFind.AddRange(propertyMap[property]);

                IEnumerable results;
                var criterion = new Criterion();
                criterion.Operation = Criterion.OperationType.EqualTo;
                if (reference.IsMultiple) {
                    criterion.Name = reference.KeyProperty.Name;
                    criterion.Value = objects.Select(o => GetId(o));
                } else {
                    criterion.Name = GetPrimaryKeys(reference.ReferencedType).FirstOrDefault();
                    criterion.Value = objects.Select(o => reference.KeyProperty.GetValue(o, null)).Where(id => id != null);
                } // end if-else
                results = ReflectedFind(reference.ReferencedType, new[] {criterion}, joinsForFind.ToArray()) as IEnumerable;
                results.GetEnumerator(); // Forces fetching
                newJoinObjects.Add(results);
            } // end foreach
            return newJoinObjects;
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
            Cursor<T> cursor = null;
            ProcessBasicCriteria<T>(criteria, "Find", crit => {
                cursor = Find<T>(crit);
            });
            if (cursor != null)
                return cursor;

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
                try {
                    var tableDef = Executor.GetTableDefinition(type.Name);
                    if (tableDef != null)
                        tableNameList.Add(tableDef.FullName);
                } catch (DataException ex) {
                    Trace.WriteLine(ex.ToString());
                    // GetTableDefinition throws DataExceptions when the table
                    // indicated by type.Name does not exist.  In those cases,
                    // catch the exception and move on.  FindTableNames will
                    // throw an exception if no tables are found.
                } // end try-catch
                type = type.BaseType;
            } // end while
            if (!tableNameList.Any())
                throw new DataException("There are no tables associated with \"" + type.FullName + "\".");

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



        public IEnumerable<string> GetTableNames(Type type) {
            if (type == null)
                throw new ArgumentNullException("type", "The type parameter was null.");

            var info = infoCache[ConnectionString];
            return info.tableNamesCache.GetValue(type, FindTableNames);
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



        private void MapJoinObjects(IEnumerable<IEnumerable> joinObjects) {
            if (joinObjects == null)
                return;

            joinObjects = joinObjects.Except(joinObjects.Where(jo => (jo as string) != null));

            foreach (var ienum in joinObjects) {
                MapObjects(ienum);
                MapObjects(ienum, joinObjectMap);
            } // end foreach
        } // end method



        private void MapObject(object id, object obj,
            IDictionary<Type, IDictionary<object, object>> map = null) {
            var type = obj.GetType();
            if (!GetTableNames(type).Any())
                return;

            if (map == null)
                map = idObjectMap;

            while (type != typeof(object)) {
                if (!map.ContainsKey(type))
                    map[type] = new Dictionary<object, object>();
                map[type][id] = obj;
                type = type.BaseType;
            } // end while
        } // end method



        private void MapObjects(IEnumerable objects,
            IDictionary<Type, IDictionary<object, object>> map = null) {
            foreach (var obj in objects) {
                var id = GetId(obj);
                if (id != null)
                    MapObject(id, obj, map);
            } // end foreach
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

            // Call GetTableDefinition and if it returns null, throw a DataException.
            // GetTableDefinition should throw an exception, but since it can be
            // overridden, also check for returned null values.
            if (Executor.GetTableDefinition(tableName) == null)
                throw new DataException(Executor.FormatInvalidTableMessage(tableName));

            var info = infoCache[ConnectionString];
            var tableNames = new List<string>();
            var baseType = type.BaseType;

            // Try to add table names associated with the baseType (assuming the
            // baseType isn't Object) to the tableNames to be put in the cache.
            try {
                if (baseType != typeof(object))
                    tableNames.AddRange(GetTableNames(baseType));
            } catch (DataException) {
                // GetTableNames throws an exception if the type passed has no
                // associated tables.  In this case, we can ignore this exception
                // since base types do not need to have backing tables.
            } // end try-catch

            tableNames.Add(tableName);
            info.tableNamesCache.PutValue(type, tableNames);
        } // end method



        public void ProcessBasicCriteria<T>(object criteria, string methodName, Action<Criterion> callback) {
            var basicEnumerable = Criterion.GetBasicEnumerable(criteria);
            if (criteria != null && !criteria.GetType().IsBasic() && basicEnumerable == null)
                return;
            
            try {
                var type = typeof(T);
                var idProperty = GetIdProperty(type);
                if (idProperty == null) {
                    var valueStr = basicEnumerable != null ? "[list-of-values]" : "" + criteria;

                    if (criteria is string || criteria is DateTime || criteria is DateTime?)
                        valueStr = '"' + valueStr + '"';
                    else if (criteria is char || criteria is char?)
                        valueStr = "'" + valueStr + "'";

                    throw new DataException("Repository." + methodName + "<T>(" +
                        valueStr + ") method cannot be used for " + type.FullName +
                        " because does not have a single column primary key or " +
                        "it doesn't have a property that corresponds to the primary key.");
                } // end if
                callback(new Criterion(idProperty.Name, "=", criteria));
            } finally {
                Executor.Dispose();
            } // end try-finally
        } // end method



        private object ReflectedFind(Type type, object criteria) {
            return ReflectedFind(type, criteria, null);
        } // end method



        private object ReflectedFind(Type type, object criteria, IEnumerable[] joinObjects) {
            var mirror = mirrorCache[GetType()];
            var method = mirror.GetMethod("Find", new[] {typeof(object)});
            method = method.MakeGenericMethod(type);
            var cursor = method.Invoke(this, new[] {criteria});
            if (joinObjects != null) {
                mirror = mirrorCache[cursor.GetType()];
                method = mirror.GetMethod("Join", new[] {typeof(IEnumerable[])});
                method.Invoke(cursor, new[] {joinObjects});
            } // end if
            return cursor;
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



        private void SetReferences(IEnumerable objs) {
            var type = objs.GetType().GetGenericArguments()[0];
            var references = GetReferences(type);
            SetMultipleReferences(objs, references);
            FindReferenceIds(objs, references);
            SetSingleReferences(objs, references);
        } // end method



        private void SetSingleReferences(IEnumerable objs, IEnumerable<Reference> references) {
            foreach (var obj in objs)
            foreach (var reference in references)
            if (reference.IsSingle) {
                var id = reference.KeyProperty.GetValue(obj, null);
                if (id == null)
                    continue;

                if (idObjectMap.ContainsKey(reference.ReferencedType) &&
                    idObjectMap[reference.ReferencedType].ContainsKey(id)) {
                    object referencedObj = idObjectMap[reference.ReferencedType][id];
                    reference.ValueProperty.SetValue(obj, referencedObj, null);
                } // end if
            } // end for-each
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