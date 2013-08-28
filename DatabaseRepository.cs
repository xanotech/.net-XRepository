using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.Linq;
using System.Text;
using Xanotech.Tools;

namespace Xanotech.Repository {

    public class DatabaseRepository : IRepository {

        private string connectionName;
        [ThreadStatic]
        private static Cache<Tuple<Type, long?>, object> idCache;
        private Cache<Type, Mirror> mirrorCache;
        private Cache<Type, IEnumerable<Reference>> referenceCache;
        private Cache<string, DataTable> schemaCache;
        private Cache<Type, IEnumerable<string>> tableNameCache;
        private Sequencer sequencer;

        private object readLock;
        private object writeLock;



        public DatabaseRepository(string connection) : this(connection, false, false) {
        } // end constructor



        public DatabaseRepository(string connection,
            bool singleThreadedReads, bool singleThreadedWrites) {
            connectionName = connection;
            mirrorCache = new Cache<Type, Mirror>();
            referenceCache = new Cache<Type, IEnumerable<Reference>>();
            schemaCache = new Cache<string, DataTable>(GetTableSchema);
            tableNameCache = new Cache<Type, IEnumerable<string>>(GetTableNames);
            sequencer = new Sequencer(connection);

            if (singleThreadedReads)
                readLock = new object();
            if (singleThreadedWrites)
                writeLock = new object();
        } // end constructor



        //private static bool ConvertToBool(long val) {
        //    return val == 1;
        //} // end method



        //private static bool? ConvertToBoolQ(long val) {
        //    bool? result = null;
        //    if (val == 0)
        //        result = false;
        //    else if (val == 1)
        //        result = true;
        //    return result;
        //} // end method



        //private static DateTime? ConvertToDateTimeQ(string val) {
        //    DateTime? result = null;
        //    // First, try to convert according to preferred format.
        //    // If that fails, try a simple parse.
        //    try {
        //        result = DateTime.ParseExact((string)val, "yyyy-MM-dd HH:mm:ss", null);
        //    } catch {
        //        try {
        //            result = DateTime.Parse((string)val);
        //        } catch {
        //        } // end try-catch
        //    } // end try-catch
        //    return result;
        //} // end method



        private static object ConvertValue(object val, Type toType) {
            //if (toType == typeof(DateTime?)) {
            //    if (val is string)
            //        val = ConvertToDateTimeQ((string)val);
            //} else if (toType == typeof(bool)) {
            //    if (val is sbyte)
            //        val = ConvertToBool((sbyte)val);
            //    else if (val is long)
            //        val = ConvertToBool((long)val);
            //} else if (toType == typeof(bool?)) {
            //    if (val is sbyte)
            //        val = ConvertToBoolQ((sbyte)val);
            //    else if (val is long)
            //        val = ConvertToBoolQ((long)val);
            //} // end if-else

            if (typeof(DateTime).IsAssignableFrom(toType))
                val = Convert.ToDateTime(val);
            else if (typeof(bool).IsAssignableFrom(toType))
                val = Convert.ToBoolean(val);

            return val;
        } // end method



        private object CreateLazyLoadEnumerable(Type type, Criterion criterion) {
            var lazyLoadType = typeof(LazyLoadEnumerable<>);
            var mirror = mirrorCache.GetValue(lazyLoadType, () => new Mirror(lazyLoadType));
            var lazyLoadGenericType = mirror.MakeGenericType(type);
            var instance = Activator.CreateInstance(lazyLoadGenericType);

            mirror = mirrorCache.GetValue(lazyLoadGenericType, () => new Mirror(lazyLoadGenericType));
            var property = mirror.GetProperty("Criterion");
            property.SetValue(instance, criterion, null);

            property = mirror.GetProperty("Repository");
            property.SetValue(instance, this, null);

            return instance;
        } // end method



        private T CreateObject<T>(IDataReader dr) where T : new() {
            T obj = new T();
            var type = typeof(T);
            var typeMirror = mirrorCache.GetValue(type, () => new Mirror(type));
            var schema = dr.GetSchemaTable();
            for (var i = 0; i < dr.FieldCount; i++) {
                var name = (string)schema.Rows[i][0];
                name = FormatColumnName(name);

                var val = dr.GetValue(i);
                var valType = val.GetType();

                var prop = typeMirror.GetProperty(name);

                if (prop == null)
                    continue;

                // DBNull objects are turned into "null" and all other values
                // are converted to the PropertyType if the valType cannt be
                // assigned to the PropertyType
                if (valType == typeof(DBNull))
                    val = null;
                else {
                    var propMirror = mirrorCache.GetValue(prop.PropertyType, () => new Mirror(prop.PropertyType));
                    if (!propMirror.IsAssignableFrom(valType))
                        val = ConvertValue(val, prop.PropertyType);
                } // end if

                prop.SetValue(obj, val, null);
            } // end for
            return obj;
        } // end method



        private IEnumerable<T> CreateObjects<T>(string connectionName, string sql) where T : new() {
            var objs = new List<T>();
            using (var con = DataTool.OpenConnection(connectionName))
            using (var cmd = con.CreateCommand()) {
                cmd.CommandText = sql;
                Log(sql);
                using (var dr = cmd.ExecuteReader())
                while (dr.Read())
                    //DatabaseObject obj = CreateObject(type);
                    //obj.Broker = this;
                    objs.Add(CreateObject<T>(dr));
            } // end using
            return objs;
        } // end sql



        public void Delete(IIdentifiable obj) {
            //if (obj.Broker == null) obj.Broker = this;
            var tableNames = tableNameCache[obj.GetType()];
            foreach (var table in tableNames)
                DeleteObjectFromTable(obj, table);
        } // end method



        private void DeleteObjectFromTable(IIdentifiable obj, string table) {
            if (RecordExists(table, obj.Id)) {
                var sql = FormatDeleteQuery(table, obj.Id);
                ExecuteWrite(sql);
            } // end if
        } // end method



        private void ExecuteNonQuery(string sql) {
            using (var con = DataTool.OpenConnection(connectionName))
            using (var cmd = con.CreateCommand()) {
                cmd.CommandText = sql;
                Log(sql);
                cmd.ExecuteNonQuery();
            } // end using
        } // end method



        private void ExecuteWrite(string sql) {
            if (writeLock == null)
                ExecuteNonQuery(sql);
            else lock (writeLock)
                ExecuteNonQuery(sql);
        } // end method



        private static string FormatColumnName(string name) {
            // Extract the name from the reader's schema.
            // Fields with the same name in multiple tables
            // selected (usually "Id") will be preceded by the
            // table name and a ".".  For instance, if Employee
            // extends from Person, the names "Person.Id" and
            // "Employee.Id" will be column names.
            // Strip the preceding table name and ".".
            var index = name.LastIndexOf('.');
            if (index > -1)
                name = name.Substring(index + 1);
            return name;
        } // end method



        private static string FormatDeleteQuery(string table, long? id) {
            var sql = new StringBuilder();
            sql.Append("DELETE FROM ");
            sql.Append(table);
            sql.Append(" WHERE Id = ");
            sql.Append(id);
            return sql.ToString();
        } // end method



        private static string FormatInsertQuery(string table, IDictionary<string, string> values) {
            var sql = new StringBuilder();
            sql.Append("INSERT INTO " + table + Environment.NewLine + "(");

            var valueString = new StringBuilder();
            string lastKey = null;
            foreach (var key in values.Keys) {
                if (lastKey != null) {
                    sql.Append(", ");
                    valueString.Append(", ");
                } // end if
                sql.Append(key);
                valueString.Append(values[key]);
                lastKey = key;
            } // end for

            sql.Append(")" + Environment.NewLine + "VALUES" + Environment.NewLine + "(");
            sql.Append(valueString.ToString());
            sql.Append(')');
            return sql.ToString();
        } // end method



        private string FormatOrderColumns(IEnumerable<string> tableNames,
            IEnumerable<string> orderColumns) {
            if (orderColumns == null)
                return null;

            var columns = new List<string>();
            foreach (string column in orderColumns) {
                var table = GetTableForColumn(tableNames, column);
                if (table != null)
                    columns.Add(table + '.' + column);
            } // end foreach

            string result = null;
            if (columns.Any())
                result = string.Join(", ", columns);
            return result;
        } // end method



        //private static string FormatQueryValue(object value) {
        //    string result;
        //    if (value == null)
        //        result = "NULL";
        //    else if (value is string) {
        //        result = value.ToString();
        //        if (result.Length == 0) result = "NULL";
        //        else result = "'" + result.Replace("'", "''") + "'";
        //    } else if (value is bool) {
        //        if ((bool)value)
        //            result = "1";
        //        else
        //            result = "0";
        //    } else if (value is bool?) {
        //        if (((bool?)value).Value)
        //            result = "1";
        //        else
        //            result = "0";
        //    } else if (value is DateTime?)
        //        result = "'" + ((DateTime?)value).Value.ToString("yyyy-MM-dd HH:mm:ss") + "'";
        //    else
        //        result = value.ToString();
        //    return result;
        //} // end method



        private string FormatSelectCriteria(IEnumerable<string> tableNames,
            IEnumerable<Criterion> criteria) {
            if (criteria == null)
                return null;

            var whereClauseItems = new List<string>();
            foreach (var criterion in criteria) {
                var table = GetTableForColumn(tableNames, criterion.Name);
                if (table != null)
                    whereClauseItems.Add(criterion.ToString(table));
            } // end foreach

            string result = null;
            if (whereClauseItems.Any())
                result = string.Join(Environment.NewLine + "AND ", whereClauseItems);
            return result;
        } // end method



        private string FormatSelectQuery(IEnumerable<string> tableNames,
            IEnumerable<Criterion> criteria, IEnumerable<string> orderColumns) {
            var  sql = new StringBuilder();
            sql.Append("SELECT * FROM ");

            string lastName = null;
            foreach (var name in tableNames) {
                if (lastName != null)
                    sql.Append(Environment.NewLine + "INNER JOIN ");
                sql.Append(name);
                if (lastName != null)
                    sql.Append(Environment.NewLine + "ON " + name + ".Id = " + lastName + ".Id");
                lastName = name;
            } // end foreach

            var selectCriteria = FormatSelectCriteria(tableNames, criteria);
            if (selectCriteria != null) {
                sql.Append(Environment.NewLine + "WHERE ");
                sql.Append(selectCriteria);
            } // end if

            var selectOrderColumns = FormatOrderColumns(tableNames, orderColumns);
            if (selectOrderColumns != null) {
                sql.Append(Environment.NewLine + "ORDER BY ");
                sql.Append(selectOrderColumns);
            } // end if

            return sql.ToString();
        } // end method



        private static string FormatUpdateQuery(string table,
            IDictionary<string, string> values, long? id) {
            var sql = new StringBuilder();
            sql.Append("UPDATE " + table + " SET" + Environment.NewLine);

            string lastKey = null;
            foreach (string key in values.Keys) {
                if (lastKey != null)
                    sql.Append("," + Environment.NewLine);
                var value = values[key];
                sql.Append(key + " = " + value);
                lastKey = key;
            } // end foreach

            sql.Append(Environment.NewLine + "WHERE Id = " + id);
            return sql.ToString();
        } // end method



        public IEnumerable<T> Get<T>() where T : new() {
            return Get<T>((IEnumerable<Criterion>)null);
        } // end method



        public IEnumerable<T> Get<T>(IEnumerable<Criterion> criteria) where T : new() {
            return Get<T>(criteria, null);
        } // end method



        public IEnumerable<T> Get<T>(IEnumerable<Criterion> criteria,
            IEnumerable<string> orderColumns) where T : new() {
            var tableNames = tableNameCache[typeof(T)];
            var sql = FormatSelectQuery(tableNames, criteria, orderColumns);
            return Get<T>(sql);
        } // end method



        public T Get<T>(long id) where T : new() {
            T obj = default(T);
            //IDictionary typeCache = (IDictionary)objectCache[type];
            //if (typeCache != null) obj = (DatabaseObject)typeCache[id];

            if (obj == null) {
                var objs = Get<T>(new Criterion("Id", "=", id));
                if (objs != null)
                    obj = objs.FirstOrDefault();
                //if (typeCache != null) typeCache[id] = obj;
            } // end if

            return obj;
        } // end method



        public T Get<T>(long? id) where T : new() {
            T obj = default(T);
            if (id != null)
                obj = Get<T>(id.Value);
            return obj;
        } // end method



        public IEnumerable<T> Get<T>(object anonymousTypeCriteria)
            where T : new() {
            return Get<T>(anonymousTypeCriteria, null);
        } // end method



        public IEnumerable<T> Get<T>(object anonymousTypeCriteria,
            IEnumerable<string> orderColumns) where T : new() {
            var sqlCriteria = Criterion.Create(anonymousTypeCriteria);
            return Get<T>(sqlCriteria, orderColumns);
        } // end method



        public IEnumerable<T> Get<T>(params Criterion[] criteria) where T : new() {
            return Get<T>((IEnumerable<Criterion>)criteria);
        } // end method



        protected IEnumerable<T> Get<T>(string sql) where T : new() {
            bool wasNull = idCache == null;
            if (wasNull)
                idCache = new Cache<Tuple<Type,long?>,object>();

            var objs = CreateObjects<T>(connectionName, sql);
            foreach (var obj in objs) {
                var identifiable = obj as IIdentifiable;
                if (identifiable == null) 
                    continue;

                idCache.GetValue(new Tuple<Type, long?>(obj.GetType(), identifiable.Id), () => obj);
            } // end foreach
            SetReferences(objs);

            if (wasNull)
                idCache = null;

            return objs;
        } // end method



        private IDictionary<string, string> GetFieldValues(object obj, string tableName) {
            var values = new Dictionary<string, string>();
            var schema = schemaCache[tableName];
            if (schema == null)
                return values;

            var mirror = mirrorCache.GetValue(obj.GetType(), () => new Mirror(obj.GetType()));
            for (int i = 0; i < schema.Rows.Count; i++) {
                var key = (string)schema.Rows[i][0];
                object value = null;
                var prop = mirror.GetProperty(key);
                if (prop != null)
                    value = prop.GetValue(obj, null);
                values[key] = value.ToSqlString();
            } // end if

            return values;
        } // end method



        private IEnumerable<Reference> GetReferences(Type type) {
            var references = new List<Reference>();
            var mirror = mirrorCache.GetValue(type, () => new Mirror(type));
            var properties = mirror.GetProperties();
            foreach (var property in properties) {
                if (property.PropertyType.IsArray ||
                    property.PropertyType.IsBasic() ||
                    property.GetSetMethod() == null)
                    continue;

                if (property.PropertyType.IsGenericType &&
                    property.PropertyType.GetGenericTypeDefinition() == typeof(IEnumerable<>)) {
                    var enumType = property.PropertyType.GetGenericArguments()[0];
                    var tableNames = tableNameCache[enumType];
                    if (!tableNames.Any(tn => schemaCache[tn] != null))
                        continue;

                    var reference = new Reference();
                    reference.Property = property;
                    reference.ReferencedType = enumType;
                    reference.ReferencingType = property.DeclaringType;
                    references.Add(reference);
                } else {
                    var tableNames = tableNameCache[property.PropertyType];
                    if (!tableNames.Any(tn => schemaCache[tn] != null))
                        continue;

                    var idProp = properties.FirstOrDefault(p => p.Name == property.Name + "Id");
                    if (idProp == null)
                        continue;

                    var reference = new Reference();
                    reference.Property = property;
                    reference.ReferencedType = property.PropertyType;
                    reference.ReferencingProperty = idProp;
                    references.Add(reference);
                } // end if-else
            } // end foreach
            return references;
        } // end method



        private string GetTableForColumn(IEnumerable<string> tableNames, string column) {
            foreach (string tableName in tableNames) {
                var schema = schemaCache[tableName];
                for (int r = 0; r < schema.Rows.Count; r++) {
                    var name = (string)schema.Rows[r][0];
                    if (column == name)
                        return tableName;
                } // end for
            } // end for
            return null;
        } // end method



        private IEnumerable<string> GetTableNames(Type type) {
            var tableNameList = new List<string>();
            while (type != typeof(object)) {
                var tableName = type.Name;
                var schema = schemaCache[tableName];
                if (schema != null)
                    tableNameList.Add(tableName);
                type = type.BaseType;
            } // end while
            tableNameList.Reverse();
            return tableNameList;
        } // end method



        private DataTable GetTableSchema(string tableName) {
            DataTable schema;
            //var sql = "SELECT * FROM " + table +
            //    " WHERE Id = (SELECT MIN(sub.Id) FROM " + table + " sub)";
            var sql = "SELECT * FROM " + tableName + " WHERE 1 = 0";
            using (var con = DataTool.OpenConnection(connectionName))
            using (var cmd = con.CreateCommand()) {
                cmd.CommandText = sql;
                Log(sql);
                try {
                    using (var dr = cmd.ExecuteReader())
                        schema = dr.GetSchemaTable();
                } catch (DbException) {
                    // This exception most likely occurs when the table does not exist.
                    // In that situation, just set the schema to null (there is no schema).
                    schema = null;
                } // end try-catch
            } // end using
            return schema;
        } // end method



        protected void Log(string msg) {
            if (Debugger.IsAttached)
                Debug.WriteLine("[" + typeof(DatabaseRepository).FullName + "] " + msg);
        } // end method



        private object ReflectedGet(Type type, long? id) {
            var mirror = mirrorCache.GetValue(GetType(), () => new Mirror(GetType()));
            var getMethod = mirror.GetMethod("Get", new[] {typeof(long?)});
            getMethod = getMethod.MakeGenericMethod(type);
            return getMethod.Invoke(this, new object[] {id});
        } // end method



        private bool RecordExists(string table, long? id) {
            var exists = false;
            var sql = "SELECT COUNT(1) FROM " + table + " WHERE Id = " + id;
            using (var con = DataTool.OpenConnection(connectionName))
            using (var cmd = con.CreateCommand()) {
                cmd.CommandText = sql;
                Log(sql);
                object result = cmd.ExecuteScalar();
                var count = result as long? ?? result as int? ?? 0;
                exists = count > 0;
            } // end using
            return exists;
        } // end method



        public void Save(IIdentifiable obj) {
            //obj.Broker = this;
            var tableNames = tableNameCache[obj.GetType()];
            foreach (var table in tableNames)
                SaveObjectToTable(obj, table);
        } // end method



        private void SaveObjectToTable(IIdentifiable obj, string table) {
            if (obj.Id == null)
                obj.Id = sequencer.GetNextValue(table, "Id");
            var values = GetFieldValues(obj, table);

            string sql;
            if (RecordExists(table, obj.Id))
                sql = FormatUpdateQuery(table, values, obj.Id);
            else
                sql = FormatInsertQuery(table, values);
            //Console.WriteLine(sql.ToString());

            ExecuteWrite(sql);
        } // end method



        private void SetReferences<T>(IEnumerable<T> objs) {
            var references = referenceCache.GetValue(typeof(T), () => GetReferences(typeof(T)));
            foreach (var obj in objs)
            foreach (var reference in references) {
                if (reference.IsMany) {
                    var identifiable = (obj as IIdentifiable);
                    if (identifiable == null)
                        continue;

                    var id = identifiable.Id;
                    if (id == null)
                        continue;

                    Criterion criterion = new Criterion(reference.ReferencingType.Name + "Id", "=", id);
                    var lazyLoadEnum = CreateLazyLoadEnumerable(reference.ReferencedType, criterion);
                    reference.Property.SetValue(obj, lazyLoadEnum, null);
                } else {
                    long? id = null;
                    try {
                        id = (long)reference.ReferencingProperty.GetValue(obj, null);
                    } catch {
                        continue;
                    } // end try-catch
                    if (id == null)
                        continue;

                    object referenceObj = idCache.GetValue(new Tuple<Type, long?>(reference.ReferencedType, id),
                        () => ReflectedGet(reference.ReferencedType, id));
                    reference.Property.SetValue(obj, referenceObj, null);
                } // end if-else
            } // end foreach
        } // end method

    } // end class
} // end namespace