using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.Linq;
using System.Reflection;
using System.Text;
using Xanotech.Tools;

namespace Xanotech.Repository {

    public class DatabaseRepository : IRepository {

        private enum PagingMechanism {
            Programmatic, // Use C# to retrieve certain sections of the data
            LimitOffset, // MySQL, PostgreSQL: SELECT * FROM SomeTable LIMIT 10 OFFSET 30
            OffsetFetchFirst // SQL Server: SELECT * FROM SomeTable ORDER BY SomeColumn OFFSET 20 ROWS FETCH FIRST 10 ROWS ONLY
        } // end enum

        // The idCache is used exclusively when retrieving objects from
        // the database and only exists for the current thread.
        [ThreadStatic]
        private static Cache<Tuple<Type, long?>, object> idCache;

        private Action<string> log;
        private Func<IDbConnection> openConnectionFunc;
        private PagingMechanism? pagingMechanism;
        private Sequencer sequencer;

        private Cache<Type, PropertyInfo> idPropertyCache;
        private Cache<Type, Mirror> mirrorCache;
        private Cache<string, IEnumerable<string>> primaryKeyCache;
        private Cache<Type, IEnumerable<Reference>> referenceCache;
        private Cache<string, DataTable> schemaTableCache;
        private Cache<string, Tuple<string, string, string>> tableDefinitionCache;
        private Cache<Type, IEnumerable<string>> tableNameCache;



        public DatabaseRepository(Func<IDbConnection> openConnectionFunc) {
            this.openConnectionFunc = openConnectionFunc;
            sequencer = new Sequencer(openConnectionFunc);

            idPropertyCache = new Cache<Type, PropertyInfo>(GetIdProperty);
            mirrorCache = new Cache<Type, Mirror>(t => new Mirror(t));
            primaryKeyCache = new Cache<string, IEnumerable<string>>(GetPrimaryKeys);
            referenceCache = new Cache<Type, IEnumerable<Reference>>(GetReferences);
            schemaTableCache = new Cache<string, DataTable>(GetSchemaTable);
            tableDefinitionCache = new Cache<string, Tuple<string, string, string>>(GetTableDefinition);
            tableNameCache = new Cache<Type, IEnumerable<string>>(GetTableNames);
        } // end constructor



        public DatabaseRepository(string connectionStringName) :
            this(() => { return DataTool.OpenConnection(connectionStringName); }) {
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



        public long Count<T>() where T : new() {
            return Count<T>((IEnumerable<Criterion>)null);
        } // end method



        public long Count<T>(IEnumerable<Criterion> criteria) where T : new() {
            var tableNames = tableNameCache[typeof(T)];
            return Count(tableNames, criteria);
        } // end method



        public long Count<T>(long? id) where T : new() {
            var type = typeof(T);
            var idProperty = idPropertyCache[type];
            if (idProperty == null)
                throw new DataException("The Repository.Count<T>(long?) method cannot be used for " +
                    type.FullName + " because does not have a single integer-based primary key or " +
                    "a property that corresponds to the primary key.");
            return Count<T>(new Criterion(idProperty.Name, "=", id));
        } // end method



        public long Count<T>(object criteria) where T : new() {
            var enumerable = Criterion.Create(criteria);
            return Count<T>(enumerable);
        } // end method



        public long Count<T>(params Criterion[] criteria) where T : new() {
            return Count<T>((IEnumerable<Criterion>)criteria);
        } // end method



        protected long Count(IEnumerable<string> tableNames, IEnumerable<Criterion> criteria) {
            var cursor = new Cursor<object>(criteria, ExecuteFind, this);
            var sql = FormatSelectQuery(tableNames, cursor, true);
            Log(sql);
            using (var con = openConnectionFunc())
            using (var cmd = con.CreateCommand()) {
                cmd.CommandText = sql;
                object result = cmd.ExecuteScalar();
                return result as long? ?? result as int? ?? 0;
            } // end using
        } // end method



        public static DatabaseRepository Create<T>(string connectionString) where T : IDbConnection, new() {
            return new DatabaseRepository(() => { return DataTool.OpenConnection<T>(connectionString); });
        } // end method



        public static DatabaseRepository Create(Func<IDbConnection> openConnectionFunc) {
            return new DatabaseRepository(openConnectionFunc);
        } // end method



        public static DatabaseRepository Create(string connectionStringName) {
            return new DatabaseRepository(connectionStringName);
        } // end method



        private object CreateLazyLoadEnumerable(Type type, Criterion criterion) {
            var lazyLoadType = typeof(LazyLoadEnumerable<>);
            var mirror = mirrorCache[lazyLoadType];
            var lazyLoadGenericType = mirror.MakeGenericType(type);
            var instance = Activator.CreateInstance(lazyLoadGenericType);

            mirror = mirrorCache[lazyLoadGenericType];
            var property = mirror.GetProperty("Criterion");
            property.SetValue(instance, criterion, null);

            property = mirror.GetProperty("Repository");
            property.SetValue(instance, this, null);

            return instance;
        } // end method



        private T CreateObject<T>(IDataReader dr) where T : new() {
            T obj = new T();
            var type = typeof(T);
            var typeMirror = mirrorCache[type];
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
                // are converted to the PropertyType if the valType cannot be
                // assigned to the PropertyType
                if (valType == typeof(DBNull))
                    val = null;
                else {
                    var propMirror = mirrorCache[prop.PropertyType];
                    if (!propMirror.IsAssignableFrom(valType))
                        val = ConvertValue(val, prop.PropertyType);
                } // end if

                prop.SetValue(obj, val, null);
            } // end for
            return obj;
        } // end method



        private IEnumerable<T> CreateObjects<T>(string sql, Cursor<T> cursor) where T : new() {
            var objs = new List<T>();
            using (var con = openConnectionFunc())
            using (var cmd = con.CreateCommand()) {
                var recordNum = 0;
                var recordStart = cursor.skip ?? 0;
                var recordStop = cursor.limit != null ? recordStart + cursor.limit : null;

                cmd.CommandText = sql;
                Log(sql);
                using (var dr = cmd.ExecuteReader())
                while (dr.Read()) {
                    // Create objects if the pagingMechanism is not Programatic or
                    // (if it is) and recordNum is on or after recordStart and
                    // before recordStop.  recordStop is null if cursor.limit
                    // is null and in that case, use recordNum + 1 so that the record
                    // always results in CreateObject (since cursor.limit wasn't specified).
                    if (pagingMechanism != PagingMechanism.Programmatic ||
                        recordNum >= recordStart &&
                        recordNum < (recordStop ?? (recordNum + 1)))
                        objs.Add(CreateObject<T>(dr));
                    
                    recordNum++;

                    // Stop iterating if the pagingMechanism is null or Programmatic,
                    // recordStop is defined, and recordNum is on or after recordStop.
                    if ((pagingMechanism == null || pagingMechanism == PagingMechanism.Programmatic) &&
                        recordStop != null && recordNum >= recordStop)
                        break;
                } // end while
            } // end using
            return objs;
        } // end method



        private static void DefaultLog(string msg) {
            if (Debugger.IsAttached)
                Debug.WriteLine("[" + typeof(DatabaseRepository).FullName + "] " + msg);
        } // end method



        private IEnumerable<T> ExecuteFind<T>(IEnumerable<Criterion> criteria, Cursor<T> cursor)
            where T : new() {
            var tableNames = tableNameCache[typeof(T)];
            var sql = FormatSelectQuery(tableNames, cursor, false);

            bool wasNull = idCache == null;
            if (wasNull)
                idCache = new Cache<Tuple<Type, long?>, object>();

            var objs = CreateObjects<T>(sql, cursor);
            foreach (var obj in objs) {
                var id = GetId(obj);
                idCache.GetValue(new Tuple<Type, long?>(obj.GetType(), id), () => obj);
            } // end foreach
            SetReferences(objs);

            if (wasNull)
                idCache = null;

            return objs;
        } // end method



        private void ExecuteNonQuery(string sql) {
            using (var con = openConnectionFunc())
            using (var cmd = con.CreateCommand()) {
                cmd.CommandText = sql;
                Log(sql);
                cmd.ExecuteNonQuery();
            } // end using
        } // end method



        private void ExecuteWrite(string sql) {
            ExecuteNonQuery(sql);
        } // end method



        public Cursor<T> Find<T>() where T : new() {
            return Find<T>((IEnumerable<Criterion>)null);
        } // end method



        public Cursor<T> Find<T>(IEnumerable<Criterion> criteria) where T : new() {
            return new Cursor<T>(criteria, ExecuteFind, this);
        } // end method



        public Cursor<T> Find<T>(long? id) where T : new() {
            var type = typeof(T);
            var idProperty = idPropertyCache[type];
            if (idProperty == null)
                throw new DataException("The Repository.Find<T>(long?) method cannot be used for " +
                    type.FullName + " because does not have a single integer-based primary key or " +
                    "a property that corresponds to the primary key.");
            return Find<T>(new Criterion(idProperty.Name, "=", id));
        } // end method



        public Cursor<T> Find<T>(object criteria) where T : new() {
            var enumerable = Criterion.Create(criteria);
            return Find<T>(enumerable);
        } // end method



        public Cursor<T> Find<T>(params Criterion[] criteria) where T : new() {
            return Find<T>((IEnumerable<Criterion>)criteria);
        } // end method



        public T FindOne<T>() where T : new() {
            return Find<T>().Limit(1).FirstOrDefault();
        } // end method



        public T FindOne<T>(IEnumerable<Criterion> criteria) where T : new() {
            return Find<T>(criteria).Limit(1).FirstOrDefault();
        } // end method



        public T FindOne<T>(long? id) where T : new() {
            return Find<T>(id).Limit(1).FirstOrDefault();
        } // end method



        public T FindOne<T>(object criteria) where T : new() {
            return Find<T>(criteria).Limit(1).FirstOrDefault();
        } // end method



        public T FindOne<T>(params Criterion[] criteria) where T : new() {
            return Find<T>(criteria).Limit(1).FirstOrDefault();
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



        private string FormatDeleteQuery(string table, IEnumerable<Criterion> criteria) {
            return "DELETE FROM " + table + " WHERE " +
                FormatWhereClause(new[] {table}, criteria);
        } // end method



        private string FormatFromClause(IEnumerable<string> tableNames) {
            var fromClause = "";
            string lastTableName = null;
            IList<string> lastPrimaryKeys = null;
            foreach (var tableName in tableNames) {
                var primaryKeys = primaryKeyCache[tableName].ToList();
                if (lastTableName != null)
                    fromClause += Environment.NewLine + "INNER JOIN ";
                fromClause += tableName;

                if (lastTableName != null) {
                    if (primaryKeys.Count != lastPrimaryKeys.Count || primaryKeys.Count == 0)
                        throw new DataException("Primary keys must be defined for and match across " +
                            "parent and child tables for objects with inherritance (parent table: " +
                            lastTableName + " / child table: " + tableName + ").");

                    fromClause += Environment.NewLine + "ON ";
                    if (primaryKeys.Count == 1)
                        fromClause += tableName + "." + primaryKeys.First() + " = " +
                            lastTableName + "." + lastPrimaryKeys.First();
                    else
                        for (int pk = 0; pk < primaryKeys.Count; pk++) {
                            if (primaryKeys[pk] != lastPrimaryKeys[pk])
                                throw new DataException("Primary keys must exactly match across " +
                                    "parent and child tables for objects with inherritance " +
                                    "(parent table keys: " + string.Join(", ", primaryKeys) +
                                    " / child table keys: " + string.Join(", ", lastPrimaryKeys) + ").");

                            if (pk > 0)
                                fromClause += Environment.NewLine + "AND ";
                            fromClause += tableName + "." + primaryKeys[pk] + " = " +
                                lastTableName + "." + lastPrimaryKeys[pk];
                        } // end for
                } // end if

                lastTableName = tableName;
                lastPrimaryKeys = primaryKeys;
            } // end foreach
            return fromClause;
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



        /// <summary>
        ///   Creates the ORDER BY clause of a SQL statement based on the
        ///   tableNames and orderColumns specified.  The tableNames are
        ///   used to prefix the columns.  The orderColumns parameter is
        ///   an IDictionary where the column names are keys and the values
        ///   indicate sorting order: 1 for ascending, -1 for descending
        ///   (columns with 0 are ignored).
        /// </summary>
        /// <param name="tableNames"></param>
        /// <param name="orderColumns"></param>
        /// <returns></returns>
        private string FormatOrderClause(IEnumerable<string> tableNames,
            IDictionary<string, int> orderColumns) {
            if (orderColumns == null)
                return null;

            var columns = new List<string>();
            foreach (var column in orderColumns.Keys) {
                if (string.IsNullOrEmpty(column))
                    continue;

                var direction = orderColumns[column];
                if (direction == 0)
                    continue;

                var table = GetTableForColumn(tableNames, column);
                if (table != null) {
                    columns.Add(table + '.' + column);
                    if (direction < 0)
                        columns.Add(" DESC");
                } // end if
            } // end foreach

            string result = null;
            if (columns.Any())
                result = string.Join(", ", columns);
            return result;
        } // end method



        private string FormatPagingClause<T>(string tableName, Cursor<T> cursor)
            where T : new() {
            if (cursor.limit == null && cursor.skip == null)
                return null;

            var paging = GetPagingMechanism(tableName);
            if (paging == PagingMechanism.Programmatic)
                return null;

            string pagingClause = null;
            if (paging == PagingMechanism.LimitOffset) {
                var parts = new List<string>(2);
                if (cursor.limit != null)
                    parts.Add("LIMIT " + cursor.limit);
                if (cursor.skip != null)
                    parts.Add("OFFSET " + cursor.skip);
                pagingClause = string.Join(" ", parts);
            } // end if

            if (paging == PagingMechanism.OffsetFetchFirst) {
                var parts = new List<string>(3);
                if (cursor.sort == null || cursor.sort.Count == 0) {
                    var firstColumn = schemaTableCache[tableName].Rows[0][0];
                    parts.Add("ORDER BY " + firstColumn);
                } // end if
                parts.Add("OFFSET " + cursor.skip ?? 0 + " ROWS");
                if (cursor.limit != null)
                    parts.Add("FETCH FIRST " + cursor.limit + " ROWS ONLY");
                pagingClause = string.Join(" ", parts);
            } // end if

            return pagingClause;
        } // end method



        private string FormatSelectQuery<T>(IEnumerable<string> tableNames, Cursor<T> cursor, bool countOnly)
            where T : new() {
            var sql = countOnly ? "SELECT COUNT(*) FROM " : "SELECT * FROM ";
            sql += FormatFromClause(tableNames);

            var selectCriteria = FormatWhereClause(tableNames, cursor.criteria);
            if (selectCriteria != null)
                sql += Environment.NewLine + "WHERE " + selectCriteria;

            var selectOrderColumns = FormatOrderClause(tableNames, cursor.sort);
            if (selectOrderColumns != null)
                sql += Environment.NewLine + "ORDER BY " + selectOrderColumns;

            var pagingClause = FormatPagingClause(tableNames.FirstOrDefault(), cursor);
            if (pagingClause != null)
                sql += Environment.NewLine + pagingClause;

            return sql;
        } // end method



        private string FormatUpdateQuery(string table,
            IDictionary<string, string> values, IEnumerable<Criterion> criteria) {
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

            sql.Append(Environment.NewLine + "WHERE " + FormatWhereClause(new[] {table}, criteria));
            return sql.ToString();
        } // end method



        private string FormatWhereClause(IEnumerable<string> tableNames,
            IEnumerable<Criterion> criteria) {
            if (criteria == null)
                return null;

            var whereClauseItems = new List<string>();
            foreach (var criterion in criteria) {
                var table = GetTableForColumn(tableNames, criterion.Name);
                if (table != null)
                    whereClauseItems.Add(table + '.' + criterion.ToString());
            } // end foreach

            string result = null;
            if (whereClauseItems.Any())
                result = string.Join(Environment.NewLine + "AND ", whereClauseItems);
            return result;
        } // end method



        private long? GetId<T>(T obj) {
            PropertyInfo idProperty;
            return GetId(obj, out idProperty);
        } // end method



        private long? GetId(object obj, out PropertyInfo idProperty) {
            idProperty = idPropertyCache[obj.GetType()];
            if (idProperty == null)
                return null;
            return idProperty.GetValue(obj, null) as long?;
        } // end method



        private PropertyInfo GetIdProperty(Type type) {
            var tableNames = tableNameCache[type];
            if (!tableNames.Any())
                return null;

            var keys = primaryKeyCache[tableNames.First()];
            if (keys.Count() != 1)
                return null;

            var mirror = mirrorCache[type];
            var property = mirror.GetProperty(keys.FirstOrDefault());
            if (typeof(long?).IsAssignableFrom(property.PropertyType))
                return property;
            else
                return null;
        } // end method



        private PagingMechanism? GetPagingMechanism(string tableName) {
            if (pagingMechanism != null)
                return pagingMechanism;

            // Try LimitOffset
            var sql = "SELECT NULL FROM " + tableName + " LIMIT 1 OFFSET 0";
            pagingMechanism = TryPagingMechanism(sql, PagingMechanism.LimitOffset);
            if (pagingMechanism != null)
                return pagingMechanism;

            // Try OffsetFetchFirst
            var firstColumn = schemaTableCache[tableName].Rows[0][0];
            sql = "SELECT " + firstColumn + " FROM " + tableName + " ORDER BY " +
                firstColumn + " OFFSET 0 ROWS FETCH FIRST 1 ROWS ONLY";
            pagingMechanism = TryPagingMechanism(sql, PagingMechanism.OffsetFetchFirst);
            if (pagingMechanism != null)
                return pagingMechanism;

            pagingMechanism = PagingMechanism.Programmatic;
            return pagingMechanism;
        } // end method



        private IEnumerable<Criterion> GetPrimaryKeyCriteria(object obj, string table) {
            var type = obj.GetType();
            var mirror = mirrorCache[type];
            var primaryKeys = primaryKeyCache[table];
            var properties = new List<PropertyInfo>();
            var criteriaMap = new Dictionary<string, object>();
            foreach (var key in primaryKeys) {
                var property = mirror.GetProperty(key,
                    BindingFlags.IgnoreCase | BindingFlags.Instance | BindingFlags.Public);
                if (property == null)
                    throw new MissingPrimaryKeyException("The object passed does " +
                        "not specify all the primary keys for " + table +
                        " (expected missing key: " + key + ").");
                criteriaMap[key] = property.GetValue(obj, null);
            } // end foreach
            return Criterion.Create(criteriaMap);
        } // end method



        private IEnumerable<string> GetPrimaryKeys(string table) {
            var tableDef = tableDefinitionCache[table];
            var sql = "SELECT kcu.column_name " +
                "FROM information_schema.key_column_usage kcu " +
                "INNER JOIN information_schema.table_constraints tc " +
                "ON (tc.constraint_name = kcu.constraint_name OR " +
                "(tc.constraint_name IS NULL AND kcu.constraint_name IS NULL)) " +
                "AND (tc.table_schema = kcu.table_schema OR " +
                "(tc.table_schema IS NULL AND kcu.table_schema IS NULL)) " +
                "AND (tc.table_name = kcu.table_name OR " +
                "(tc.table_name IS NULL AND kcu.table_name IS NULL)) " +
                "WHERE tc.constraint_type = 'PRIMARY KEY' " +
                "AND kcu.table_name = " + tableDef.Item2.ToSqlString();
            if (tableDef.Item1 != null)
                sql += " AND kcu.table_schema = " + tableDef.Item1.ToSqlString();
            Log(sql);
            IEnumerable<IDictionary<string, object>> results;
            using (var con = openConnectionFunc())
                results = con.ExecuteReader(sql);
            return results.Select(r => r["column_name"] as string).OrderBy(cn => cn);
        } // end method



        private IEnumerable<Reference> GetReferences(Type type) {
            var references = new List<Reference>();
            var mirror = mirrorCache[type];
            var properties = mirror.GetProperties();
            foreach (var property in properties) {
                if (property.PropertyType.IsArray ||
                    property.PropertyType.IsBasic() ||
                    property.GetSetMethod() == null)
                    continue;

                if (property.PropertyType.IsGenericType &&
                    property.PropertyType.GetGenericTypeDefinition() == typeof(IEnumerable<>)) {
                    var enumType = property.PropertyType.GetGenericArguments()[0];
                    if (enumType.IsBasic())
                        continue;

                    var tableNames = tableNameCache[enumType];
                    if (!tableNames.Any(tn => schemaTableCache[tn] != null))
                        continue;

                    var reference = new Reference();
                    reference.Property = property;
                    reference.ReferencedType = enumType;
                    reference.ReferencingType = property.DeclaringType;
                    references.Add(reference);
                } else {
                    var tableNames = tableNameCache[property.PropertyType];
                    if (!tableNames.Any(tn => schemaTableCache[tn] != null))
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



        private DataTable GetSchemaTable(string tableName) {
            DataTable schema;
            //var sql = "SELECT * FROM " + table +
            //    " WHERE Id = (SELECT MIN(sub.Id) FROM " + table + " sub)";
            var sql = "SELECT * FROM " + tableName + " WHERE 1 = 0";
            using (var con = openConnectionFunc())
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



        private Tuple<string, string, string> GetTableDefinition(string typeName) {
            if (typeName == null)
                throw new ArgumentNullException("typeName", "The typeName parameter was not supplied.");

            var sql = "SELECT table_schema, table_name " +
                    "FROM information_schema.tables WHERE table_name = " + typeName.ToSqlString();
            Log(sql);
            IEnumerable<IDictionary<string, object>> results;
            using (var con = openConnectionFunc())
                results = con.ExecuteReader(sql);

            var count = results.Count();
            if (count == 0)
                return null;
            else if (count > 1)
                throw new DataException("The table \"" + typeName +
                    "\" is ambiguous because it is defined in multiple database schemas (" +
                    string.Join(", ", results.Select(r => r["TABLE_SCHEMA"])) +
                    ").  Use the Repository.Map method to explicitly define how " +
                    typeName + " maps to the database.");

            var first = results.First();

            var schemaName = first["table_schema"] as string;
            var tableName = first["table_name"] as string;
            var fullName = tableName;
            if (string.IsNullOrEmpty(schemaName))
                fullName = schemaName + "." + fullName;
            return new Tuple<string, string, string>(schemaName, tableName, fullName);
        } // end method



        private string GetTableForColumn(IEnumerable<string> tableNames, string column) {
            foreach (string tableName in tableNames) {
                var schema = schemaTableCache[tableName];
                for (int r = 0; r < schema.Rows.Count; r++) {
                    var name = (string)schema.Rows[r][0];
                    if (column == name)
                        return tableName;
                } // end for
            } // end for
            return null;
        } // end method



        private IDictionary<string, string> GetValues(object obj, string tableName) {
            var values = new Dictionary<string, string>();
            var schema = schemaTableCache[tableName];
            if (schema == null)
                return values;

            var mirror = mirrorCache[obj.GetType()];
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



        private IEnumerable<string> GetTableNames(Type type) {
            var tableNameList = new List<string>();
            while (type != typeof(object)) {
                var tableDef = tableDefinitionCache[type.Name];
                if (tableDef != null)
                    tableNameList.Add(tableDef.Item3);
                type = type.BaseType;
            } // end while
            tableNameList.Reverse();
            return tableNameList;
        } // end method



        public Action<string> Log {
            get {
                return log ?? DefaultLog;
            } // end get
            set {
                log = value;
            } // end set
        } // end property



        private bool RecordExists(string table, IEnumerable<Criterion> keys) {
            return Count(new[] {table}, keys) > 0;
        } // end method



        private object ReflectedFindOne(Type type, long? id) {
            var mirror = mirrorCache[GetType()];
            var method = mirror.GetMethod("FindOne", new[] { typeof(long?) });
            method = method.MakeGenericMethod(type);
            return method.Invoke(this, new object[] { id });
        } // end method



        public void Remove(object obj) {
            var enumerable = obj as IEnumerable;
            if (enumerable != null)
                foreach (var item in enumerable)
                    Remove(item);
            else {
                var tableNames = tableNameCache[obj.GetType()];
                foreach (var table in tableNames)
                    RemoveObjectFromTable(obj, table);
            } // end if
        } // end method



        private void RemoveObjectFromTable(object obj, string table) {
            var keys = GetPrimaryKeyCriteria(obj, table);
            if (RecordExists(table, keys)) {
                var sql = FormatDeleteQuery(table, keys);
                ExecuteWrite(sql);
            } // end if
        } // end method



        public void Save(object obj) {
            var enumerable = obj as IEnumerable;
            if (enumerable != null)
                foreach (var item in enumerable)
                    Save(item);
            else {
                var tableNames = tableNameCache[obj.GetType()];
                foreach (var tableName in tableNames)
                    SaveObjectToTable(obj, tableName);
            } // end if
        } // end method



        private void SaveObjectToTable<T>(T obj, string tableName) {
            PropertyInfo idProperty;
            var id = GetId(obj, out idProperty);
            if (id == null && idProperty != null) {
                id = sequencer.GetNextValue(tableName, idProperty.Name);
                idProperty.SetValue(obj, id, null);
            } // end if

            var criteria = GetPrimaryKeyCriteria(obj, tableName);
            var values = GetValues(obj, tableName);

            string sql;
            if (RecordExists(tableName, criteria))
                sql = FormatUpdateQuery(tableName, values, criteria);
            else
                sql = FormatInsertQuery(tableName, values);

            ExecuteWrite(sql);
        } // end method



        private void SetReferences<T>(IEnumerable<T> objs) {
            var references = referenceCache[typeof(T)];
            foreach (var obj in objs)
            foreach (var reference in references) {
                if (reference.IsMany) {
                    var id = GetId(obj);
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
                        () => ReflectedFindOne(reference.ReferencedType, id));
                    reference.Property.SetValue(obj, referenceObj, null);
                } // end if-else
            } // end foreach
        } // end method



        private PagingMechanism? TryPagingMechanism(string sql, PagingMechanism? paging) {
            using (var con = openConnectionFunc())
            try {
                Log(sql);
                con.ExecuteReader(sql);
            } catch (DbException) {
                paging = null;
            } // end try-catch
            return paging;
        } // end method

    } // end class
} // end namespace