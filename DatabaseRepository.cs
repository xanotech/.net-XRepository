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

        // The idCache is used exclusively when retrieving objects from
        // the database and only exists for the current thread.
        [ThreadStatic]
        private static Cache<Tuple<Type, long?>, object> idCache;

        // This connection is created as soon as it is needed during a repository call
        // (ie Count, Find, Save, Removed, etc).  At the the end of the call, it is Disposed.
        // It should only be accessed via GetConnection and CloseConnection;
        private IDbConnection connection;
        private static long connectionId = 0; // Tracks connections as they are opened / closed.

        // Created as it is needed during a repository call.  It relies on connection
        // but does not need to be disposed since disposing of connection is sufficient.
        // It should only be accessed via GetInfo.
        private DatabaseInfo info;

        private Action<string> log;
        private Func<IDbConnection> openConnectionFunc;



        public DatabaseRepository(Func<IDbConnection> openConnectionFunc) {
            this.openConnectionFunc = openConnectionFunc;
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



        protected void CloseConnection() {
            if (connection != null) {
                Log("Connection " + connectionId + " closed");
                connection.Dispose();
            } // end if

            info = null;
            connection = null;
        } // end method



        protected IDbConnection Connection {
            get {
                if (connection == null) {
                    connectionId++;
                    Log("Connection " + connectionId + " opened");
                    connection = openConnectionFunc();
                } // end function
                return connection;
            } // end get
        } // end property



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
            try {
                var tableNames = Info.GetTableNames(typeof(T));
                return Count(tableNames, criteria);
            } finally {
                CloseConnection();
            } // end try-finally
        } // end method



        public long Count<T>(long? id) where T : new() {
            try {
                var type = typeof(T);
                var idProperty = Info.GetIdProperty(type);
                if (idProperty == null)
                    throw new DataException("The Repository.Count<T>(long?) method cannot be used for " +
                        type.FullName + " because does not have a single integer-based primary key or " +
                        "a property that corresponds to the primary key.");
                return Count<T>(new Criterion(idProperty.Name, "=", id));
            } finally {
                CloseConnection();
            } // end try-finally
        } // end method



        public long Count<T>(object criteria) where T : new() {
            var enumerable = Criterion.Create(criteria);
            return Count<T>(enumerable);
        } // end method



        public long Count<T>(params Criterion[] criteria) where T : new() {
            return Count<T>((IEnumerable<Criterion>)criteria);
        } // end method



        protected long Count(IEnumerable<string> tableNames, IEnumerable<Criterion> criteria) {
            try {
                var cursor = new Cursor<object>(criteria, Fetch, this);
                var sql = FormatSelectQuery(tableNames, cursor, true);

                object result;
                using (var cmd = Connection.CreateCommand()) {
                    cmd.CommandText = sql;
                    Log(sql);
                    result = cmd.ExecuteScalar();
                } // end using

                return result as long? ?? result as int? ?? 0;
            } finally {
                CloseConnection();
            } // end try-finally
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
            var mirror = Info.GetMirror(lazyLoadType);
            var lazyLoadGenericType = mirror.MakeGenericType(type);
            var instance = Activator.CreateInstance(lazyLoadGenericType);

            mirror = Info.GetMirror(lazyLoadGenericType);
            var property = mirror.GetProperty("Criterion");
            property.SetValue(instance, criterion, null);

            property = mirror.GetProperty("Repository");
            property.SetValue(instance, this, null);

            return instance;
        } // end method



        private T CreateObject<T>(IDataReader dr) where T : new() {
            T obj = new T();
            var type = typeof(T);
            var typeMirror = Info.GetMirror(type);
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
                    var propMirror = Info.GetMirror(prop.PropertyType);
                    if (!propMirror.IsAssignableFrom(valType))
                        val = ConvertValue(val, prop.PropertyType);
                } // end if

                prop.SetValue(obj, val, null);
            } // end for
            return obj;
        } // end method



        private IEnumerable<T> CreateObjects<T>(string sql, Cursor<T> cursor) where T : new() {
            var objs = new List<T>();
            using (var cmd = Connection.CreateCommand()) {
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
                    if (cursor.pagingMechanism != DatabaseInfo.PagingMechanism.Programmatic ||
                        recordNum >= recordStart &&
                        recordNum < (recordStop ?? (recordNum + 1)))
                        objs.Add(CreateObject<T>(dr));
                    
                    recordNum++;

                    // Stop iterating if the pagingMechanism is null or Programmatic,
                    // recordStop is defined, and recordNum is on or after recordStop.
                    if ((cursor.pagingMechanism == null ||
                        cursor.pagingMechanism == DatabaseInfo.PagingMechanism.Programmatic) &&
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



        private void ExecuteNonQuery(string sql) {
            using (var cmd = Connection.CreateCommand()) {
                cmd.CommandText = sql;
                Log(sql);
                cmd.ExecuteNonQuery();
            } // end using
        } // end method



        private void ExecuteWrite(string sql) {
            ExecuteNonQuery(sql);
        } // end method



        private IEnumerable<T> Fetch<T>(IEnumerable<Criterion> criteria, Cursor<T> cursor)
            where T : new() {
            try {
                var tableNames = Info.GetTableNames(typeof(T));
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
            } finally {
                CloseConnection();
            } // end try-finally
        } // end method



        public Cursor<T> Find<T>() where T : new() {
            return Find<T>((IEnumerable<Criterion>)null);
        } // end method



        public Cursor<T> Find<T>(IEnumerable<Criterion> criteria) where T : new() {
            return new Cursor<T>(criteria, Fetch, this);
        } // end method



        public Cursor<T> Find<T>(long? id) where T : new() {
            try {
                var type = typeof(T);
                var idProperty = Info.GetIdProperty(type);
                if (idProperty == null)
                    throw new DataException("The Repository.Find<T>(long?) method cannot be used for " +
                        type.FullName + " because does not have a single integer-based primary key or " +
                        "a property that corresponds to the primary key.");
                return Find<T>(new Criterion(idProperty.Name, "=", id));
            } finally {
                CloseConnection();
            } // end try-finally
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
                var primaryKeys = Info.GetPrimaryKeys(tableName).ToList();
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

            cursor.pagingMechanism = Info.GetPagingMechanism(tableName);
            if (cursor.pagingMechanism == DatabaseInfo.PagingMechanism.Programmatic)
                return null;

            string pagingClause = null;
            if (cursor.pagingMechanism == DatabaseInfo.PagingMechanism.LimitOffset) {
                var parts = new List<string>(2);
                if (cursor.limit != null)
                    parts.Add("LIMIT " + cursor.limit);
                if (cursor.skip != null)
                    parts.Add("OFFSET " + cursor.skip);
                pagingClause = string.Join(" ", parts);
            } // end if

            if (cursor.pagingMechanism == DatabaseInfo.PagingMechanism.OffsetFetchFirst) {
                var parts = new List<string>(3);
                if (cursor.sort == null || cursor.sort.Count == 0) {
                    var firstColumn = Info.GetSchemaTable(tableName).Rows[0][0];
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
            idProperty = Info.GetIdProperty(obj.GetType());
            if (idProperty == null)
                return null;
            return idProperty.GetValue(obj, null) as long?;
        } // end method



        private IEnumerable<Criterion> GetPrimaryKeyCriteria(object obj, string tableName) {
            var type = obj.GetType();
            var mirror = Info.GetMirror(type);
            var primaryKeys = Info.GetPrimaryKeys(tableName);
            var properties = new List<PropertyInfo>();
            var criteriaMap = new Dictionary<string, object>();
            foreach (var key in primaryKeys) {
                var property = mirror.GetProperty(key,
                    BindingFlags.IgnoreCase | BindingFlags.Instance | BindingFlags.Public);
                if (property == null)
                    throw new MissingPrimaryKeyException("The object passed does " +
                        "not specify all the primary keys for " + tableName +
                        " (expected missing key: " + key + ").");
                criteriaMap[key] = property.GetValue(obj, null);
            } // end foreach
            return Criterion.Create(criteriaMap);
        } // end method



        private string GetTableForColumn(IEnumerable<string> tableNames, string column) {
            foreach (string tableName in tableNames) {
                var schema = Info.GetSchemaTable(tableName);
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
            var schema = Info.GetSchemaTable(tableName);
            if (schema == null)
                return values;

            var mirror = Info.GetMirror(obj.GetType());
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



        protected DatabaseInfo Info {
            get {
                if (info == null)
                    info = new DatabaseInfo(Connection, s => Log(s));
                return info;
            } // end get
        } // property



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
            var mirror = Info.GetMirror(GetType());
            var method = mirror.GetMethod("FindOne", new[] { typeof(long?) });
            method = method.MakeGenericMethod(type);
            return method.Invoke(this, new object[] { id });
        } // end method



        public void Remove(object obj) {
            try {
                var enumerable = obj as IEnumerable;
                if (enumerable != null)
                    foreach (var item in enumerable)
                        Remove(item);
                else {
                    var tableNames = Info.GetTableNames(obj.GetType());
                    foreach (var tableName in tableNames)
                        RemoveObjectFromTable(obj, tableName);
                } // end if
            } finally {
                CloseConnection();
            } // end try-finally
        } // end method



        private void RemoveObjectFromTable(object obj, string tableName) {
            var keys = GetPrimaryKeyCriteria(obj, tableName);
            if (RecordExists(tableName, keys)) {
                var sql = FormatDeleteQuery(tableName, keys);
                ExecuteWrite(sql);
            } // end if
        } // end method



        public void Save(object obj) {
            try {
                var enumerable = obj as IEnumerable;
                if (enumerable != null)
                    foreach (var item in enumerable)
                        Save(item);
                else {
                    var tableNames = Info.GetTableNames(obj.GetType());
                    foreach (var tableName in tableNames)
                        SaveObjectToTable(obj, tableName);
                } // end if
            } finally {
                CloseConnection();
            } // end try-finally
        } // end method



        private void SaveObjectToTable<T>(T obj, string tableName) {
            PropertyInfo idProperty;
            var id = GetId(obj, out idProperty);
            if (id == null && idProperty != null) {
                if (Info.Sequencer == null)
                    Info.Sequencer = new Sequencer(openConnectionFunc);
                id = Info.Sequencer.GetNextValue(tableName, idProperty.Name);
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
            var references = Info.GetReferences(typeof(T));
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

    } // end class
} // end namespace