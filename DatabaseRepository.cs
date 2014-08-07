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
using Xanotech.Tools;

namespace Xanotech.Repository {

    public class DatabaseRepository : IRepository {

        private const BindingFlags CaseInsensitiveBinding =
            BindingFlags.IgnoreCase | BindingFlags.Instance | BindingFlags.Public;

        /// <summary>
        ///   A static collection of connectionId values with their associated ConnectionInfos.
        /// </summary>
        private static ConcurrentDictionary<long, ConnectionInfo> connectionInfoMap =
            new ConcurrentDictionary<long, ConnectionInfo>();

        /// <summary>
        ///   Holds the next value to be assigned to connectionId when a new connection is created.
        /// </summary>
        private static long nextConnectionId = 0;

        /// <summary>
        ///   Used as a lock for thread-safe access to nextConnectionId.
        /// </summary>
        private static object nextConnectionIdLock = new object(); // Used for thread locking when accessing nextConnectionId.



        /// <summary>
        ///   Created by the Connection property as it is needed during a repository call
        ///   (ie Count, Find, Save, Remove, etc).  At the the end of the call, it is Disposed.
        ///   It should only be accessed via the Connection property.
        /// </summary>
        private IDbConnection connection;



        /// <summary>
        ///   Holds the "id" of the current connection.  The id values are simply consecutive
        ///   integers stored in the static nextConnectionId which is incremented whenever
        ///   a new connection is created.
        /// </summary>
        private long connectionId;

        /// <summary>
        ///   Used exclusively when retrieving objects from the database for the purpose of
        ///   setting references values (SetReferences) and only exists for the current thread.
        /// </summary>
        private IDictionary<Type, IDictionary<object, object>> idObjectMap;

        private IDictionary<Type, IDictionary<object, object>> joinObjectMap;

        /// <summary>
        ///   The Func used for creating / opening a new connection (provided during construction).
        /// </summary>
        private Func<IDbConnection> openConnectionFunc;



        public DatabaseRepository(Func<IDbConnection> openConnectionFunc) {
            this.openConnectionFunc = openConnectionFunc;
            CreationStack = new StackTrace(true).ToString();
            IsReferenceAssignmentActive = true;
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



        private void AddFromClause(IDbCommand cmd, IEnumerable<string> tableNames) {
            var sql = new StringBuilder();
            string lastTableName = null;
            IList<string> lastPrimaryKeys = null;
            foreach (var tableName in tableNames) {
                var primaryKeys = DatabaseInfo.GetPrimaryKeys(tableName).ToList();
                if (lastTableName != null)
                    sql.Append(Environment.NewLine + "INNER JOIN ");
                sql.Append(tableName);

                if (lastTableName != null) {
                    if (primaryKeys.Count != lastPrimaryKeys.Count || primaryKeys.Count == 0)
                        throw new DataException("Primary keys must be defined for and match across " +
                            "parent and child tables for objects with inherritance (parent table: " +
                            lastTableName + " / child table: " + tableName + ").");

                    sql.Append(Environment.NewLine + "ON ");
                    if (primaryKeys.Count == 1)
                        sql.Append(tableName + "." + primaryKeys.First() + " = " +
                            lastTableName + "." + lastPrimaryKeys.First());
                    else
                        for (int pk = 0; pk < primaryKeys.Count; pk++) {
                            if (primaryKeys[pk] != lastPrimaryKeys[pk])
                                throw new DataException("Primary keys must exactly match across " +
                                    "parent and child tables for objects with inherritance " +
                                    "(parent table keys: " + string.Join(", ", primaryKeys) +
                                    " / child table keys: " + string.Join(", ", lastPrimaryKeys) + ").");

                            if (pk > 0)
                                sql.Append(Environment.NewLine + "AND ");

                            sql.Append(tableName + "." + primaryKeys[pk] + " = " +
                                lastTableName + "." + lastPrimaryKeys[pk]);
                        } // end for
                } // end if

                lastTableName = tableName;
                lastPrimaryKeys = primaryKeys;
            } // end foreach
            cmd.CommandText += sql;
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
        private void AddOrderByClause(IDbCommand cmd, IEnumerable<string> tableNames,
            IDictionary<string, int> orderColumns) {
            if (orderColumns == null)
                return;

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

            if (columns.Any())
                cmd.CommandText += Environment.NewLine + "ORDER BY " + string.Join(", ", columns);
        } // end method



        private void AddPagingClause<T>(IDbCommand cmd, string tableName, Cursor<T> cursor)
            where T : new() {
            if (cursor.limit == null && cursor.skip == null)
                return;

            cursor.pagingMechanism = DatabaseInfo.GetPagingMechanism(tableName);
            if (cursor.pagingMechanism == DatabaseInfo.PagingMechanism.Programmatic)
                return;

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
                    var firstColumn = DatabaseInfo.GetSchemaTable(tableName).Rows[0][0];
                    parts.Add("ORDER BY " + firstColumn);
                } // end if
                parts.Add("OFFSET " + cursor.skip ?? 0 + " ROWS");
                if (cursor.limit != null)
                    parts.Add("FETCH FIRST " + cursor.limit + " ROWS ONLY");
                pagingClause = string.Join(" ", parts);
            } // end if

            if (pagingClause != null)
                cmd.CommandText += Environment.NewLine + pagingClause;
        } // end method



        private void AddSelectClause<T>(IDbCommand cmd, IEnumerable<string> tableNames, bool countOnly) {
            var mirror = DatabaseInfo.GetMirror(typeof(T));
            if (countOnly)
                cmd.CommandText = "SELECT COUNT(*) FROM ";
            else {
                var sql = new StringBuilder("SELECT ");
                bool isAfterFirst = false;
                var valuesOnLineCount = 0;
                foreach (var tableName in tableNames) {
                    var schema = DatabaseInfo.GetSchemaTable(tableName);
                    for (int i = 0; i < schema.Rows.Count; i++) {
                        var column = (string)schema.Rows[i][0];
                        var prop = mirror.GetProperty(column, CaseInsensitiveBinding);
                        if (prop == null)
                            continue;

                        if (isAfterFirst) {
                            sql.Append(',');
                            if (valuesOnLineCount == 4) {
                                sql.Append(Environment.NewLine);
                                valuesOnLineCount = 0;
                            } else
                                sql.Append(' ');
                        } // end if
                        valuesOnLineCount++;
                        sql.Append(tableName + '.' + column);
                        isAfterFirst = true;
                    } // end if
                } // end foreach
                sql.Append(Environment.NewLine + "FROM ");
                cmd.CommandText = sql.ToString();
            } // end if-else
        } // end method



        private void AddWhereClause(IDbCommand cmd, IEnumerable<string> tableNames,
            IEnumerable<Criterion> criteria) {
            if (criteria == null)
                return;

            var sql = new StringBuilder();
            var whereAdded = false;
            foreach (var criterion in criteria) {
                var table = GetTableForColumn(tableNames, criterion.Name);
                if (table == null)
                    continue;

                sql.Append(Environment.NewLine);
                sql.Append(whereAdded ? "AND " : "WHERE ");
                sql.Append(table + '.' + criterion.ToString(cmd, true));
                whereAdded = true;
            } // end foreach
            cmd.CommandText += sql;
        } // end method



        private void ApplySequenceId(object obj, string tableName) {
            PropertyInfo idProperty;
            var id = GetIntId(obj, out idProperty);
            if (id == null && idProperty != null) {
                id = Sequencer.GetNextValue(tableName, idProperty.Name);
                idProperty.SetValue(obj, id, null);
            } // end if
        } // end method



        private object CastToTypedEnumerable(IEnumerable enumerable, Type type) {
            var enumerableMirror = DatabaseInfo.GetMirror(typeof(Enumerable));
            var castMethod = enumerableMirror.GetMethod("Cast", new[] {typeof(IEnumerable)});
            castMethod = castMethod.MakeGenericMethod(new[] {type});
            return castMethod.Invoke(null, new object[] {enumerable});
        } // end method



        private static void CheckForConnectionLeaks(int seconds) {
            if (!Debugger.IsAttached)
                return;

            var nl = Environment.NewLine;
            var now = DateTime.UtcNow;
            foreach (var info in connectionInfoMap.Values)
                if ((now - info.CreationDatetime).Seconds > seconds)
                    throw new DataException("An unclosed database connection has been detected." + nl + nl +
                        "This exception is likely due to either 1) a bug in DatabaseRepository, " +
                        "2) an undisposed Transaction returned from BeginTransaction, or " +
                        "3) a bug in a class that extends DatabaseRepository.  This exception " +
                        "can only thrown when a Debugger is attached to the running process " +
                        "(so it should not appear in a live application)." + nl + nl +
                        "Information About the Unclosed Connection" + nl +
                        "Creation Date / Time: " + info.CreationDatetime + nl +
                        "SQL Command(s) Executed..." + nl +
                        string.Join(Environment.NewLine, info.SqlLog) + nl +
                        "DatabaseRepository.CreationStack..." + nl +
                        info.RepositoryCreationStack);
        } // end method



        protected void CloseConnection() {
            if (connection != null) {
                connection.Dispose();
                Log("Connection " + connectionId + " closed");
                connectionInfoMap.TryRemove(connectionId);
                CheckForConnectionLeaks(300);
            } // end if

            connection = null;
            ConnectionInfo = null;
            DatabaseInfo = null;
        } // end method



        protected IDbConnection Connection {
            get {
                if (connection == null) {
                    CheckForConnectionLeaks(300);
                    lock (nextConnectionIdLock)
                        connectionId = nextConnectionId++;
                    connection = openConnectionFunc();
                    Log("Connection " + connectionId + " opened");
                    ConnectionInfo = new ConnectionInfo();
                    ConnectionInfo.Id = connectionId;
                    ConnectionInfo.CreationDatetime = DateTime.UtcNow;
                    ConnectionInfo.RepositoryCreationStack = CreationStack;
                    connectionInfoMap.TryAdd(connectionId, ConnectionInfo);
                } // end function
                return connection;
            } // end get
        } // end property



        protected ConnectionInfo ConnectionInfo { get; private set; }



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
                var tableNames = DatabaseInfo.GetTableNames(typeof(T));
                return Count(tableNames, criteria);
            } finally {
                CloseConnection();
            } // end try-finally
        } // end method



        public long Count<T>(object criteria) where T : new() {
            if (criteria == null || criteria.GetType().IsBasic()) try {
                var type = typeof(T);
                var idProperty = DatabaseInfo.GetIdProperty(type);
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
                CloseConnection();
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



        protected long Count(IEnumerable<string> tableNames, IEnumerable<Criterion> criteria) {
            try {
                var cursor = new Cursor<object>(criteria, Fetch, this);
                using (var cmd = CreateSelectCommand(tableNames, cursor, true)) {
                    var result = cmd.ExecuteScalar();
                    return result as long? ?? result as int? ?? 0;
                } // end using
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



        private IDbCommand CreateDeleteCommand(string table, IEnumerable<Criterion> criteria) {
            var cmd = Connection.CreateCommand();
            try {
                cmd.CommandText = "DELETE FROM " + table;
                AddWhereClause(cmd, new[] {table}, criteria);
                LogCommand(cmd);
                return cmd;
            } catch {
                // If any exceptions occur, Dispose cmd (since its IDisposable and all)
                // and then let the exception bubble up the stack.
                cmd.Dispose();
                throw;
            } // end try-catch
        } // end method



        private IDbCommand CreateInsertCommand(string table, IDictionary<string, object> values) {
            var cmd = Connection.CreateCommand();
            try {
                var sql = new StringBuilder("INSERT INTO " + table + Environment.NewLine + "(");
                var valueString = new StringBuilder();
                bool isAfterFirst = false;
                foreach (var column in values.Keys) {
                    if (isAfterFirst) {
                        sql.Append(", ");
                        valueString.Append(", ");
                    } // end if
                    sql.Append(column);
                    valueString.Append('@' + column);
                    cmd.AddParameter(column, values[column]);
                    isAfterFirst = true;
                } // end for

                sql.Append(")" + Environment.NewLine + "VALUES" + Environment.NewLine + "(");
                sql.Append(valueString.ToString());
                sql.Append(')');

                cmd.CommandText = sql.ToString();
                LogCommand(cmd);
                return cmd;
            } catch {
                // If any exceptions occur, Dispose cmd (since its IDisposable and all)
                // and then let the exception bubble up the stack.
                cmd.Dispose();
                throw;
            } // end try-catch
        } // end method



        private object CreateLazyLoadEnumerable(Type type, Criterion criterion, object referencingObject) {
            var lazyLoadType = typeof(LazyLoadEnumerable<>);
            var mirror = DatabaseInfo.GetMirror(lazyLoadType);
            var lazyLoadGenericType = mirror.MakeGenericType(type);
            var instance = Activator.CreateInstance(lazyLoadGenericType);

            mirror = DatabaseInfo.GetMirror(lazyLoadGenericType);
            var property = mirror.GetProperty("Criterion");
            property.SetValue(instance, criterion, null);

            property = mirror.GetProperty("ReferencingObject");
            property.SetValue(instance, referencingObject, null);

            property = mirror.GetProperty("Repository");
            property.SetValue(instance, this, null);

            return instance;
        } // end method



        private T CreateObject<T>(IDataReader dr) where T : new() {
            T obj = new T();
            var type = typeof(T);
            var typeMirror = DatabaseInfo.GetMirror(type);
            var schema = dr.GetSchemaTable();
            for (var i = 0; i < dr.FieldCount; i++) {
                var name = (string)schema.Rows[i][0];
                name = FormatColumnName(name);

                var val = dr.GetValue(i);
                var valType = val.GetType();

                var prop = typeMirror.GetProperty(name, CaseInsensitiveBinding);

                if (prop == null)
                    continue;

                // DBNull objects are turned into "null" and all other values
                // are converted to the PropertyType if the valType cannot be
                // assigned to the PropertyType
                if (val == DBNull.Value)
                    val = null;
                else {
                    var propMirror = DatabaseInfo.GetMirror(prop.PropertyType);
                    if (!propMirror.IsAssignableFrom(valType))
                        val = ConvertValue(val, prop.PropertyType);
                } // end if

                prop.SetValue(obj, val, null);
            } // end for
            return obj;
        } // end method



        private IEnumerable<T> CreateObjects<T>(IDbCommand cmd, Cursor<T> cursor) where T : new() {
            var objs = new List<T>();
            var recordNum = 0;
            var recordStart = cursor.skip ?? 0;
            var recordStop = cursor.limit != null ? recordStart + cursor.limit : null;

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
            return objs;
        } // end method



        private IDbCommand CreateSelectCommand<T>(IEnumerable<string> tableNames, Cursor<T> cursor, bool countOnly)
            where T : new() {
            var cmd = Connection.CreateCommand();
            try {
                AddSelectClause<T>(cmd, tableNames, countOnly);
                AddFromClause(cmd, tableNames);
                AddWhereClause(cmd, tableNames, cursor.criteria);
                AddOrderByClause(cmd, tableNames, cursor.sort);
                AddPagingClause(cmd, tableNames.FirstOrDefault(), cursor);
                LogCommand(cmd);
                return cmd;
            } catch {
                // If any exceptions occur, Dispose cmd (since its IDisposable and all)
                // and then let the exception bubble up the stack.
                cmd.Dispose();
                throw;
            } // end try-catch
        } // end method



        private IDbCommand CreateUpdateCommand(string table,
            IDictionary<string, object> values, IEnumerable<Criterion> criteria) {

            var cmd = Connection.CreateCommand();
            try {
                var sql = new StringBuilder("UPDATE " + table + " SET" + Environment.NewLine);

                var primaryKeyColumns = criteria.Select(c => c.Name.ToUpper());
                bool isAfterFirst = false;
                foreach (string column in values.Keys) {
                    if (primaryKeyColumns.Contains(column.ToUpper()))
                        continue;

                    if (isAfterFirst)
                        sql.Append("," + Environment.NewLine);
                    sql.Append(column + " = @" + column);
                    cmd.AddParameter(column, values[column]);
                    isAfterFirst = true;
                } // end foreach
                cmd.CommandText = sql.ToString();
                AddWhereClause(cmd, new[] { table }, criteria);

                LogCommand(cmd);
                return cmd;
            } catch {
                // If any exceptions occur, Dispose cmd (since its IDisposable and all)
                // and then let the exception bubble up the stack.
                cmd.Dispose();
                throw;
            } // end try-catch
        } // end method



        public string CreationStack { get; private set; }



        private DatabaseInfo databaseInfo;
        protected DatabaseInfo DatabaseInfo {
            get {
                if (databaseInfo == null)
                    databaseInfo = new DatabaseInfo(Connection, s => Log(s));
                return databaseInfo;
            } // end get
            private set {
                databaseInfo = value;
            } // end set
        } // property



        private static void DefaultLog(string msg) {
            if (Debugger.IsAttached)
                Debug.WriteLine("[" + typeof(DatabaseRepository).FullName + "] " + msg);
        } // end method



        private static void DisposeMappedCommands(Dictionary<Type, IDictionary<string, IDbCommand>> commandMap) {
            foreach (var subMap in commandMap.Values)
            if (subMap != null)
            foreach (var cmd in subMap.Values)
            if (cmd != null)
                cmd.Dispose();
        } // end method



        private IEnumerable<T> Fetch<T>(Cursor<T> cursor, IEnumerable[] joinObjects)
            where T : new() {
            try {
                bool wasNull = idObjectMap == null;
                if (wasNull)
                    idObjectMap = new Dictionary<Type, IDictionary<object, object>>();

                IEnumerable<T> objs;
                var tableNames = DatabaseInfo.GetTableNames(typeof(T));
                using (var cmd = CreateSelectCommand(tableNames, cursor, false))
                    objs = CreateObjects<T>(cmd, cursor);
                foreach (var obj in objs) {
                    var id = GetId(obj);
                    if (id != null)
                        MapObject(id, obj, idObjectMap);
                } // end foreach

                if (joinObjectMap == null)
                    InitObjectMaps(joinObjects);
                SetReferences(objs);
                if (wasNull) {
                    idObjectMap = null;
                    joinObjectMap = null;
                } // end if

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



        public Cursor<T> Find<T>(object criteria) where T : new() {
            if (criteria == null || criteria.GetType().IsBasic()) try {
                var type = typeof(T);
                var idProperty = DatabaseInfo.GetIdProperty(type);
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
                CloseConnection();
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
            if (reference.IsOne) {
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
                var idProperty = DatabaseInfo.GetIdProperty(type);
                if (idProperty == null)
                    continue;

                var criterion = new Criterion(idProperty.Name, "=", idsToFind[type]);
                var results = ReflectedFind(type, new[] {criterion});
                // Retrieves results (which loads them into idObjectMap)
                (results as IEnumerable).GetEnumerator();
            } // end foreach
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



        private string FormatLogMessage(IDbCommand cmd) {
            var msg = new StringBuilder(cmd.CommandText);
            if (cmd.Parameters.Count == 0)
                return msg.ToString();

            msg.Append(Environment.NewLine);
            var isAfterFirst = false;
            foreach (IDbDataParameter parameter in cmd.Parameters) {
                if (isAfterFirst)
                    msg.Append(", ");

                msg.Append('@' + parameter.ParameterName + " = " + parameter.Value.ToSqlString());
                isAfterFirst = true;
            } // end foreach
            return msg.ToString();
        } // end method



        private object GetId(object obj) {
            PropertyInfo idProperty;
            return GetId(obj, out idProperty);
        } // end method



        private object GetId(object obj, out PropertyInfo idProperty) {
            idProperty = DatabaseInfo.GetIdProperty(obj.GetType());
            if (idProperty == null)
                return null;
            return idProperty.GetValue(obj, null);
        } // end method



        private long? GetIntId(object obj) {
            return GetId(obj) as long?;
        } // end method



        private long? GetIntId(object obj, out PropertyInfo idProperty) {
            var id = GetId(obj, out idProperty) as long?;
            if (idProperty != null &&
                !typeof(long?).IsAssignableFrom(idProperty.PropertyType))
                idProperty = null;
            return id;
        } // end method



        private IDbCommand GetMappedCommand(IDictionary<Type, IDictionary<string, IDbCommand>> commandMap,
            Type type, string tableName, IDictionary<string, object> values, Func<IDbCommand> createCmdFunc) {
            IDbCommand cmd;
            if (commandMap[type].ContainsKey(tableName)) {
                cmd = commandMap[type][tableName];
                foreach (IDbDataParameter parameter in cmd.Parameters)
                    parameter.Value = values[parameter.ParameterName] ?? DBNull.Value;

                // Only explicity log here (when the command exists after parameters
                // are set) because CreateDeleteCommand already logs the command.
                LogCommand(cmd);
            } else {
                cmd = createCmdFunc();
                cmd.Prepare();
                commandMap[type][tableName] = cmd;
            } // end if-else
            return cmd;
        } // end method



        private IEnumerable<Criterion> GetPrimaryKeyCriteria(object obj, string tableName) {
            var type = obj.GetType();
            var mirror = DatabaseInfo.GetMirror(type);
            var primaryKeys = DatabaseInfo.GetPrimaryKeys(tableName);
            var properties = new List<PropertyInfo>();
            var criteriaMap = new Dictionary<string, object>();
            foreach (var key in primaryKeys) {
                var property = mirror.GetProperty(key, CaseInsensitiveBinding);
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
                var schema = DatabaseInfo.GetSchemaTable(tableName);
                for (int r = 0; r < schema.Rows.Count; r++) {
                    var name = (string)schema.Rows[r][0];
                    if (column == name)
                        return tableName;
                } // end for
            } // end for
            return null;
        } // end method



        private IDictionary<string, object> GetValues(object obj, string tableName) {
            var values = new Dictionary<string, object>();
            var schema = DatabaseInfo.GetSchemaTable(tableName);
            if (schema == null)
                return values;

            var mirror = DatabaseInfo.GetMirror(obj.GetType());
            for (int i = 0; i < schema.Rows.Count; i++) {
                var key = (string)schema.Rows[i][0];
                var prop = mirror.GetProperty(key, CaseInsensitiveBinding);
                if (prop != null)
                    values[key] = prop.GetValue(obj, null);
            } // end if

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



        public bool IsReferenceAssignmentActive { get; set; }



        private Action<string> log;
        public Action<string> Log {
            get {
                return log ?? DefaultLog;
            } // end get
            set {
                log = value;
            } // end set
        } // end property



        protected void LogCommand(IDbCommand cmd) {
            var msg = FormatLogMessage(cmd);
            Log(msg);
            ConnectionInfo.SqlLog.Add(msg);
        } // end method



        private void MapObject(object id, object obj,
            params IDictionary<Type, IDictionary<object, object>>[] maps) {
            var type = obj.GetType();
            while (type != typeof(object)) {
                if (DatabaseInfo.GetTableDefinition(type.Name) != null)
                    foreach (var map in maps) {
                        if (!map.ContainsKey(type))
                            map[type] = new Dictionary<object, object>();
                        map[type][id] = obj;
                    } // end foreach
                type = type.BaseType;
            } // end while
        } // end method



        private bool RecordExists(string table, IEnumerable<Criterion> keys) {
            return Count(new[] {table}, keys) > 0;
        } // end method



        private object ReflectedFind(Type type, object criteria) {
            var mirror = DatabaseInfo.GetMirror(GetType());
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



        private void RemoveRange(IEnumerable enumerable) {
            // Used for storing prepared statements to be used repeatedly
            var cmdMap = new Dictionary<Type, IDictionary<string, IDbCommand>>();
            try {
                // For each object and for each of that objects associate tables,
                // use the object's primary keys as parameters for a delete query.
                // Commands created for first Type / tableName combination are
                // prepared (compiled) and then stored in cmdMap to be used again
                // for other objects.
                foreach (var obj in enumerable) {
                    var type = obj.GetType();
                    if (!cmdMap.ContainsKey(type))
                        cmdMap[type] = new Dictionary<string, IDbCommand>();

                    var tableNames = DatabaseInfo.GetTableNames(type);
                    foreach (var tableName in tableNames) {
                        var criteria = GetPrimaryKeyCriteria(obj, tableName);

                        // If the keys criteria has a null value, then the operator will be
                        // "IS" or "IS NOT" instead of "=" or "<>" making the query usable only
                        // once.  In that case, simply create the cmd, run it, and dispose it.
                        // Otherwise, use the cmdMap.  If the cmd required doesn't exist
                        // in the cmdMap, create it, prepare it, and add it.  If the cmd
                        // does exist, change the parameters to the object's keys.
                        if (criteria.HasNullValue())
                            using (var cmd = CreateDeleteCommand(tableName, criteria))
                                cmd.ExecuteNonQuery();
                        else {
                            var cmd = GetMappedCommand(cmdMap, type, tableName, criteria.ToDictionary(),
                                () => { return CreateDeleteCommand(tableName, criteria); });
                            cmd.ExecuteNonQuery();
                        } // end if-else
                    } // end foreach
                } // end foreach
            } finally {
                foreach (var subMap in cmdMap.Values)
                    foreach (var cmd in subMap.Values)
                        cmd.Dispose();
                CloseConnection();
            } // end try-finally
        } // end method



        public void Save(object obj) {
            var enumerable = obj as IEnumerable;
            if (enumerable == null)
                enumerable = new[] { obj };
            SaveRange(enumerable);
        } // end method



        private void SaveRange(IEnumerable enumerable) {
            // Used for storing prepared statements to be used repeatedly
            var countCmdMap = new Dictionary<Type, IDictionary<string, IDbCommand>>();
            var insertCmdMap = new Dictionary<Type, IDictionary<string, IDbCommand>>();
            var updateCmdMap = new Dictionary<Type, IDictionary<string, IDbCommand>>();
            try {
                foreach (var obj in enumerable) {
                    var type = obj.GetType();
                    if (!countCmdMap.ContainsKey(type))
                        countCmdMap[type] = new Dictionary<string, IDbCommand>();
                    if (!insertCmdMap.ContainsKey(type))
                        insertCmdMap[type] = new Dictionary<string, IDbCommand>();
                    if (!updateCmdMap.ContainsKey(type))
                        updateCmdMap[type] = new Dictionary<string, IDbCommand>();

                    var tableNames = DatabaseInfo.GetTableNames(type);
                    foreach (var tableName in tableNames) {
                        ApplySequenceId(obj, tableName);
                        var criteria = GetPrimaryKeyCriteria(obj, tableName);
                        var values = GetValues(obj, tableName);

                        var criteriaAndValues = new Dictionary<string, object>(values);
                        criteriaAndValues.AddRange(criteria.ToDictionary());

                        // If the keys criteria has a null value, then the operator will be
                        // "IS" or "IS NOT" instead of "=" or "<>" making the query usable only
                        // once.  In that case, simply create the cmd, run it, and dispose it.
                        // Otherwise, use the cmdMap.  If the cmd required doesn't exist
                        // in the cmdMap, create it, prepare it, and add it.  If the cmd
                        // does exist, change the parameters to the object's keys.
                        object result;
                        long? count;
                        if (criteria.HasNullValue()) {
                            var cursor = new Cursor<object>(criteria, Fetch, this);
                            using (var cmd = CreateSelectCommand(tableNames, cursor, true))
                                result = cmd.ExecuteScalar();
                            count = result as long? ?? result as int? ?? 0;
                            using (var cmd = count == 0 ?
                                CreateInsertCommand(tableName, values) :
                                CreateUpdateCommand(tableName, values, criteria))
                                cmd.ExecuteNonQuery();
                        } else {
                            var cursor = new Cursor<object>(criteria, Fetch, this);
                            IDbCommand cmd = GetMappedCommand(countCmdMap, type, tableName, criteria.ToDictionary(),
                                () => { return CreateSelectCommand(tableNames, cursor, true); });
                            result = cmd.ExecuteScalar();
                            count = result as long? ?? result as int? ?? 0;

                            if (count == 0)
                                cmd = GetMappedCommand(insertCmdMap, type, tableName, criteriaAndValues,
                                    () => { return CreateInsertCommand(tableName, values); });
                            else
                                cmd = GetMappedCommand(updateCmdMap, type, tableName, criteriaAndValues,
                                    () => { return CreateUpdateCommand(tableName, values, criteria); });
                            cmd.ExecuteNonQuery();
                        } // end if-else

                    } // end foreach
                } // end foreach
            } finally {
                DisposeMappedCommands(countCmdMap);
                DisposeMappedCommands(insertCmdMap);
                DisposeMappedCommands(updateCmdMap);
                CloseConnection();
            } // end try-finally
        } // end method



        private Sequencer sequencer;
        public Sequencer Sequencer {
            get {
                if (sequencer == null) {
                    if (DatabaseInfo.Sequencer == null)
                        DatabaseInfo.Sequencer = new Sequencer(openConnectionFunc);
                    sequencer = DatabaseInfo.Sequencer;
                } // end if
                return sequencer;
            } // end get
            set {
                sequencer = value;
            } // end set
        } // end property



        private void SetManyReferences(IEnumerable objs, IEnumerable<Reference> references) {
            // This funky collection holds enumerables (ILists) keyed by Type and KeyProperty value == id
            var joinEnumerables = new Dictionary<Type, IDictionary<object, IList<object>>>();

            foreach (var obj in objs)
            foreach (var reference in references)
            if (reference.IsMany) {
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



        private void SetOneReferences(IEnumerable objs, IEnumerable<Reference> references) {
            foreach (var obj in objs)
            foreach (var reference in references)
            if (reference.IsOne) {
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
            var references = DatabaseInfo.GetReferences(type);
            SetManyReferences(objs, references);
            FindReferenceIds(objs, references);
            SetOneReferences(objs, references);
        } // end method

    } // end class
} // end namespace