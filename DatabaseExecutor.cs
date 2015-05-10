using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Data.OleDb;
using System.Diagnostics;
using System.Linq;
using System.Reflection;
using System.Text;
using XTools;



namespace XRepository {
    using IRecord = IDictionary<string, object>;
    using Record = Dictionary<string, object>;
    
    public class DatabaseExecutor : Executor {

        public enum PagingMechanism {
            Programmatic, // Use code logic to retrieve certain sections of the data
            LimitOffset, // MySQL, PostgreSQL: SELECT * FROM SomeTable LIMIT 10 OFFSET 30
            OffsetFetchFirst // SQL Server: SELECT * FROM SomeTable ORDER BY SomeColumn OFFSET 20 ROWS FETCH FIRST 10 ROWS ONLY
        } // end enum

        /// <summary>
        ///   A static collection of connectionId values with their associated ConnectionInfos.
        /// </summary>
        private static ConcurrentDictionary<long, ConnectionInfo> connectionInfoMap =
            new ConcurrentDictionary<long, ConnectionInfo>();

        private static Cache<string, DatabaseInfoCache> infoCache = new Cache<string, DatabaseInfoCache>(s => new DatabaseInfoCache());

        private static Cache<Type, Mirror> mirrorCache = new Cache<Type, Mirror>(t => new Mirror(t));

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



        protected virtual void AddFromClause(IDbCommand cmd, IEnumerable<string> tableNames) {
            var sql = new StringBuilder();
            string lastTableName = null;
            IList<string> lastPrimaryKeys = null;
            foreach (var tableName in tableNames) {
                var primaryKeys = GetPrimaryKeys(tableName).ToList();
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
        protected virtual void AddOrderByClause(IDbCommand cmd, IEnumerable<string> tableNames,
            IDictionary<string, int> orderColumns) {
            if (orderColumns == null)
                return;

            var orderBys = new List<string>();
            foreach (var column in orderColumns.Keys) {
                if (string.IsNullOrEmpty(column))
                    continue;

                var direction = orderColumns[column];
                if (direction == 0)
                    continue;

                var table = GetTableForColumn(tableNames, column);
                if (table != null) {
                    var orderBy = table + '.' + column;
                    if (direction < 0)
                        orderBy += " DESC";
                    orderBys.Add(orderBy);
                } // end if
            } // end foreach

            if (orderBys.Any())
                cmd.CommandText += Environment.NewLine + "ORDER BY " + string.Join(", ", orderBys);
        } // end method



        protected virtual void AddPagingClause(IDbCommand cmd, string tableName, CursorData cursorData) {
            if (cursorData.limit == null && cursorData.skip == null)
                return;

            cursorData.pagingMechanism = GetPagingMechanism(tableName);
            if (cursorData.pagingMechanism == PagingMechanism.Programmatic)
                return;

            string pagingClause = null;
            if (cursorData.pagingMechanism == PagingMechanism.LimitOffset) {
                var parts = new List<string>(2);
                if (cursorData.limit != null)
                    parts.Add("LIMIT " + cursorData.limit);
                if (cursorData.skip != null)
                    parts.Add("OFFSET " + cursorData.skip);
                pagingClause = string.Join(" ", parts);
            } // end if

            if (cursorData.pagingMechanism == PagingMechanism.OffsetFetchFirst) {
                var parts = new List<string>(3);
                if (cursorData.sort == null || cursorData.sort.Count == 0) {
                    var firstColumn = GetSchemaTable(tableName).Rows[0]["ColumnName"];
                    parts.Add("ORDER BY " + tableName + "." + firstColumn);
                } // end if
                parts.Add("OFFSET " + (cursorData.skip ?? 0) + " ROWS");
                if (cursorData.limit != null)
                    parts.Add("FETCH FIRST " + cursorData.limit + " ROWS ONLY");
                pagingClause = string.Join(" ", parts);
            } // end if

            if (pagingClause != null)
                cmd.CommandText += Environment.NewLine + pagingClause;
        } // end method



        protected virtual void AddSelectClause(IDbCommand cmd, IEnumerable<string> tableNames, bool countOnly) {
            //var mirror = mirrorCache[typeof(T)];
            if (countOnly)
                cmd.CommandText = "SELECT COUNT(*) FROM ";
            else {
                cmd.CommandText = "SELECT * FROM ";
                //var sql = new StringBuilder("SELECT ");
                //bool isAfterFirst = false;
                //var valuesOnLineCount = 0;
                //foreach (var tableName in tableNames) {
                //    var schema = GetSchemaTable(tableName);
                //    for (int i = 0; i < schema.Rows.Count; i++) {
                //        var column = (string)schema.Rows[i]["ColumnName"];
                //        var prop = mirror.GetProperty(column, CaseInsensitiveBinding);
                //        if (prop == null)
                //            continue;

                //        if (isAfterFirst) {
                //            sql.Append(',');
                //            if (valuesOnLineCount == 4) {
                //                sql.Append(Environment.NewLine);
                //                valuesOnLineCount = 0;
                //            } else
                //                sql.Append(' ');
                //        } // end if
                //        valuesOnLineCount++;
                //        sql.Append(tableName + '.' + column);
                //        isAfterFirst = true;
                //    } // end if
                //} // end foreach
                //sql.Append(Environment.NewLine + "FROM ");
                //cmd.CommandText = sql.ToString();
            } // end if-else
        } // end method



        protected virtual void AddWhereClause(IDbCommand cmd, IEnumerable<string> tableNames,
            IEnumerable<Criterion> criteria) {
            if (criteria == null)
                return;

            var sql = new StringBuilder();
            var whereAdded = false;
            foreach (var criterion in criteria) {
                var schemaRow = GetSchemaTableRow(tableNames, criterion.Name);
                var table = GetTableForColumn(tableNames, criterion.Name);
                sql.Append(Environment.NewLine);
                sql.Append(whereAdded ? "AND " : "WHERE ");
                if (schemaRow == null || table == null)
                    sql.Append(criterion.Name.ToSqlString() + " = 'does not map to a column'");
                else
                    sql.Append(table + '.' + criterion.ToString(true, cmd, schemaRow));
                whereAdded = true;
            } // end foreach
            cmd.CommandText += sql;
        } // end method



        private void ApplyDefaultMaxParameters(IDbConnection con) {
            if (MaxParameters != default(int) || con == null)
                return;

            var typeName = con.GetType().FullName;
            // TODO: Put in Oracle's max, which is 1000
            if (typeName == "System.Data.SqlClient.SqlConnection")
                MaxParameters = 2090;
            else if (typeName == "System.Data.SQLite.SQLiteConnection")
                MaxParameters = 999;
            else if (typeName == "System.Data.SqlServerCe.SqlCeConnection")
                MaxParameters = int.MaxValue;
        } // end method



        protected void AttemptTransaction(Action<IDbTransaction> action) {
            IDbTransaction transaction = null;
            try {
                transaction = Connection.BeginTransaction();
            } catch {
                // Do nothing.  Some databases don't support transactions (like Informix)
            } // end try-catch

            try {
                action(transaction);
                if (transaction != null)
                    transaction.Commit();
            } finally {
                if (transaction != null)
                    transaction.Dispose();
            } // end try-finally
        } // end method



        protected static void CheckForConnectionLeaks(int seconds) {
            if (!Debugger.IsAttached)
                return;

            var nl = Environment.NewLine;
            var now = DateTime.UtcNow;
            foreach (var info in connectionInfoMap.Values)
                if ((now - info.CreationDatetime).Seconds > seconds)
                    throw new DataException("An unclosed database connection has been detected." + nl + nl +
                        "This exception is likely due to either 1) a bug in NRepository, " +
                        "2) an undisposed Transaction returned from BeginTransaction, or " +
                        "3) a bug in a class that extends NRepository.  This exception " +
                        "can only thrown when a Debugger is attached to the running process " +
                        "(so it should not appear in a live application)." + nl + nl +
                        "Information About the Unclosed Connection" + nl +
                        "Creation Date / Time: " + info.CreationDatetime + nl +
                        "SQL Command(s) Executed..." + nl +
                        string.Join(Environment.NewLine, info.SqlLog) + nl +
                        "NRepository.CreationStack..." + nl +
                        info.RepositoryCreationStack);
        } // end method



        protected virtual IDbConnection Connection {
            get {
                if (connection == null) {
                    CheckForConnectionLeaks(300);
                    lock (nextConnectionIdLock)
                        connectionId = nextConnectionId++;
                    connection = OpenConnection();
                    ApplyDefaultMaxParameters(connection);
                    Log("Connection " + connectionId + " opened");
                    ConnectionInfo = new ConnectionInfo();
                    ConnectionInfo.Id = connectionId;
                    ConnectionInfo.CreationDatetime = DateTime.UtcNow;
                    ConnectionInfo.RepositoryCreationStack = RepositoryCreationStack;
                    connectionInfoMap.TryAdd(connectionId, ConnectionInfo);
                } // end function
                return connection;
            } // end get
        } // end property



        protected ConnectionInfo ConnectionInfo { get; set; }



        public override long Count(IEnumerable<string> tableNames, IEnumerable<Criterion> criteria) {
            var cursorData = new CursorData();
            cursorData.criteria = criteria;
            var count = 0L;
            var splits = cursorData.SplitLargeCollections(MaxParameters);
            foreach (var split in splits) {
                using (var cmd = CreateSelectCommand(tableNames, split, true)) {
                    var result = cmd.ExecuteScalar();
                    count += (DataTool.AsLong(result) ?? 0);
                } // end using
            } // end foreach
            return count;
        } // end method



        private IDbCommand CreateDeleteCommand(string table, IEnumerable<Criterion> criteria) {
            var cmd = Connection.CreateCommand();
            try {
                cmd.CommandText = "DELETE FROM " + table;
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



        private IDbCommand CreateInsertCommand(string table, IRecord record) {
            var cmd = Connection.CreateCommand();
            try {
                var sql = new StringBuilder("INSERT INTO " + table + Environment.NewLine + "(");
                var valueString = new StringBuilder();
                bool isAfterFirst = false;
                var columns = GetColumns(table);
                foreach (var column in columns) {
                    if (!record.ContainsKey(column))
                        continue;

                    if (isAfterFirst) {
                        sql.Append(", ");
                        valueString.Append(", ");
                    } // end if
                    sql.Append(column);
                    valueString.Append(cmd.FormatParameter(column));
                    cmd.AddParameter(column, record[column],
                        GetSchemaTableRow(new[] { table }, column));
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



        private IDbCommand CreateSelectCommand(IEnumerable<string> tableNames,
            CursorData cursorData, bool countOnly) {
            if (!tableNames.Any())
                throw new ArgumentException("The tableNames parameter was empty.", "tableNames");

            var cmd = Connection.CreateCommand();
            try {
                AddSelectClause(cmd, tableNames, countOnly);
                AddFromClause(cmd, tableNames);
                AddWhereClause(cmd, tableNames, cursorData.criteria);
                AddOrderByClause(cmd, tableNames, cursorData.sort);
                AddPagingClause(cmd, tableNames.FirstOrDefault(), cursorData);
                LogCommand(cmd);
                return cmd;
            } catch {
                // If any exceptions occur, Dispose cmd (since its IDisposable and all)
                // and then let the exception bubble up the stack.
                cmd.Dispose();
                throw;
            } // end try-catch
        } // end method



        private TableDefinition CreateTableDefinition(IEnumerable<IRecord> records) {
            var count = records.Count();
            if (count == 0)
                return null;
            else if (count > 1) {
                var tableName = records.First()["table_name"];
                throw new DataException("The table \"" + tableName +
                    "\" is ambiguous because it is defined in multiple database schemas (" +
                    string.Join(", ", records.Select(record => record["TABLE_SCHEMA"])) +
                    ").  Use the Repository.Map method to explicitly define how " +
                    tableName + " maps to the database.");
            } // end else-if

            var first = records.First();
            return new TableDefinition(first["table_schema"] as string,
                first["table_name"] as string);
        } // end method



        private IDbCommand CreateUpdateCommand(string table,
            IRecord record, IEnumerable<Criterion> criteria) {

            var cmd = Connection.CreateCommand();
            try {
                var sql = new StringBuilder("UPDATE " + table + " SET" + Environment.NewLine);

                var primaryKeyColumns = criteria.Select(c => c.Name.ToUpper());
                bool isAfterFirst = false;
                var columns = GetColumns(table);
                foreach (var column in columns) {
                    if (!record.ContainsKey(column) ||
                        primaryKeyColumns.Contains(column.ToUpper()))
                        continue;

                    if (isAfterFirst)
                        sql.Append("," + Environment.NewLine);
                    sql.Append(column + " = " + cmd.FormatParameter(column));
                    cmd.AddParameter(column, record[column],
                        GetSchemaTableRow(new[] { table }, column));
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



        private static void DefaultLog(string msg) {
            if (Debugger.IsAttached)
                Trace.WriteLine("[XRepository] " + msg);
        } // end method



        public override void Dispose() {
            if (connection != null) {
                connection.Dispose();
                Log("Connection " + connectionId + " closed");
                connectionInfoMap.TryRemove(connectionId);
                CheckForConnectionLeaks(300);
            } // end if

            connection = null;
            ConnectionInfo = null;
        } // end method



        public override void Fetch(IEnumerable<string> tableNames, CursorData cursorData,
            BlockingCollection<IRecord> records) {
            var splits = cursorData.SplitLargeCollections(MaxParameters);
            foreach (var split in splits) {
                using (var cmd = CreateSelectCommand(tableNames, split, false))
                using (var reader = cmd.ExecuteReader())
                foreach (var record in reader.ReadData()) {
                    record["_tableNames"] = tableNames;
                    records.Add(record);
                } // end foreach
            } // end foreach
        } // end method



        private IEnumerable<string> FindColumns(string tableName) {
            var columns = new List<string>();
            var schema = GetSchemaTable(tableName);
            for (int r = 0; r < schema.Rows.Count; r++)
                columns.Add(schema.Rows[r]["ColumnName"] as string);
            return columns;
        } // end method



        private PagingMechanism FindPagingMechanism(string tableName) {
            // Try LimitOffset
            var sql = "SELECT NULL FROM " + tableName + " LIMIT 1 OFFSET 0";
            var pagingMechanism = TryPagingMechanism(sql, PagingMechanism.LimitOffset);
            if (pagingMechanism != null)
                return pagingMechanism.Value;

            // Try OffsetFetchFirst
            var firstColumn = GetSchemaTable(tableName).Rows[0]["ColumnName"];
            sql = "SELECT " + firstColumn + " FROM " + tableName + " ORDER BY " +
                firstColumn + " OFFSET 0 ROWS FETCH FIRST 1 ROWS ONLY";
            pagingMechanism = TryPagingMechanism(sql, PagingMechanism.OffsetFetchFirst);
            if (pagingMechanism != null)
                return pagingMechanism.Value;

            pagingMechanism = PagingMechanism.Programmatic;
            return pagingMechanism.Value;
        } // end method



        private IEnumerable<string> FindPrimaryKeys(string tableName) {
            try {
                return FindPrimaryKeysWithInformationSchema(tableName);
            } catch (DbException ex) {
                string exName = ex.GetType().Name.Remove("Exception");
                try {
                    var def = HandleException<IEnumerable<string>>("FindPrimaryKeys", exName, tableName);
                    return def;
                } catch (MissingMethodException) {
                    // Swallow the MissingMethodException and just throw the original
                } // end try-catch
                throw;
            } // end try-catch
        } // end method



        private IEnumerable<string> FindPrimaryKeys_DB2(string tableName) {
            var tableDef = GetTableDefinition(tableName);
            var sql = "SELECT KCU.COLNAME" + Environment.NewLine +
                "FROM SYSCAT.KEYCOLUSE KCU" + Environment.NewLine +
                "INNER JOIN SYSCAT.TABCONST TC" + Environment.NewLine +
                "ON TC.CONSTNAME = KCU.CONSTNAME" + Environment.NewLine +
                "WHERE TC.TYPE = 'P'" + Environment.NewLine +
                "AND UPPER(TRIM(KCU.TABNAME)) = " + tableDef.TableName.ToSqlString();
            if (!string.IsNullOrEmpty(tableDef.SchemaName))
                sql += Environment.NewLine + "AND UPPER(TRIM(KCU.TABSCHEMA)) = " + tableDef.SchemaName.ToSqlString();
            sql += Environment.NewLine + "ORDER BY KCU.COLNAME";
            Log(sql);
            var results = Connection.ExecuteReader(sql);
            return results.Select(record => record["colname"] as string);
        } // end method



        private IEnumerable<string> FindPrimaryKeys_Ifx(string tableName) {
            var sql = "SELECT cl.colname" + Environment.NewLine +
                "FROM systables t" + Environment.NewLine +
                "INNER JOIN sysconstraints cn" + Environment.NewLine +
                "ON cn.tabid = t.tabid" + Environment.NewLine +
                "INNER JOIN sysindexes i" + Environment.NewLine +
                "ON i.idxname = cn.idxname" + Environment.NewLine +
                "INNER JOIN syscolumns cl" + Environment.NewLine +
                "ON cl.tabid = t.tabid" + Environment.NewLine +
                "AND (cl.colno = i.part1 OR cl.colno = i.part2 OR" + Environment.NewLine +
                "cl.colno = i.part3 OR cl.colno = i.part4 OR" + Environment.NewLine +
                "cl.colno = i.part5 OR cl.colno = i.part6 OR" + Environment.NewLine +
                "cl.colno = i.part7 OR cl.colno = i.part8 OR" + Environment.NewLine +
                "cl.colno = i.part9 OR cl.colno = i.part10 OR" + Environment.NewLine +
                "cl.colno = i.part11 OR cl.colno = i.part12 OR" + Environment.NewLine +
                "cl.colno = i.part13 OR cl.colno = i.part14 OR" + Environment.NewLine +
                "cl.colno = i.part15 OR cl.colno = i.part16)" + Environment.NewLine +
                "WHERE cn.constrtype = 'P'" + Environment.NewLine +
                "AND UPPER(t.tabname) = " + tableName.ToUpper().ToSqlString() + Environment.NewLine +
                "ORDER BY cl.colname";
            Log(sql);
            var results = Connection.ExecuteReader(sql);
            return results.Select(record => record["colname"] as string);
        } // end method



        private IEnumerable<string> FindPrimaryKeys_OleDb(string tableName) {
            var tableDef = GetTableDefinition(tableName);
            var filter = "TABLE_NAME = " + tableDef.TableName.ToSqlString();
            if (!string.IsNullOrEmpty(tableDef.SchemaName))
                filter += "AND TABLE_SCHEMA = " + tableDef.SchemaName.ToSqlString();
            var oleCon = Connection as OleDbConnection;
            var keys = oleCon.GetOleDbSchemaTable(OleDbSchemaGuid.Primary_Keys, null);
            var keyRows = keys.Select(filter);
            return keyRows.Select(r => r["COLUMN_NAME"] as string).OrderBy(cn => cn);
        } // end method



        private IEnumerable<string> FindPrimaryKeys_Oracle(string tableName) {
            var sql = "SELECT acc.column_name" + Environment.NewLine +
                "FROM all_constraints ac" + Environment.NewLine +
                "INNER JOIN all_cons_columns acc" + Environment.NewLine +
                "ON acc.constraint_name = ac.constraint_name" + Environment.NewLine +
                "AND acc.owner = ac.owner" + Environment.NewLine +
                "WHERE ac.constraint_type = 'P'" + Environment.NewLine +
                "AND acc.table_name = " + tableName.ToUpper().ToSqlString() + Environment.NewLine +
                "ORDER BY acc.table_name";
            Log(sql);
            var results = Connection.ExecuteReader(sql);
            return results.Select(record => record["column_name"] as string);
        } // end method



        private IEnumerable<string> FindPrimaryKeys_SQLite(string tableName) {
            var sql = "PRAGMA table_info(" + tableName.ToSqlString() + ")";
            Log(sql);
            var results = Connection.ExecuteReader(sql);
            return results.Where(record => ((long)record["pk"]) == 1)
                .Select(record => record["name"] as string)
                .OrderBy(n => n);
        } // end method



        private IEnumerable<string> FindPrimaryKeysWithInformationSchema(string tableName) {
            var tableDef = GetTableDefinition(tableName);
            var sql = "SELECT kcu.column_name" + Environment.NewLine +
                "FROM information_schema.key_column_usage kcu" + Environment.NewLine +
                "INNER JOIN information_schema.table_constraints tc" + Environment.NewLine +
                "ON (tc.constraint_name = kcu.constraint_name OR" + Environment.NewLine +
                "(tc.constraint_name IS NULL AND kcu.constraint_name IS NULL))" + Environment.NewLine +
                "AND (tc.table_schema = kcu.table_schema OR" + Environment.NewLine +
                "(tc.table_schema IS NULL AND kcu.table_schema IS NULL))" + Environment.NewLine +
                "AND (tc.table_name = kcu.table_name OR" + Environment.NewLine +
                "(tc.table_name IS NULL AND kcu.table_name IS NULL))" + Environment.NewLine +
                "WHERE tc.constraint_type = 'PRIMARY KEY'" + Environment.NewLine +
                "AND UPPER(kcu.table_name) = " + tableDef.TableName.ToSqlString();
            if (!string.IsNullOrEmpty(tableDef.SchemaName))
                sql += Environment.NewLine + "AND UPPER(kcu.table_schema) = " + tableDef.SchemaName.ToSqlString();
            sql += Environment.NewLine + "ORDER BY kcu.column_name";
            Log(sql);
            var results = Connection.ExecuteReader(sql);
            return results.Select(record => record["column_name"] as string);
        } // end method



        private DataTable FindSchemaTable(string tableName) {
            DataTable schema;
            var tableDef = GetTableDefinition(tableName);
            var sql = "SELECT * FROM " + tableDef.FullName + " WHERE 1 = 0";
            using (var cmd = Connection.CreateCommand()) {
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



        private TableDefinition FindTableDefinition(string tableName) {
            try {
                return FindTableDefinitionWithInformationSchema(tableName);
            } catch (DbException ex) {
                string exName = ex.GetType().Name.Remove("Exception");
                try {
                    var def = HandleException<TableDefinition>("FindTableDefinition", exName, tableName);
                    return def;
                } catch (MissingMethodException) {
                    // Swallow the MissingMethodException and just throw the original
                } // end try-catch
                throw;
            } // end try-catch
        } // end method



        private TableDefinition FindTableDefinition_DB2(string tableName) {
            var splitTableName = tableName.Split('.');
            tableName = splitTableName.Last();
            var sql = "SELECT TRIM(CREATOR) AS table_schema, TRIM(NAME) AS table_name" + Environment.NewLine +
                "FROM sysibm.systables WHERE UPPER(TRIM(NAME)) = " + tableName.ToSqlString();
            if (splitTableName.Length > 1)
                sql += Environment.NewLine + "AND UPPER(TRIM(CREATOR)) = " +
                    splitTableName.First().ToSqlString();
            Log(sql);
            return CreateTableDefinition(Connection.ExecuteReader(sql));
        } // end method



        private TableDefinition FindTableDefinition_Ifx(string tableName) {
            var sql = "SELECT CAST(NULL AS VARCHAR) AS table_schema," + Environment.NewLine +
                "tabname AS table_name" + Environment.NewLine +
                "FROM systables WHERE upper(tabname) = " + tableName.ToSqlString();
            Log(sql);
            return CreateTableDefinition(Connection.ExecuteReader(sql));
        } // end method



        private TableDefinition FindTableDefinition_OleDb(string tableName) {
            var oleCon = Connection as OleDbConnection;
            var tables = oleCon.GetOleDbSchemaTable(OleDbSchemaGuid.Tables, null);
            var tableRows = tables.Select("TABLE_NAME = " + tableName.ToSqlString());
            if (tableRows.Length != 1)
                return null;

            var record = new Record(StringComparer.OrdinalIgnoreCase);
            record["table_schema"] = null;
            record["table_name"] = tableRows[0]["TABLE_NAME"];
            return CreateTableDefinition(new[] {record});
        } // end method



        private TableDefinition FindTableDefinition_Oracle(string tableName) {
            var sql = "SELECT NULL AS table_schema, table_name" + Environment.NewLine +
                "FROM all_tables WHERE upper(table_name) = " + tableName.ToSqlString();
            Log(sql);
            return CreateTableDefinition(Connection.ExecuteReader(sql));
        } // end method



        private TableDefinition FindTableDefinition_SQLite(string tableName) {
            var sql = "SELECT NULL AS table_schema, name AS table_name" + Environment.NewLine +
                "FROM sqlite_master WHERE upper(type) = 'TABLE'" + Environment.NewLine +
                "AND upper(table_name) = " + tableName.ToSqlString();
            Log(sql);
            return CreateTableDefinition(Connection.ExecuteReader(sql));
        } // end method



        private TableDefinition FindTableDefinitionWithInformationSchema(string tableName) {
            var splitTableName = tableName.Split('.');
            tableName = splitTableName.Last();
            var sql = "SELECT table_schema, table_name" + Environment.NewLine +
                "FROM information_schema.tables" + Environment.NewLine +
                "WHERE UPPER(table_name) = " + tableName.ToSqlString();
            if (splitTableName.Length > 1)
                sql += Environment.NewLine + "AND UPPER(table_schema) = " +
                    splitTableName.First().ToSqlString();
            Log(sql);
            return CreateTableDefinition(Connection.ExecuteReader(sql));
        } // end method



        protected virtual string FormatLogMessage(IDbCommand cmd) {
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



        public override IEnumerable<string> GetColumns(string tableName) {
            if (tableName == null)
                throw new ArgumentNullException("tableName", "The tableName parameter was null.");

            var info = infoCache[Connection.ConnectionString];
            return info.columnsCache.GetValue(tableName.ToUpper(), FindColumns);
        } // end method



        private IDbCommand GetMappedCommand(IDictionary<string, IDbCommand> commandMap, IEnumerable<string> tableNames,
            IRecord record, IDbTransaction transaction, Func<IDbCommand> createCmdFunc) {
            IDbCommand cmd;
            var tableNamesKey = string.Join("+", tableNames);
            if (commandMap.ContainsKey(tableNamesKey)) {
                cmd = commandMap[tableNamesKey];
                foreach (IDbDataParameter parameter in cmd.Parameters)
                    parameter.Set(record[parameter.ParameterName]);

                // Only explicity log here (when the command exists after parameters
                // are set) because createCmdFunc already logs the command.
                LogCommand(cmd);
            } else {
                cmd = createCmdFunc();
                if (transaction != null)
                    cmd.Transaction = transaction;
                cmd.Prepare();
                commandMap[tableNamesKey] = cmd;
            } // end if-else
            return cmd;
        } // end method



        public virtual PagingMechanism GetPagingMechanism(string tableName) {
            if (tableName == null)
                throw new ArgumentNullException("tableName", "The tableName parameter was null.");

            var info = infoCache[Connection.ConnectionString];
            if (info.pagingMechanism == null)
                info.pagingMechanism = FindPagingMechanism(tableName);
            return info.pagingMechanism.Value;
        } // end method



        private IEnumerable<Criterion> GetPrimaryKeyCriteria(IRecord record, string tableName) {
            var primaryKeys = GetPrimaryKeys(tableName);
            var criteriaMap = new Record(StringComparer.OrdinalIgnoreCase);
            foreach (var key in primaryKeys) {
                if (!record.ContainsKey(key))
                    throw new MissingPrimaryKeyException("The object passed does " +
                        "not specify all the primary keys for " + tableName +
                        " (expected missing key: " + key + ").");
                criteriaMap[key] = record[key];
            } // end foreach
            return Criterion.Create(criteriaMap);
        } // end method



        public override IEnumerable<string> GetPrimaryKeys(string tableName) {
            if (tableName == null)
                throw new ArgumentNullException("tableName", "The tableName parameter was null.");

            var info = infoCache[Connection.ConnectionString];
            return info.primaryKeysCache.GetValue(tableName.ToUpper(), FindPrimaryKeys);
        } // end method



        protected DataTable GetSchemaTable(string tableName) {
            if (tableName == null)
                throw new ArgumentNullException("tableName", "The tableName parameter was null.");

            var info = infoCache[Connection.ConnectionString];
            return info.schemaTableCache.GetValue(tableName.ToUpper(), FindSchemaTable);
        } // end method



        private DataRow GetSchemaTableRow(IEnumerable<string> tableNames, string column) {
            foreach (string tableName in tableNames) {
                var schema = GetSchemaTable(tableName);
                for (int r = 0; r < schema.Rows.Count; r++)
                    if (column.Is(schema.Rows[r]["ColumnName"] as string))
                        return schema.Rows[r];
            } // end for
            return null;
        } // end method



        public override TableDefinition GetTableDefinition(string tableName) {
            if (tableName == null)
                throw new ArgumentNullException("tableName", "The tableName parameter was null.");

            var info = infoCache[Connection.ConnectionString];
            return info.tableDefinitionCache.GetValue(tableName.ToUpper(), FindTableDefinition);
        } // end method



        protected string GetTableForColumn(IEnumerable<string> tableNames, string column) {
            foreach (string tableName in tableNames) {
                var schema = GetSchemaTable(tableName);
                for (int r = 0; r < schema.Rows.Count; r++)
                    if (column.Is(schema.Rows[r]["ColumnName"] as string))
                        return tableName;
            } // end for
            return null;
        } // end method



        private T HandleException<T>(string prefix, string suffix, object parameter) {
            var type = GetType();
            var mirror = mirrorCache[type];
            var methodName = prefix + '_' + suffix;
            var handler = mirror.GetMethod(methodName,
                BindingFlags.Instance | BindingFlags.NonPublic);
            if (handler == null)
                throw new MissingMethodException(type.FullName, methodName);
            return (T)handler.Invoke(this, new[] {parameter});
        } // end method



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



        public int MaxParameters { get; set; }



        public override void Remove(BlockingCollection<IRecord> records) {
            AttemptTransaction(transaction => {
                // Used for storing prepared statements to be used repeatedly
                var cmdMap = new Dictionary<string, IDbCommand>();
                try {
                    // For each object and for each of that objects associate tables,
                    // use the object's primary keys as parameters for a delete query.
                    // Commands created for first Type / tableName combination are
                    // prepared (compiled) and then stored in cmdMap to be used again
                    // for other objects.
                    foreach (var record in records) {
                        var tableNames = record["_tableNames"] as IEnumerable<string>;
                        if (tableNames == null)
                            throw new MissingFieldException("The _tableNames key was not present " +
                                "or not defined as IEnumerable<string> in one or more of the records passed.");
                        foreach (var tableName in tableNames) {
                            var criteria = GetPrimaryKeyCriteria(record, tableName);

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
                                var cmd = GetMappedCommand(cmdMap, new[] {tableName}, criteria.ToDictionary(), transaction,
                                    () => { return CreateDeleteCommand(tableName, criteria); });
                                cmd.ExecuteNonQuery();
                            } // end if-else
                        } // end foreach
                    } // end foreach
                } finally {
                    foreach (var cmd in cmdMap.Values)
                        cmd.Dispose();
                } // end try-finally
            });
        } // end method



        public override void Save(BlockingCollection<IRecord> records) {
            AttemptTransaction(transaction => {
                // Used for storing prepared statements to be used repeatedly
                var countCmdMap = new Dictionary<string, IDbCommand>();
                var insertCmdMap = new Dictionary<string, IDbCommand>();
                var updateCmdMap = new Dictionary<string, IDbCommand>();
                try {
                    foreach (var record in records) {
                        var tableNames = record["_tableNames"] as IEnumerable<string>;
                        if (tableNames == null)
                            throw new MissingFieldException("The _tableNames key was not present " +
                                "or not defined as IEnumerable<string> in one or more of the records passed.");
                        foreach (var tableName in tableNames) {
                            var criteria = GetPrimaryKeyCriteria(record, tableName);
                            var criteriaAndValues = new Record(record);
                            criteriaAndValues.AddRange(criteria.ToDictionary());

                            // If the keys criteria has a null value, then the operator will be
                            // "IS" or "IS NOT" instead of "=" or "<>" making the query usable only
                            // once.  In that case, simply create the cmd, run it, and dispose it.
                            // Otherwise, use the cmdMap.  If the cmd required doesn't exist
                            // in the cmdMap, create it, prepare it, and add it.  If the cmd
                            // does exist, change the parameters to the object's keys.
                            object result;
                            long? count;
                            var cursorData = new CursorData();
                            cursorData.criteria = criteria;
                            if (criteria.HasNullValue()) {
                                using (var cmd = CreateSelectCommand(tableNames, cursorData, true))
                                    result = cmd.ExecuteScalar();
                                count = DataTool.AsLong(result) ?? 0;
                                using (var cmd = count == 0 ?
                                    CreateInsertCommand(tableName, record) :
                                    CreateUpdateCommand(tableName, record, criteria))
                                    cmd.ExecuteNonQuery();
                            } else {
                                IDbCommand cmd = GetMappedCommand(countCmdMap, tableNames, criteria.ToDictionary(), transaction,
                                    () => { return CreateSelectCommand(tableNames, cursorData, true); });
                                result = cmd.ExecuteScalar();
                                count = DataTool.AsLong(result) ?? 0;

                                if (count == 0)
                                    cmd = GetMappedCommand(insertCmdMap, new[] {tableName}, criteriaAndValues, transaction,
                                        () => { return CreateInsertCommand(tableName, record); });
                                else
                                    cmd = GetMappedCommand(updateCmdMap, new[] {tableName}, criteriaAndValues, transaction,
                                        () => { return CreateUpdateCommand(tableName, record, criteria); });
                                cmd.ExecuteNonQuery();
                            } // end if-else
                        } // end foreach
                    } // end foreach
                } finally {
                    foreach (var cmd in countCmdMap.Values)
                        cmd.Dispose();
                    foreach (var cmd in insertCmdMap.Values)
                        cmd.Dispose();
                    foreach (var cmd in updateCmdMap.Values)
                        cmd.Dispose();
                } // end try-finally
            });
        } // end method



        public Sequencer Sequencer {
            get {
                var info = infoCache[Connection.ConnectionString];
                return info.sequencer;
            } // end get
            set {
                var info = infoCache[Connection.ConnectionString];
                info.sequencer = value;
            } // end set
        } // end property



        private PagingMechanism? TryPagingMechanism(string sql, PagingMechanism? paging) {
            try {
                Log(sql);
                Connection.ExecuteReader(sql);
            } catch (DbException) {
                paging = null;
            } // end try-catch
            return paging;
        } // end method

    } // end class
} // end namespace
