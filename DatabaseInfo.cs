using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Data.OleDb;
using System.Linq;
using System.Reflection;
using Xanotech.Tools;

namespace Xanotech.Repository {
    public class DatabaseInfo {

        public enum PagingMechanism {
            Programmatic, // Use code logic to retrieve certain sections of the data
            LimitOffset, // MySQL, PostgreSQL: SELECT * FROM SomeTable LIMIT 10 OFFSET 30
            OffsetFetchFirst // SQL Server: SELECT * FROM SomeTable ORDER BY SomeColumn OFFSET 20 ROWS FETCH FIRST 10 ROWS ONLY
        } // end enum

        private const BindingFlags CaseInsensitiveBinding =
            BindingFlags.IgnoreCase | BindingFlags.Instance | BindingFlags.Public;

        private static Cache<string, DatabaseInfoCache> infoCache = new Cache<string, DatabaseInfoCache>(s => new DatabaseInfoCache());
        private static Cache<Type, Mirror> mirrorCache = new Cache<Type, Mirror>(t => new Mirror(t));

        private IDbConnection connection;
        private Action<string> log;



        internal DatabaseInfo(IDbConnection connection, Action<string> log) {
            this.connection = connection;
            this.log = log;
        } // end constructor



        private Reference CreateReference(PropertyInfo property) {
            // If a property is an Array, if it is "basic", or if
            // it's ready only, then it can't be a reference property.
            if (property.PropertyType.IsArray ||
                property.PropertyType.IsBasic() ||
                property.GetSetMethod() == null)
                return null;

            string prefix;
            Type referencedType, primaryType, foreignType;
            var isMany = true;
            if (property.PropertyType.IsGenericType &&
                property.PropertyType.GetGenericTypeDefinition() == typeof(IEnumerable<>)) {
                referencedType = property.PropertyType.GetGenericArguments()[0];
                if (referencedType.IsBasic())
                    return null;

                primaryType = property.DeclaringType;
                foreignType = referencedType;
                prefix = primaryType.Name;
            } else {
                isMany = false;
                referencedType = property.PropertyType;
                primaryType = property.PropertyType;
                foreignType = property.DeclaringType;
                prefix = property.Name;
            } // end if

            // Validate the referencedType and make sure that at least
            // one of the tableNames (if it has any) has a SchemaTable
            // demonstrating that it one or more database tables.
            var tableNames = GetTableNames(referencedType);
            if (tableNames.All(tn => GetSchemaTable(tn) == null))
                return null;

            var keyProperty = FindReferenceKeyProperty(prefix, primaryType, foreignType);
            if (keyProperty == null)
                return null;

            var reference = new Reference();
            reference.IsMany = isMany;
            reference.KeyProperty = keyProperty;
            reference.ValueProperty = property;
            reference.ReferencedType = referencedType;
            return reference;
        } // end Method



        private PropertyInfo FindIdProperty(Type type) {
            var keys = GetPrimaryKeys(type);
            if (keys.Count() != 1)
                return null;

            var mirror = GetMirror(type);
            return mirror.GetProperty(keys.FirstOrDefault(), CaseInsensitiveBinding);
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
            } catch (OleDbException e) {
                // Special MSAccess logic
                if (e.Message.Contains("information_schema"))
                    return FindPrimaryKeysWithOleDbSchemaTable(tableName);
                else
                    throw;
            } catch (DbException e) {
                // Only SQLite would throw a SQLiteException
                if (e.GetType().Name == "SQLiteException")
                    return FindPrimaryKeysForSqlite(tableName);
                // Oracle does not support the ANSI standard information_schema
                // and errors with ORA-00942: table or view does not exist
                else if (e.Message.StartsWith("ORA-00942"))
                    return FindPrimaryKeysForOracle(tableName);
                else
                    throw;
            } // end try-catch
        } // end method



        private IEnumerable<string> FindPrimaryKeysForOracle(string tableName) {
            var sql = "SELECT acc.column_name" + Environment.NewLine +
                "FROM all_constraints ac" + Environment.NewLine +
                "INNER JOIN all_cons_columns acc" + Environment.NewLine +
                "ON acc.constraint_name = ac.constraint_name" + Environment.NewLine +
                "AND acc.owner = ac.owner" + Environment.NewLine +
                "WHERE ac.constraint_type = 'P'" + Environment.NewLine +
                "AND acc.table_name = " + tableName.ToUpper().ToSqlString() + Environment.NewLine +
                "ORDER BY acc.table_name";
            log(sql);
            var results = connection.ExecuteReader(sql);
            return results.Select(record => record["column_name"] as string);
        } // end method



        private IEnumerable<string> FindPrimaryKeysForSqlite(string tableName) {
            var sql = "PRAGMA table_info(" + tableName.ToSqlString() + ")";
            log(sql);
            var results = connection.ExecuteReader(sql);
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
                "AND UPPER(kcu.table_name) = " + tableDef.Item2.ToUpper().ToSqlString();
            if (!string.IsNullOrEmpty(tableDef.Item1))
                sql += Environment.NewLine + "AND UPPER(kcu.table_schema) = " + tableDef.Item1.ToUpper().ToSqlString();
            sql += Environment.NewLine + "ORDER BY kcu.column_name";
            log(sql);
            var results = connection.ExecuteReader(sql);
            return results.Select(record => record["column_name"] as string);
        } // end method



        private IEnumerable<string> FindPrimaryKeysWithOleDbSchemaTable(string tableName) {
            var tableDef = GetTableDefinition(tableName);
            var filter = "TABLE_NAME = " + tableDef.Item2.ToSqlString();
            if (!string.IsNullOrEmpty(tableDef.Item1))
                filter += "AND TABLE_SCHEMA = " + tableDef.Item1.ToSqlString();
            var oleCon = connection as OleDbConnection;
            var keys = oleCon.GetOleDbSchemaTable(OleDbSchemaGuid.Primary_Keys, null);
            var keyRows = keys.Select(filter);
            return keyRows.Select(r => r["COLUMN_NAME"] as string).OrderBy(cn => cn);
        } // end method



        private PropertyInfo FindReferenceKeyProperty(string prefix, Type primaryType, Type foreignType) {
            var keys = GetPrimaryKeys(primaryType);
            if (keys.Count() != 1)
                return null;

            var tableNames = GetTableNames(primaryType);
            var keyName = keys.First();
            foreach (var name in tableNames)
                keyName = keyName.RemoveIgnoreCase(name); // TODO: Make this case-insensitive
            keyName = prefix + keyName;

            var mirror = GetMirror(foreignType);
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



        private DataTable FindSchemaTable(string tableName) {
            DataTable schema;
            var sql = "SELECT * FROM " + tableName + " WHERE 1 = 0";
            using (var cmd = connection.CreateCommand()) {
                cmd.CommandText = sql;
                log(sql);
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



        private Tuple<string, string, string> FindTableDefinition(string tableName) {
            IEnumerable<IDictionary<string, object>> results;
            try {
                results = FindTableDefinitionWithInformationSchema(tableName);
            } catch (OleDbException e) {
                // Special MSAccess logic
                if (e.Message.Contains("information_schema"))
                    results = FindTableDefinitionWithOleDbSchemaTable(tableName);
                else
                    throw;
            } catch (DbException e) {
                // Only SQLite would throw a SQLiteException
                if (e.GetType().Name == "SQLiteException")
                    results = FindTableDefinitionForSqlite(tableName);
                // Oracle does not support the ANSI standard information_schema
                // and errors with ORA-00942: table or view does not exist
                else if (e.Message.StartsWith("ORA-00942"))
                    results = FindTableDefinitionForOracle(tableName);
                else
                    throw;
            } // end try-catch

            var count = results.Count();
            if (count == 0)
                return null;
            else if (count > 1)
                throw new DataException("The table \"" + tableName +
                    "\" is ambiguous because it is defined in multiple database schemas (" +
                    string.Join(", ", results.Select(record => record["TABLE_SCHEMA"])) +
                    ").  Use the Repository.Map method to explicitly define how " +
                    tableName + " maps to the database.");

            var first = results.First();

            tableName = first["table_name"] as string;
            var schemaName = first["table_schema"] as string;
            var fullName = tableName;
            if (!string.IsNullOrEmpty(schemaName))
                fullName = schemaName + "." + tableName;
            return new Tuple<string, string, string>(schemaName, tableName, fullName);
        } // end method



        private IEnumerable<IDictionary<string, object>> FindTableDefinitionForOracle(string tableName) {
            var sql = "SELECT NULL AS table_schema, table_name" + Environment.NewLine +
                "FROM all_tables WHERE upper(table_name) = " + tableName.ToSqlString();
            log(sql);
            return connection.ExecuteReader(sql);
        } // end method



        private IEnumerable<IDictionary<string, object>> FindTableDefinitionForSqlite(string tableName) {
            var sql = "SELECT NULL AS table_schema, name AS table_name" + Environment.NewLine +
                "FROM sqlite_master WHERE upper(type) = 'TABLE'" + Environment.NewLine +
                "AND upper(table_name) = " + tableName.ToSqlString();
            log(sql);
            return connection.ExecuteReader(sql);
        } // end method



        private IEnumerable<IDictionary<string, object>> FindTableDefinitionWithInformationSchema(string tableName) {
            var splitTableName = tableName.Split('.');
            tableName = splitTableName.Last();
            var sql = "SELECT table_schema, table_name" + Environment.NewLine +
                "FROM information_schema.tables" + Environment.NewLine +
                "WHERE UPPER(table_name) = " + tableName.ToSqlString();
            if (splitTableName.Length > 1)
                sql += Environment.NewLine + "AND UPPER(table_schema) = " +
                    splitTableName[splitTableName.Length - 2].ToSqlString();
            log(sql);
            return connection.ExecuteReader(sql);
        } // end method



        private IEnumerable<IDictionary<string, object>> FindTableDefinitionWithOleDbSchemaTable(string tableName) {
            var oleCon = connection as OleDbConnection;
            var tables = oleCon.GetOleDbSchemaTable(OleDbSchemaGuid.Tables, null);
            var tableRows = tables.Select("TABLE_NAME = " + tableName.ToSqlString());
            if (tableRows.Length != 1)
                return null;

            var record = new Dictionary<string, object>();
            record["table_schema"] = null;
            record["table_name"] = tableRows[0]["TABLE_NAME"];
            return new[] {record};
        } // end method



        private IEnumerable<string> FindTableNames(Type type) {
            var tableNameList = new List<string>();
            while (type != typeof(object)) {
                var tableDef = GetTableDefinition(type.Name);
                if (tableDef != null)
                    tableNameList.Add(tableDef.Item3);
                type = type.BaseType;
            } // end while
            tableNameList.Reverse();
            return tableNameList;
        } // end method



        public PropertyInfo GetIdProperty(Type type) {
            if (type == null)
                throw new ArgumentNullException("type", "The type parameter was null.");

            var info = infoCache[connection.ConnectionString];
            return info.idPropertyCache.GetValue(type, FindIdProperty);
        } // end method



        public Mirror GetMirror(Type type) {
            return mirrorCache[type];
        } // end method



        public PagingMechanism GetPagingMechanism(string tableName) {
            if (tableName == null)
                throw new ArgumentNullException("tableName", "The tableName parameter was null.");

            var info = infoCache[connection.ConnectionString];
            if (info.pagingMechanism == null)
                info.pagingMechanism = FindPagingMechanism(tableName);
            return info.pagingMechanism.Value;
        } // end method



        public IEnumerable<string> GetPrimaryKeys(string tableName) {
            if (tableName == null)
                throw new ArgumentNullException("tableName", "The tableName parameter was null.");

            var info = infoCache[connection.ConnectionString];
            return info.primaryKeysCache.GetValue(tableName.ToUpper(), FindPrimaryKeys);
        } // end method



        public IEnumerable<string> GetPrimaryKeys(Type type) {
            if (type == null)
                throw new ArgumentNullException("type", "The type parameter was null.");

            var info = infoCache[connection.ConnectionString];
            var tableNames = GetTableNames(type);
            if (tableNames.Any())
                return GetPrimaryKeys(tableNames.Last());
            else
                return new string[0];
        } // end method



        internal IEnumerable<Reference> GetReferences(Type type) {
            if (type == null)
                throw new ArgumentNullException("type", "The type parameter was null.");

            var info = infoCache[connection.ConnectionString];
            return info.referencesCache.GetValue(type, FindReferences);
        } // end method



        public DataTable GetSchemaTable(string tableName) {
            if (tableName == null)
                throw new ArgumentNullException("tableName", "The tableName parameter was null.");

            var info = infoCache[connection.ConnectionString];
            return info.schemaTableCache.GetValue(tableName.ToUpper(), FindSchemaTable);
        } // end method



        public Tuple<string, string, string> GetTableDefinition(string tableName) {
            if (tableName == null)
                throw new ArgumentNullException("tableName", "The tableName parameter was null.");

            var info = infoCache[connection.ConnectionString];
            return info.tableDefinitionCache.GetValue(tableName.ToUpper(), FindTableDefinition);
        } // end method



        public IEnumerable<string> GetTableNames(Type type) {
            if (type == null)
                throw new ArgumentNullException("type", "The type parameter was null.");

            var info = infoCache[connection.ConnectionString];
            return info.tableNamesCache.GetValue(type, FindTableNames);
        } // end method



        public Sequencer Sequencer {
            get {
                var info = infoCache[connection.ConnectionString];
                return info.sequencer;
            } // end get
            internal set {
                var info = infoCache[connection.ConnectionString];
                info.sequencer = value;
            } // end set
        } // end property



        private PagingMechanism? TryPagingMechanism(string sql, PagingMechanism? paging) {
            try {
                log(sql);
                connection.ExecuteReader(sql);
            } catch (DbException) {
                paging = null;
            } // end try-catch
            return paging;
        } // end method

    } // end class
} // end namespace
