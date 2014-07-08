using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
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

        private static Cache<string, DatabaseInfoCache> infoCache = new Cache<string, DatabaseInfoCache>(s => new DatabaseInfoCache());
        private static Cache<Type, Mirror> mirrorCache = new Cache<Type, Mirror>(t => new Mirror(t));

        private IDbConnection connection;
        private Action<string> log;



        internal DatabaseInfo(IDbConnection connection, Action<string> log) {
            this.connection = connection;
            this.log = log;
        } // end constructor



        private PropertyInfo FindIdProperty(Type type) {
            var tableNames = GetTableNames(type);
            if (!tableNames.Any())
                return null;

            var keys = GetPrimaryKeys(tableNames.First());
            if (keys.Count() != 1)
                return null;

            var mirror = GetMirror(type);
            var property = mirror.GetProperty(keys.FirstOrDefault());
            if (typeof(long?).IsAssignableFrom(property.PropertyType))
                return property;
            else
                return null;
        } // end method



        private PagingMechanism FindPagingMechanism(string tableName) {
            // Try LimitOffset
            var sql = "SELECT NULL FROM " + tableName + " LIMIT 1 OFFSET 0";
            var pagingMechanism = TryPagingMechanism(sql, PagingMechanism.LimitOffset);
            if (pagingMechanism != null)
                return pagingMechanism.Value;

            // Try OffsetFetchFirst
            var firstColumn = GetSchemaTable(tableName).Rows[0][0];
            sql = "SELECT " + firstColumn + " FROM " + tableName + " ORDER BY " +
                firstColumn + " OFFSET 0 ROWS FETCH FIRST 1 ROWS ONLY";
            pagingMechanism = TryPagingMechanism(sql, PagingMechanism.OffsetFetchFirst);
            if (pagingMechanism != null)
                return pagingMechanism.Value;

            pagingMechanism = PagingMechanism.Programmatic;
            return pagingMechanism.Value;
        } // end method



        private IEnumerable<string> FindPrimaryKeys(string tableName) {
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
                "AND kcu.table_name = " + tableDef.Item2.ToSqlString();
            if (!string.IsNullOrEmpty(tableDef.Item1))
                sql += Environment.NewLine + "AND kcu.table_schema = " + tableDef.Item1.ToSqlString();
            log(sql);
            IEnumerable<IDictionary<string, object>> results;
            results = connection.ExecuteReader(sql);
            return results.Select(r => r["column_name"] as string).OrderBy(cn => cn);
        } // end method



        private IEnumerable<Reference> FindReferences(Type type) {
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

                    var tableNames = GetTableNames(enumType);
                    if (!tableNames.Any(tn => GetSchemaTable(tn) != null))
                        continue;

                    var reference = new Reference();
                    reference.Property = property;
                    reference.ReferencedType = enumType;
                    reference.ReferencingType = property.DeclaringType;
                    references.Add(reference);
                } else {
                    var tableNames = GetTableNames(property.PropertyType);
                    if (!tableNames.Any(tn => GetSchemaTable(tn) != null))
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
            var splitTableName = tableName.Split('.');
            tableName = splitTableName.Last();
            var sql = "SELECT table_schema, table_name" + Environment.NewLine +
                    "FROM information_schema.tables" + Environment.NewLine +
                    "WHERE table_name = " + tableName.ToSqlString();
            if (splitTableName.Length > 1)
                sql += Environment.NewLine + "AND table_schema = " +
                    splitTableName[splitTableName.Length - 2].ToSqlString();
            log(sql);
            IEnumerable<IDictionary<string, object>> results;
            results = connection.ExecuteReader(sql);

            var count = results.Count();
            if (count == 0)
                return null;
            else if (count > 1)
                throw new DataException("The table \"" + tableName +
                    "\" is ambiguous because it is defined in multiple database schemas (" +
                    string.Join(", ", results.Select(r => r["TABLE_SCHEMA"])) +
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
