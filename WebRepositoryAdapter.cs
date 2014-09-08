using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using XTools;

namespace XRepository {
    public class WebRepositoryAdapter : IDisposable {

        private Func<IDbConnection> openConnectionFunc;



        public WebRepositoryAdapter(Func<IDbConnection> openConnection) {
            openConnectionFunc = openConnection;
        } // end constructor



        public WebRepositoryAdapter(string connectionStringName) :
            this(() => { return DataTool.OpenConnection(connectionStringName); }) {
        } // end constructor



        protected IDictionary<string, object> ApplySequenceId(JObject jObj, string tableName) {
            if (Sequencer == null)
                return null;

            var keys = Executor.GetPrimaryKeys(tableName);
            if (keys.Count() != 1)
                return null;
            var key = keys.First();

            JToken jTok;
            if (jObj.TryGetValue(key, out jTok) &&
                jTok.Type != JTokenType.Null &&
                jTok.Type != JTokenType.Undefined)
                return null;

            var id = Sequencer.GetNextValue(tableName, key);
            jObj[key] = id;
            var idRecord = new Dictionary<string, object>(StringComparer.OrdinalIgnoreCase);
            idRecord[key] = id;
            return idRecord;
        } // end method



        public string Count(string tableNames, string cursor) {
            var tableNamesEnum = ParseTableNames(tableNames);
            var cursorData = JsonConvert.DeserializeObject<CursorData>(cursor);
            var count = Executor.Count(tableNamesEnum, cursorData.criteria);
            return JsonConvert.SerializeObject(count);
        } // end method



        public static WebRepositoryAdapter Create<T>(string connectionString) where T : IDbConnection, new() {
            return new WebRepositoryAdapter(() => { return DataTool.OpenConnection<T>(connectionString); });
        } // end method



        public static WebRepositoryAdapter Create(Func<IDbConnection> openConnection) {
            return new WebRepositoryAdapter(openConnection);
        } // end method



        public static WebRepositoryAdapter Create(string connectionStringName) {
            return new WebRepositoryAdapter(connectionStringName);
        } // end method



        protected IDictionary<string, object> CreateDatabaseRecord(JObject jObj) {
            var tableNames = GetTableNames(jObj);
            var record = GetValues(jObj, tableNames);
            record["_tableNames"] = tableNames;
            return record;
        } // end method



        protected IList<JObject> CreateJObjectList(JToken jTok) {
            var jObjList = new List<JObject>();

            if (jTok.Type == JTokenType.Array)
                foreach (JToken element in jTok)
                    jObjList.Add(element as JObject);

            if (jTok.Type == JTokenType.Object)
                jObjList.Add(jTok as JObject);

            return jObjList;
        } // end method



        public void Dispose() {
            Executor.Dispose();
        } // end method



        private IExecutor executor;
        protected IExecutor Executor {
            get {
                if (executor == null) {
                    var dbExec = new DatabaseExecutor(openConnectionFunc);
                    executor = dbExec;
                } // end if
                return executor;
            } // end get
            private set {
                executor = value;
            } // end set
        } // property



        public string Fetch(string tableNames, string cursor) {
            var tableNamesEnum = ParseTableNames(tableNames);
            var cursorData = JsonConvert.DeserializeObject<CursorData>(cursor);

            var objectValuesList = new List<IDictionary<string, object>>();
            var objects = new BlockingCollection<IDictionary<string, object>>();
            Executor.Fetch(tableNamesEnum, cursorData, objects);
            FixDates(objects);
            return JsonConvert.SerializeObject(objects);
        } // end method



        protected static void FixDates(IEnumerable<IDictionary<string, object>> objects) {
            foreach (var obj in objects) {
                var isoDates = new Dictionary<string, string>();
                foreach (var key in obj.Keys) {
                    var dateTime = obj[key] as DateTime?;
                    if (dateTime != null)
                        isoDates[key] = dateTime.Value.ToString("yyyy-MM-ddTHH:mm:ssZ");
                } // end foreach
                foreach (var key in isoDates.Keys)
                    obj[key] = isoDates[key];
            } // end foreach
        } // end method



        public string GetColumns(string tableName) {
            return JsonConvert.SerializeObject(Executor.GetColumns(tableName));
        } // end method



        public string GetPrimaryKeys(string tableName) {
            return JsonConvert.SerializeObject(Executor.GetPrimaryKeys(tableName));
        } // end method



        public string GetTableDefinition(string tableName) {
            return JsonConvert.SerializeObject(Executor.GetTableDefinition(tableName));
        } // end method



        protected IEnumerable<string> GetTableNames(JObject jObj) {
            var tableNames = new List<string>();
            JToken jTok;
            if (!jObj.TryGetValue("_tableNames", out jTok))
                throw new MissingFieldException("The _tableNames property was not present in " +
                    "one or more of the objects passed (json = " + jObj.ToString() + ").");
            if (jTok.Type != JTokenType.Array)
                throw new FormatException("The _tableNames property was not an array (json = " +
                    jTok.ToString() + ").");
            foreach (JToken tableNameElement in jTok) {
                if (tableNameElement.Type != JTokenType.String)
                    throw new FormatException("The _tableNames property contained an " +
                        "element that was not a string (json = " + jTok.ToString() + ").");
                var tableName = tableNameElement.Value<string>();
                var tableDef = Executor.GetTableDefinition(tableName);
                if (tableDef == null)
                    throw new DataException("The table \"" + tableName + "\" is not a valid table.");
                tableNames.Add(tableDef.FullName);
            } // end foreach
            return tableNames;
        } // end method



        protected IDictionary<string, object> GetValues(JObject jObj, IEnumerable<string> tableNames) {
            var processedColumns = new Dictionary<string, bool>(StringComparer.OrdinalIgnoreCase);
            foreach (var tableName in tableNames)
                foreach (var column in Executor.GetColumns(tableName))
                    processedColumns[column.ToUpper()] = false;

            var values = new Dictionary<string, object>(StringComparer.OrdinalIgnoreCase);
            foreach (KeyValuePair<string, JToken> property in jObj) {
                string propertyName = property.Key.ToUpper();
                if (!processedColumns.ContainsKey(propertyName) || processedColumns[propertyName])
                    continue;

                JArray jArray = property.Value as JArray;
                if (jArray == null)
                    values[property.Key] = (property.Value as JValue).Value;
                else {
                    object[] array = new object[jArray.Count];
                    int a = 0;
                    foreach (JToken jTok in jArray)
                        array[a++] = (jTok as JValue).Value;
                    values[property.Key] = array;
                } // end if-else
                processedColumns[propertyName] = true;
            } // end foreach

            return values;
        } // end method



        public Action<string> Log {
            get {
                var dbExec = Executor as DatabaseExecutor;
                if (dbExec == null)
                    return null;
                return dbExec.Log;
            } // end get
            set {
                var dbExec = Executor as DatabaseExecutor;
                if (dbExec != null)
                    dbExec.Log = value;
            } // end set
        } // end property



        protected IEnumerable<string> ParseTableNames(string tableNames) {
            var tableNamesList = new List<string>();
            if (string.IsNullOrEmpty(tableNames))
                throw new ArgumentNullException("tableNames", "The tableNames parameter is null.");
            var jTok = JToken.Parse(tableNames);
            if (jTok.Type == JTokenType.String)
                tableNamesList.Add(jTok.Value<string>());
            else if (jTok.Type == JTokenType.Array)
                foreach (JToken tableNameElement in jTok) {
                    if (tableNameElement.Type != JTokenType.String)
                        throw new FormatException("The tableNames parameter contained an " +
                            "element that was not a string (json = " + jTok.ToString() + ").");
                    var tableName = tableNameElement.Value<string>();
                    var tableDef = Executor.GetTableDefinition(tableName);
                    if (tableDef == null)
                        throw new DataException("The table \"" + tableName + "\" is not a valid table.");
                    tableNamesList.Add(tableDef.FullName);
                } // end foreach
            else
                return null;
            return tableNamesList;
        } // end method



        public void Remove(string data) {
            var records = new BlockingCollection<IDictionary<string, object>>();
            var jObjList = CreateJObjectList(JToken.Parse(data));
            foreach (JObject jObj in jObjList)
                records.Add(CreateDatabaseRecord(jObj));
            Executor.Remove(records);
        } // end method



        public string Save(string data) {
            var idRecords = new List<IDictionary<string, object>>();
            var records = new BlockingCollection<IDictionary<string, object>>();
            var jObjList = CreateJObjectList(JToken.Parse(data));
            foreach (JObject jObj in jObjList) {
                var tableNames = GetTableNames(jObj);
                var idRecord = ApplySequenceId(jObj, tableNames.First());
                idRecords.Add(idRecord);
                records.Add(CreateDatabaseRecord(jObj));
            } // end foreach
            Executor.Save(records);
            return JsonConvert.SerializeObject(idRecords);;
        } // end method



        public Sequencer Sequencer {
            get {
                var dbExec = Executor as DatabaseExecutor;
                if (dbExec == null)
                    return null;

                if (dbExec.Sequencer == null)
                    dbExec.Sequencer = new Sequencer(openConnectionFunc);
                return dbExec.Sequencer;
            } // end get
            set {
                var dbExec = Executor as DatabaseExecutor;
                if (dbExec != null && value != null)
                    dbExec.Sequencer = value;
            } // end set
        } // end property

    } // end class
} // end namespace