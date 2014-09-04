using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.Linq;
using System.Web.Http;
using System.Web.Mvc;
using System.Web.Routing;
using XTools;

namespace XRepository {
    public class RepositoryController : Controller {

        private static IDictionary<string, Func<IDbConnection>> connectionFuncs = new Dictionary<string, Func<IDbConnection>>();



        private IDictionary<string, object> ApplySequenceId(JObject jObj, string tableName) {
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



        public JsonResult Count(string tableNames, string cursor) {
            var tableNamesEnum = ParseTableNames(tableNames);
            var cursorData = JsonConvert.DeserializeObject<CursorData>(cursor);
            var count = Executor.Count(tableNamesEnum, cursorData.criteria);
            return Json(count, JsonRequestBehavior.AllowGet);
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



        protected override void Dispose(bool disposing) {
            base.Dispose(disposing);
            Executor.Dispose();
        } // end method



        private IExecutor executor;
        protected IExecutor Executor {
            get {
                if (executor == null) {
                    var key = ControllerContext.RouteData.Values["controller"].ToString();
                    var dbExec = new DatabaseExecutor(connectionFuncs[key]);
                    executor = dbExec;
                } // end if
                return executor;
            } // end get
            private set {
                executor = value;
            } // end set
        } // property



        public ActionResult Fetch(string tableNames, string cursor) {
            var tableNamesEnum = ParseTableNames(tableNames);
            var cursorData = JsonConvert.DeserializeObject<CursorData>(cursor);

            var objectValuesList = new List<IDictionary<string, object>>();
            var objects = new BlockingCollection<IDictionary<string, object>>();
            Executor.Fetch(tableNamesEnum, cursorData, objects);
            FixDates(objects);
            return Json(objects, JsonRequestBehavior.AllowGet);
        } // end method



        private static void FixDates(IEnumerable<IDictionary<string, object>> objects) {
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



        public JsonResult GetColumns(string tableName) {
            return Json(Executor.GetColumns(tableName), JsonRequestBehavior.AllowGet);
        } // end method



        public JsonResult GetPrimaryKeys(string tableName) {
            return Json(Executor.GetPrimaryKeys(tableName), JsonRequestBehavior.AllowGet);
        } // end method



        public JsonResult GetTableDefinition(string tableName) {
            return Json(Executor.GetTableDefinition(tableName), JsonRequestBehavior.AllowGet);
        } // end method



        private IEnumerable<string> GetTableNames(JObject jObj) {
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



        private IDictionary<string, object> GetValues(JObject jObj, IEnumerable<string> tableNames) {
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



        private IEnumerable<string> ParseTableNames(string tableNames) {
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



        public ActionResult Remove(string data) {
            var records = new BlockingCollection<IDictionary<string, object>>();
            var jObjList = CreateJObjectList(JToken.Parse(data));
            foreach (JObject jObj in jObjList)
                records.Add(CreateDatabaseRecord(jObj));
            Executor.Remove(records);

            return null;
        } // end method



        public ActionResult Save(string data) {
            Response.Write("[");
            bool afterFirst = false;

            var records = new BlockingCollection<IDictionary<string, object>>();
            var jObjList = CreateJObjectList(JToken.Parse(data));
            foreach (JObject jObj in jObjList) {
                var tableNames = GetTableNames(jObj);
                var idRecord = ApplySequenceId(jObj, tableNames.First());
                if (afterFirst)
                    Response.Write(",");
                Response.Write(Environment.NewLine);
                Response.Write(JsonConvert.SerializeObject(idRecord));
                afterFirst = true;
                records.Add(CreateDatabaseRecord(jObj));
            } // end foreach
            Executor.Save(records);

            Response.Write(Environment.NewLine + "]");
            return null;
        } // end method



        public Sequencer Sequencer {
            get {
                var dbExec = Executor as DatabaseExecutor;
                if (dbExec == null)
                    return null;

                if (dbExec.Sequencer == null) {
                    var key = ControllerContext.RouteData.Values["controller"].ToString();
                    dbExec.Sequencer = new Sequencer(connectionFuncs[key]);
                } // end if
                return dbExec.Sequencer;
            } // end get
            set {
                var dbExec = Executor as DatabaseExecutor;
                if (dbExec != null && value != null)
                    dbExec.Sequencer = value;
            } // end set
        } // end property



        public static void Setup<T>(string connectionString, string url = "Repository")
            where T : IDbConnection, new() {
            SetupRoute(url);
            connectionFuncs[url] = () => DataTool.OpenConnection<T>(connectionString);
        } // end method



        public static void Setup(string connectionStringName, string url = "Repository") {
            SetupRoute(url);
            connectionFuncs[url] = () => DataTool.OpenConnection(connectionStringName);
        } // end method



        private static void SetupRoute(string url) {
            if (url.Contains("{action}"))
                throw new ArgumentException("The parameter cannot contain \"{action}\" " +
                    "because all requests must route to the appropriate actions within RepositoryController.", "url");

            if (url.Contains("{controller}"))
                throw new ArgumentException("The parameter cannot contain \"{controller}\" " +
                    "because all requests must route to the RepositoryController.", "url");

            url = url.Trim('/');
            RouteTable.Routes.MapRoute("RepositoryController@" + url,
                url + "/{action}", new { controller = "Repository" });
        } // end method

    } // end class
} // end namespace