using System;
using System.Collections.Generic;
using System.Data;
using System.Text;
using System.Web.Mvc;
using System.Web.Routing;
using XTools;

namespace XRepository {
    [HandleAjaxError]
    public class RepositoryController : Controller {

        private static IDictionary<string, Func<IDbConnection>> connectionFuncs = new Dictionary<string, Func<IDbConnection>>();



        private WebRepositoryAdapter adapter;
        public WebRepositoryAdapter Adapter {
            get {
                if (adapter == null) {
                    var key = ControllerContext.RouteData.Values["controller"].ToString();
                    adapter = new WebRepositoryAdapter(connectionFuncs[key]);
                } // end if
                return adapter;
            } // end get
            protected set {
                adapter = value;
            } // end set
        } // property



        public virtual ActionResult Count(string tableNames, string cursor) {
            return JsonContent(Adapter.Count(tableNames, cursor));
        } // end method



        protected override void Dispose(bool disposing) {
            base.Dispose(disposing);
            Adapter.Dispose();
        } // end method



        public Type ExecutorType {
            get { return Adapter.ExecutorType; }
            set { Adapter.ExecutorType = value; }
        } // end property



        public virtual ActionResult Fetch(string tableNames, string cursor) {
            return JsonContent(Adapter.Fetch(tableNames, cursor));
        } // end method



        public virtual ActionResult GetColumns(string tableName) {
            return JsonContent(Adapter.GetColumns(tableName));
        } // end method



        public virtual ActionResult GetPrimaryKeys(string tableName) {
            return JsonContent(Adapter.GetPrimaryKeys(tableName));
        } // end method



        public virtual ActionResult GetTableDefinition(string tableName) {
            return JsonContent(Adapter.GetTableDefinition(tableName));
        } // end method



        protected virtual ActionResult JsonContent(string jsonText) {
            return Content(jsonText, "application/json", Encoding.UTF8);
        } // end method



        public virtual ActionResult Remove(string data) {
            Adapter.Remove(data);
            return null;
        } // end method



        public virtual ActionResult Save(string data) {
            return JsonContent(Adapter.Save(data));
        } // end method



        public Sequencer Sequencer {
            get { return Adapter.Sequencer; }
            set { Adapter.Sequencer = value; }
        } // end property



        public static void Setup(Func<IDbConnection> openConnectionFunc, string url = "Repository") {
            SetupRoute(url);
            connectionFuncs[url] = openConnectionFunc;
        } // end method



        public static void Setup<T>(string connectionString, string url = "Repository")
            where T : IDbConnection, new() {
            Setup(() => DataTool.OpenConnection<T>(connectionString), url);
        } // end method



        public static void Setup(string connectionStringName, string url = "Repository") {
            Setup(() => DataTool.OpenConnection(connectionStringName), url);
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