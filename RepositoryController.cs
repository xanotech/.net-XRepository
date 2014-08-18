using System;
using System.Collections.Generic;
using System.Data;
using System.Web.Http;
using System.Web.Mvc;
using System.Web.Routing;
using XTools;

namespace XRepository {
    public class RepositoryController : Controller {

        private static IDictionary<string, Func<IDbConnection>> connectionFuncs = new Dictionary<string, Func<IDbConnection>>();



        public ActionResult Count() {
            return null;
        } // end method



        public ActionResult Create() {
            return null;
        } // end method



        public ActionResult Find() {
            return null;
        } // end method



        public ActionResult FindOne() {
            return null;
        } // end method



        public ActionResult Remove() {
            return null;
        } // end method



        public ActionResult Save() {
            return null;
        } // end method



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
            RouteTable.Routes.MapHttpRoute("RepositoryController@" + url,
                url + "/{action}", new { controller = "Repository" });
        } // end method

    } // end class
} // end namespace