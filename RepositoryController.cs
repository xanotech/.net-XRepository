﻿using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Web.Mvc;
using System.Web.Routing;
using XTools;

namespace XRepository {
    [HandleAjaxError]
    public class RepositoryController : Controller {

        private static IDictionary<string, Func<IDbConnection>> connectionFuncs = new Dictionary<string, Func<IDbConnection>>();

        private static IDictionary<string, Type> executorTypes = new Dictionary<string, Type>();

        private static Cache<string, ConcurrentSet<Type>> pendingInterceptorTypes =
            new Cache<string, ConcurrentSet<Type>>(s => new ConcurrentSet<Type>());



        private WebRepositoryAdapter adapter;
        public WebRepositoryAdapter Adapter {
            get {
                var path = ControllerContext.HttpContext.Request.AppRelativeCurrentExecutionFilePath;
                var splitPath = path.Split('/');
                path = splitPath[splitPath.Length - 2];

                if (adapter == null)
                    adapter = new WebRepositoryAdapter(connectionFuncs[path]);

                if (executorTypes.ContainsKey(path))
                    adapter.ExecutorType = executorTypes[path];

                var interceptorTypes = pendingInterceptorTypes[path];
                if (interceptorTypes.Any()) {
                    foreach (var type in interceptorTypes)
                        adapter.RegisterInterceptor(type);
                    interceptorTypes.Clear();
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



        public static void RegisterInterceptor<T>() where T : Interceptor {
            RegisterInterceptor(typeof(T));
        } // end method



        public static void RegisterInterceptor(Type interceptorType, string path = "Repository") {
            pendingInterceptorTypes[path].TryAdd(interceptorType);
        } // end method



        public static void RegisterInterceptors(Assembly assembly, string path = "Repository") {
            if (assembly == null)
                throw new ArgumentNullException("assembly");

            foreach (var type in assembly.GetTypes())
                RegisterInterceptor(type, path);
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



        public static void SetExecutorType<T>(string path = "Repository") {
            SetExecutorType(typeof(T), path);
        } // end property



        public static void SetExecutorType(Type type, string path = "Repository") {
            executorTypes[path] = type;
        } // end method



        public static void Setup(Func<IDbConnection> openConnectionFunc, string path = "Repository") {
            SetupRoute(path);
            connectionFuncs[path] = openConnectionFunc;
        } // end method



        public static void Setup<T>(string connectionString, string path = "Repository")
            where T : IDbConnection, new() {
            Setup(() => DataTool.OpenConnection<T>(connectionString), path);
        } // end method



        public static void Setup(string connectionStringName, string path = "Repository") {
            Setup(() => DataTool.OpenConnection(connectionStringName), path);
        } // end method



        private static void SetupRoute(string path) {
            if (path.Contains("{action}"))
                throw new ArgumentException("The parameter cannot contain \"{action}\" " +
                    "because all requests must route to the appropriate actions within RepositoryController.", "path");

            if (path.Contains("{controller}"))
                throw new ArgumentException("The parameter cannot contain \"{controller}\" " +
                    "because all requests must route to the RepositoryController.", "path");

            path = path.Trim('/');
            RouteTable.Routes.MapRoute("RepositoryController@" + path,
                path + "/{action}", new { controller = "Repository" });
        } // end method

    } // end class
} // end namespace