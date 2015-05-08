using System;
using System.Collections.Generic;
using System.Data;
using System.Reflection;
using System.Web;
using XTools;

namespace XRepository {
    using IRecord = IDictionary<string, object>;

    public class RepositoryBase {

        private static Cache<string, ConcurrentSet<Type>> interceptorTypeCache =
            new Cache<string, ConcurrentSet<Type>>(s => new ConcurrentSet<Type>());
        
        private Executor executor;



        private string connectionString;
        public string ConnectionString {
            get {
                if (connectionString == null)
                    using (var con = OpenConnection())
                        connectionString = con.ConnectionString;
                return connectionString;
            } // end get
            protected set {
                connectionString = value;
            } // end set
        } // end 



        public string CreationStack { get; protected set; }



        protected Executor Executor {
            get {
                if (executor == null) {
                    var type = ExecutorType ?? typeof(DatabaseExecutor);
                    var constructor = type.GetConstructor(Type.EmptyTypes);
                    var dbExec = constructor.Invoke(null) as Executor;
                    dbExec.OpenConnection = OpenConnection;
                    dbExec.RepositoryCreationStack = CreationStack;
                    executor = dbExec;
                } // end if
                return executor;
            } // end get
            private set {
                executor = value;
            } // end set
        } // end property



        private Type executorType;
        public Type ExecutorType {
            get {
                if (executor != null)
                    return executor.GetType();

                return executorType;
            } // end get
            set {
                if (executor != null)
                    return;

                if (!typeof(Executor).IsAssignableFrom(value))
                    value = null;
                executorType = value;
            } // end set
        } // end property


    
        public HttpContextBase HttpContext { get; set; }



        private IEnumerable<Interceptor> interceptors;
        protected IEnumerable<Interceptor> Interceptors {
            get {
                if (interceptors != null)
                    return interceptors;
                    
                var list = new List<Interceptor>();
                foreach (var type in interceptorTypeCache[ConnectionString]) {
                    var interceptor = Activator.CreateInstance(type) as Interceptor;
                    if (interceptor == null)
                        continue;

                    interceptor.Executor = Executor;
                    interceptor.HttpContext = HttpContext;
                    list.Add(interceptor);
                } // end foreach
                interceptors = list;

                return interceptors;
            } // end get
            private set {
                interceptors = value;
            } // end set
        } // end method



        protected virtual void InvokeCountInterceptors(IEnumerable<string> tableNames, IEnumerable<Criterion> criteria) {
            foreach (var interceptor in Interceptors)
                if (interceptor.IsMatch(tableNames))
                    interceptor.InterceptCount(tableNames, criteria);
        } // end method



        protected virtual void InvokeFindInterceptors(IEnumerable<string> tableNames, IEnumerable<Criterion> criteria) {
            foreach (var interceptor in Interceptors)
                if (interceptor.IsMatch(tableNames))
                    interceptor.InterceptFind(tableNames, criteria);
        } // end method



        protected virtual void InvokeRemoveInterceptors(IEnumerable<string> tableNames, IRecord record) {
            foreach (var interceptor in Interceptors)
                if (interceptor.IsMatch(tableNames))
                    interceptor.InterceptRemove(record);
        } // end method



        protected virtual void InvokeSaveInterceptors(IEnumerable<string> tableNames, IRecord record) {
            foreach (var interceptor in Interceptors)
                if (interceptor.IsMatch(tableNames))
                    interceptor.InterceptSave(record);
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
                if (dbExec != null && value != null)
                    dbExec.Log = value;
            } // end set
        } // end property



        protected Func<IDbConnection> OpenConnection { get; set; }
        
        
        
        public void RegisterInterceptor<T>() where T : Interceptor {
            RegisterInterceptor(typeof(T));
        } // end method



        public void RegisterInterceptor(Type interceptorType) {
            Interceptors = null;
            if (typeof(Interceptor).IsAssignableFrom(interceptorType))
                interceptorTypeCache[ConnectionString].TryAdd(interceptorType);
        } // end method



        public void RegisterInterceptors(Assembly assembly) {
            if (assembly == null)
                throw new ArgumentNullException("assembly");

            foreach (var type in assembly.GetTypes())
                RegisterInterceptor(type);
        } // end method

        
        
        public Sequencer Sequencer {
            get {
                var dbExec = Executor as DatabaseExecutor;
                if (dbExec == null)
                    return null;

                if (dbExec.Sequencer == null) {
                    dbExec.Sequencer = new Sequencer(OpenConnection);
                    var tableDef = Executor.GetTableDefinition("Sequencer");
                    if (tableDef != null)
                        dbExec.Sequencer.BackingTableName = tableDef.FullName;
                } // end if
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