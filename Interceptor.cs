using System.Collections.Generic;
using System.Web;

namespace XRepository {
    using IRecord = IDictionary<string, object>;

    public abstract class Interceptor {

        protected internal HttpContextBase HttpContext { get; set; }
        public virtual bool IsMatch(IEnumerable<string> tableNames) { return true; }
        public virtual void InterceptCount(IEnumerable<string> tableNames, IEnumerable<Criterion> criteria) {}
        public virtual void InterceptFind(IEnumerable<string> tableNames, IEnumerable<Criterion> criteria) {}
        public virtual void InterceptFindComplete(IEnumerable<string> tableNames, IEnumerable<IRecord> records) {}
        public virtual void InterceptRemove(IRecord record) {}
        public virtual void InterceptSave(IRecord record) {}
        public Executor Executor { get; internal set; }

    } // end class

} // end namespace