using System;
using System.Collections;
using System.Collections.Generic;
using Xanotech.Tools;

namespace Xanotech.Repository {
    public class Cursor<T> : IEnumerable<T> where T : new() {

        internal IEnumerable<Criterion> criteria;
        private IList<T> data;
        private Func<IEnumerable<Criterion>, Cursor<T>, IEnumerable<T>> fetch;
        private long? limit;
        private IRepository repository;
        private long? skip;
        private IDictionary<string, int> sort;



        internal Cursor(IEnumerable<Criterion> criteria,
            Func<IEnumerable<Criterion>, Cursor<T>, IEnumerable<T>> fetchFunc,
            IRepository repository) {
            if (fetchFunc == null)
                throw new ArgumentNullException("fetchFunc", "The fetchFunc parameter is null.");

            this.criteria = criteria;
            this.fetch = fetchFunc;
            this.repository = repository;
        } // end constructor



        public long Count(bool applySkipLimit = false) {
            if (applySkipLimit) {
                data = data ?? new List<T>(fetch(criteria, this));
                return data.Count;
            } else
                return repository.Count<T>(criteria);
        } // end method



        public IEnumerator<T> GetEnumerator() {
            data = data ?? new List<T>(fetch(criteria, this));
            return data.GetEnumerator();
        } // end method



        IEnumerator IEnumerable.GetEnumerator() {
            return GetEnumerator();
        } // end method

        

        public long? Limit {
            get { return limit; }
            set {
                data = null;
                limit = value;
            }
        } // end property



        public long Size() {
            return Count(true);
        } // end method



        public long? Skip {
            get { return skip; }
            set {
                data = null;
                skip = value;
            }
        } // end property



        public IDictionary<string, int> Sort {
            get { return sort; }
            set {
                data = null;
                sort = value;
            }
        } // end property

    } // end class
} // end namespace