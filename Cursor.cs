using System;
using System.Collections;
using System.Collections.Generic;
using Xanotech.Tools;

namespace Xanotech.Repository {
    public class Cursor<T> : IEnumerable<T> where T : new() {

        // Used by the spawning repository for populating data.
        internal IEnumerable<Criterion> criteria;
        internal DatabaseInfo.PagingMechanism? pagingMechanism;

        // data and fetch are closely related.  data is essentially initialized
        // the the results of fetch whenever results need to be pulled.
        private IList<T> data;
        private Func<Cursor<T>, IEnumerable[], IEnumerable<T>> fetch;

        private IRepository repository; // Only used for Count.

        private IEnumerable[] joinObjects; // Passed to fetch, received from Join method.

        // The following values hold the underlying values for
        // the limit, skip, and sort methods / properties.
        private long? limitValue;
        private long? skipValue;
        private IDictionary<string, int> sortValue;



        internal Cursor(IEnumerable<Criterion> criteria,
            Func<Cursor<T>, IEnumerable[], IEnumerable<T>> fetchFunc,
            IRepository repository) {
            if (fetchFunc == null)
                throw new ArgumentNullException("fetchFunc", "The fetchFunc parameter is null.");

            this.criteria = criteria;
            this.fetch = fetchFunc;
            this.repository = repository;
        } // end constructor



        public long Count(bool applySkipLimit = false) {
            if (applySkipLimit) {
                data = data ?? new List<T>(fetch(this, joinObjects));
                return data.Count;
            } else
                return repository.Count<T>(criteria);
        } // end method



        public IEnumerator<T> GetEnumerator() {
            data = data ?? new List<T>(fetch(this, joinObjects));
            return data.GetEnumerator();
        } // end method



        IEnumerator IEnumerable.GetEnumerator() {
            return GetEnumerator();
        } // end method



        public Cursor<T> Join(params IEnumerable[] objects) {
            data = null;
            joinObjects = objects;
            return this;
        } // end method

        

        public long? limit {
            get { return limitValue; }
            set { Limit(value); }
        } // end property



        public Cursor<T> Limit(long? rows) {
            data = null;
            limitValue = rows;
            return this;
        } // end method



        public long Size() {
            return Count(true);
        } // end method



        public long? skip {
            get { return skipValue; }
            set { Skip(value); }
        } // end property



        public Cursor<T> Skip(long? rows) {
            data = null;
            skipValue = rows;
            return this;
        } // end method



        public IDictionary<string, int> sort {
            get { return sortValue; }
            set { Sort(value); }
        } // end property



        public Cursor<T> Sort(IDictionary<string, int> sortBy) {
            data = null;
            sortValue = sortBy;
            return this;
        } // end method



        public Cursor<T> Sort(IEnumerable<string> sortBy) {
            var sortByDictionary = new Dictionary<string, int>();
            foreach (var column in sortBy)
                sortByDictionary[column] = 1;
            return Sort(sortByDictionary);
        } // end method



        public Cursor<T> Sort(params string[] sortBy) {
            return Sort((IEnumerable<string>)sortBy);
        } // end method

    } // end class
} // end namespace