using System;
using System.Collections;
using System.Collections.Generic;
using XTools;

namespace XRepository {
    public class Cursor<T> : IEnumerable<T> where T : new() {

        // data and fetch are closely related.  data is essentially initialized
        // the the results of fetch whenever results need to be pulled.
        private IList<T> data;
        private Func<Cursor<T>, IEnumerable[], IEnumerable<T>> fetch;

        private IRepository repository; // Only used for Count.

        private IEnumerable[] joinObjects; // Passed to fetch, received from Join method.



        internal Cursor(IEnumerable<Criterion> criteria,
            Func<Cursor<T>, IEnumerable[], IEnumerable<T>> fetchFunc,
            IRepository repository) {
            if (fetchFunc == null)
                throw new ArgumentNullException("fetchFunc", "The fetchFunc parameter is null.");

            CursorData.criteria = criteria;
            this.fetch = fetchFunc;
            this.repository = repository;
        } // end constructor



        public long Count(bool applySkipLimit = false) {
            if (applySkipLimit) {
                data = data ?? new List<T>(fetch(this, joinObjects));
                return data.Count;
            } else
                return repository.Count<T>(cursorData.criteria);
        } // end method



        private CursorData cursorData;
        internal CursorData CursorData {
            get {
                return cursorData = cursorData ?? new CursorData();
            } // end get
        } // end property



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
            get { return CursorData.limit; }
            set { Limit(value); }
        } // end property



        public Cursor<T> Limit(long? rows) {
            data = null;
            CursorData.limit = rows;
            return this;
        } // end method



        public long Size() {
            return Count(true);
        } // end method



        public long? skip {
            get { return CursorData.skip; }
            set { Skip(value); }
        } // end property



        public Cursor<T> Skip(long? rows) {
            data = null;
            CursorData.skip = rows;
            return this;
        } // end method



        public IDictionary<string, int> sort {
            get { return CursorData.sort; }
            set { Sort(value); }
        } // end property



        public Cursor<T> Sort(IDictionary<string, int> sortBy) {
            data = null;
            CursorData.sort = sortBy;
            return this;
        } // end method



        public Cursor<T> Sort(IEnumerable<string> sortBy) {
            var sortByDictionary = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
            foreach (var column in sortBy)
                sortByDictionary[column] = 1;
            return Sort(sortByDictionary);
        } // end method



        public Cursor<T> Sort(params string[] sortBy) {
            return Sort((IEnumerable<string>)sortBy);
        } // end method

    } // end class
} // end namespace