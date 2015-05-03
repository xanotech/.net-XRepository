using System;
using System.Collections;
using System.Collections.Generic;
using XTools;

namespace XRepository {
    public class Cursor<T> : IEnumerable<T> where T : new() {

        private static Cache<Type, Mirror> mirrorCache = new Cache<Type, Mirror>(t => new Mirror(t));

        // data and fetch are closely related.  data is essentially initialized
        // the the results of fetch whenever results need to be pulled.
        private IList<T> data;
        private Func<Cursor<T>, IEnumerable[], IEnumerable<T>> fetch;

        private NRepository repository; // Only used for Count.

        private IEnumerable[] joinObjects; // Passed to fetch, received from Join method.



        internal Cursor(IEnumerable<Criterion> criteria,
            Func<Cursor<T>, IEnumerable[], IEnumerable<T>> fetchFunc,
            NRepository repository) {
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



        public Cursor<T> Sort(IDictionary<string, bool> sortBy) {
            var sortByDictionary = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
            foreach (var column in sortBy.Keys)
                sortByDictionary[column] = sortBy[column] ? 1 : -1;
            return Sort(sortByDictionary);
        } // end method



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



        public Cursor<T> Sort(object sortBy) {
            if (sortBy == null)
                return Sort(new string[0]);

            if (TryKnownSorts(sortBy))
                return this;

            var sortByMap = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
            var type = sortBy.GetType();
            var mirror = mirrorCache[type];
            foreach (var prop in mirror.GetProperties()) {
                var value = prop.GetValue(sortBy, null);
                if (value == null)
                    continue;

                var boolVal = value as bool?;
                if (boolVal != null)
                    value = boolVal.Value ? 1 : -1;
                
                var intVal = value as int?;
                if (intVal != null)
                    sortByMap[prop.Name] = intVal.Value;
            } // end foreach
            return Sort(sortByMap);
        } // end method



        public Cursor<T> Sort(params string[] sortBy) {
            return Sort((IEnumerable<string>)sortBy);
        } // end method



        private bool TryKnownSorts(object sortBy)  {
            var sortByStr = sortBy as string;
            if (sortByStr != null)
                Sort(new[] {sortByStr});
            
            var sortByStrs = sortBy as string[];
            if (sortByStrs != null)
                Sort(sortByStrs);
            
            var sortByBoolMap = sortBy as IDictionary<string, bool>;
            if (sortByBoolMap != null)
                Sort(sortByBoolMap);
            
            var sortByIntMap = sortBy as IDictionary<string, int>;
            if (sortByIntMap != null)
                Sort(sortByIntMap);

            return sortByStr != null || sortByStrs != null ||
                sortByBoolMap != null || sortByIntMap != null;
        } // end method

    } // end class
} // end namespace