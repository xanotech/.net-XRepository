using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Xanotech.Repository {
    class LazyLoadEnumerable<T> : IEnumerable<T> where T : new() {

        private List<T> data;
        private static IDictionary<Type, Type> genericTypeCache = new Dictionary<Type, Type>();

        public Criterion Criterion { get; set; }
        public IRepository Repository { get; set; }



        public IEnumerator<T> GetEnumerator() {
            if (data != null)
                return data.GetEnumerator();

            if (Criterion == null || Repository == null)
                data = new List<T>();
            else
                data = new List<T>(Repository.Find<T>(Criterion));
            return data.GetEnumerator();
        } // end method



        IEnumerator IEnumerable.GetEnumerator() {
            return GetEnumerator();
        } // end method

    } // end class
} // end namespace