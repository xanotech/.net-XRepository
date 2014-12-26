using System.Collections.Generic;

namespace XRepository {
    public class CursorData {

        // Used by the spawning repository for populating data.
        public IEnumerable<Criterion> criteria;
        public DatabaseExecutor.PagingMechanism? pagingMechanism;

        // The following values hold the underlying values for
        // the limit, skip, and sort methods / properties.
        public long? limit;
        public long? skip;
        public IDictionary<string, int> sort;



        public CursorData Clone() {
            var newCursorData = new CursorData();

            if (criteria != null) {
                var newCriteria = new List<Criterion>();
                foreach (var criterion in criteria)
                    newCriteria.Add(criterion.Clone());
                newCursorData.criteria = newCriteria;
            } // end if

            newCursorData.limit = limit;
            newCursorData.skip = skip;

            if (sort != null) {
                var newSort = new Dictionary<string, int>();
                foreach (var key in sort.Keys)
                    newSort[key] = sort[key];
                newCursorData.sort = newSort;
            } // end if

            return newCursorData;
        } // end method

    } // end class
} // end namespace