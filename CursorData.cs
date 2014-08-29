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

    } // end class
} // end namespace