using System.Collections;
using System.Collections.Generic;
using System.Linq;
using XTools;

namespace XRepository {
    public class CursorData {

        // Used for holding the columns to be fetched.
        // If it is null or empty, "SELECT *" will be used.
        public IEnumerable<string> columns;

        // Used by the spawning repository for populating data.
        public IEnumerable<Criterion> criteria;

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



        internal Criterion GetLargestCollectionCriterion(ref int maxParameters) {
            if (criteria == null)
                return null;

            if (maxParameters == default(int))
                maxParameters = int.MaxValue;

            var total = 0;
            Criterion largestCollectionCriterion = null;
            var largestCollectionCount = 0;
            foreach (var criterion in criteria) {
                var vals = criterion.GetValues();

                // If the Value is not a collection, add 1 to total
                // (if its not null) and skip to the next criterion.
                if (vals == null) {
                    if (criterion.Value != null)
                        total++;
                    continue;
                } // end if

                // Find the count of the collection and add it to the total.
                var count = vals.Count();
                total += count;

                // If the count of this collection is larger than what's been found in
                // previous iterations, then set largestCollection references to current values.
                if (count > largestCollectionCount &&
                    criterion.Operation != Criterion.OperationType.NotEqualTo) {
                    largestCollectionCriterion = criterion;
                    largestCollectionCount = count;
                } // end if
            } // end foreach

            // Since we can break up the largest collection, subtract it from the total.
            total -= largestCollectionCount;

            // If the total is still higher than the maxParameters, there's no way to split
            // the calls up.  Basically, just set largestCollectionCriterion to null
            // and let whatever database error occurs bubble up.  Otherwise, subtract total
            // from maxParameters.  SplitLargeCollections will then break up the largest
            // collection by that number.
            if (total >= maxParameters)
                largestCollectionCriterion = null;
            else
                maxParameters -= total;

            return largestCollectionCriterion;
        } // end method



        internal IEnumerable<CursorData> SplitLargeCollections(int maxParameters) {
            var cursorDatas = new List<CursorData>();

            if (criteria != null)
                foreach (var criterion in criteria)
                    criterion.Distinctify();

            var largest = GetLargestCollectionCriterion(ref maxParameters);
            if (largest == null)
                cursorDatas.Add(this);
            else {
                // Get the vals from the largest collection and then set the Value to null.
                // Value is set to null to allow it to be null when the criterion is cloned
                // and is reset its intial value after the split is done.
                var vals = largest.GetValues();
                largest.Value = null;

                CursorData clone = null;
                Criterion replacementCriterion = null;
                List<object> replacementVals = null;
                foreach (var val in vals) {
                    // If clone is null, which would be either the first iteration or one
                    // following a break, clone the current CursorData, get a reference to
                    // the criterion matching the largest criterion (replacementCriterion)
                    // and set its value to a new list.
                    if (clone == null) {
                        clone = Clone();
                        replacementCriterion = clone.criteria.First(c =>
                            c.Name == largest.Name &&
                            c.Operation == largest.Operation &&
                            c.Value == null);
                        replacementVals = new List<object>();
                        replacementCriterion.Value = replacementVals;
                    } // end if

                    replacementVals.Add(val);

                    // Iteration break: when replacementVals is at the max, add the clone
                    // to cursorDatas and set it to null.  If there are move values to add,
                    // the next iteration will create a new clone.
                    if (replacementVals.Count == maxParameters) {
                        cursorDatas.Add(clone);
                        clone = null;
                    } // end if
                } // end foreach

                // If there is clone (and there probably is) add it to cursorDatas.
                if (clone != null)
                    cursorDatas.Add(clone);

                largest.Value = vals; // Reset Value to its original, er... value.
            } // end if-else

            return cursorDatas;
        } // end method

    } // end class
} // end namespace