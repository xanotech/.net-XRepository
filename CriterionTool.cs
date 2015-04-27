using System;
using System.Collections.Generic;

namespace XRepository {
    using IRecord = IDictionary<string, object>;
    using Record = Dictionary<string, object>;

    public static class CriterionTool {

        public static bool HasNullValue(this IEnumerable<Criterion> criteria) {
            foreach (var criterion in criteria)
                if (criterion.Value == null)
                    return true;
            return false;
        } // end method



        public static object GetValue(this IEnumerable<Criterion> criteria, string name) {
            foreach (var criterion in criteria)
                if (criterion.Name == name)
                    return criterion.Value;
            return null;
        } // end method



        public static IRecord ToDictionary(this IEnumerable<Criterion> criteria) {
            var map = new Record(StringComparer.OrdinalIgnoreCase);
            foreach (var criterion in criteria)
                map[criterion.Name] = criterion.Value;
            return map;
        } // end method

    } // end class
} // end namespace