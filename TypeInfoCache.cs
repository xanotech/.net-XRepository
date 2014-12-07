using System;
using System.Collections.Generic;
using System.Data;
using System.Reflection;
using XTools;

namespace XRepository {
    class TypeInfoCache {
        internal Cache<Type, IDictionary<string, string>> columnMapCache =
            new Cache<Type, IDictionary<string, string>>(t => new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase));
        internal Cache<Type, PropertyInfo> idPropertyCache = new Cache<Type, PropertyInfo>();
        internal Cache<Type, IEnumerable<Reference>> referencesCache = new Cache<Type, IEnumerable<Reference>>();
        internal Cache<Type, IEnumerable<string>> tableNamesCache = new Cache<Type, IEnumerable<string>>();
    } // end class
} // end namespace
