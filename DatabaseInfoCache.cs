using System;
using System.Collections.Generic;
using System.Data;
using System.Reflection;
using XTools;

namespace XRepository {
    class DatabaseInfoCache {
        internal DatabaseInfo.PagingMechanism? pagingMechanism;
        internal Sequencer sequencer;

        internal Cache<Type, PropertyInfo> idPropertyCache = new Cache<Type, PropertyInfo>();
        internal Cache<string, IEnumerable<string>> primaryKeysCache = new Cache<string, IEnumerable<string>>();
        internal Cache<Type, IEnumerable<Reference>> referencesCache = new Cache<Type, IEnumerable<Reference>>();
        internal Cache<string, DataTable> schemaTableCache = new Cache<string, DataTable>();
        internal Cache<string, Tuple<string, string, string>> tableDefinitionCache = new Cache<string, Tuple<string, string, string>>();
        internal Cache<Type, IEnumerable<string>> tableNamesCache = new Cache<Type, IEnumerable<string>>();
    } // end class
} // end namespace
