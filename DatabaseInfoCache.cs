using System;
using System.Collections.Generic;
using System.Data;
using System.Reflection;
using XTools;

namespace XRepository {
    class DatabaseInfoCache {
        internal bool? isBoolAllowed;
        internal Action<string> log;
        internal int maxParameters;
        internal DatabaseExecutor.PagingMechanism? pagingMechanism;
        internal Sequencer sequencer;

        internal Cache<string, IEnumerable<string>> columnsCache = new Cache<string, IEnumerable<string>>();
        internal Cache<string, IEnumerable<string>> primaryKeysCache = new Cache<string, IEnumerable<string>>();
        internal Cache<string, DataTable> schemaTableCache = new Cache<string, DataTable>();
        internal Cache<string, TableDefinition> tableDefinitionCache = new Cache<string, TableDefinition>();
    } // end class
} // end namespace
