using System;
using System.Collections.Concurrent;
using System.Collections.Generic;

namespace XRepository {
    public interface IExecutor : IDisposable {
        void Dispose();

        IEnumerable<string> GetColumns(string tableName);
        IEnumerable<string> GetPrimaryKeys(string tableName);
        TableDefinition GetTableDefinition(string tableName);

        long Count(IEnumerable<string> tableNames, IEnumerable<Criterion> criteria);
        void Fetch(IEnumerable<string> tableNames, CursorData cursorData,
            BlockingCollection<IDictionary<string, object>> results);
        void Remove(BlockingCollection<IDictionary<string, object>> data);
        void Save(BlockingCollection<IDictionary<string, object>> data);
    } // end interface
} // end namespace
