using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.Reflection;
using XTools;

namespace XRepository {
    using IRecord = IDictionary<string, object>;

    public abstract class Executor : IDisposable {

        /// <summary>
        ///   This property is the stack trace generated when the Executor is created.
        ///   It is use to uniquely identify a particular Executor in the code set
        ///   for debugging and error handling purposes.
        /// </summary>
        public string RepositoryCreationStack { get; set; }

        /// <summary>
        ///   The Func used for creating / opening a new connection.
        /// </summary>
        public Func<IDbConnection> OpenConnection { get; set; }

        public abstract long Count(IEnumerable<string> tableNames, IEnumerable<Criterion> criteria);
        public abstract void Dispose();
        public abstract void Fetch(IEnumerable<string> tableNames, CursorData cursorData,
            BlockingCollection<IRecord> results);
        public abstract IEnumerable<string> GetColumns(string tableName);
        public abstract IEnumerable<string> GetPrimaryKeys(string tableName);
        public abstract TableDefinition GetTableDefinition(string tableName);
        public abstract void Remove(BlockingCollection<IRecord> data);
        public abstract void Save(BlockingCollection<IRecord> data);
    } // end class
} // end namespace
