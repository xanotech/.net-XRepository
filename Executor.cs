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
        public abstract IEnumerable<IRecord> Fetch(IEnumerable<string> tableNames, CursorData cursorData);
        public abstract IEnumerable<string> GetColumns(string tableName);
        public abstract IEnumerable<string> GetPrimaryKeys(string tableName);
        public abstract TableDefinition GetTableDefinition(string tableName);
        public abstract void Remove(IEnumerable<IRecord> data);
        public abstract void Save(IEnumerable<IRecord> data);



        /// <summary>
        ///   This method takes a tablename and returns a standard error message.
        ///   It exists because the JavaScript version of XRepository uses the message
        ///   to determine the nature of server-side errors, whether they are simply
        ///   invalid tables or a more severe condition (like database taken over by
        ///   a hostile AI).
        /// </summary>
        /// <param name="tableName">The name of the invalid table.</param>
        /// <returns>an appropriate error message.</returns>
        internal static string FormatInvalidTableMessage(string tableName) {
            return "The table \"" + tableName + "\" is not a valid table.";
        } // end method

    } // end class
} // end namespace
