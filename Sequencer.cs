using System.Collections.Generic;
using System.Data;
using Xanotech.Tools;

namespace Xanotech.Repository {

    /// <summary>
    ///   Used to sequence fields of database tables.
    /// </summary>
    public class Sequencer {

        private string connectionName;
        private IDictionary<string, long> sequences;



        /// <summary>
        ///   Constructs a Sequencer using the specified connection.
        /// </summary>
        public Sequencer(string connection) {
            connectionName = connection;
            sequences = new Dictionary<string, long>();
        } // End constructor



        /// <summary>
        ///   Selects the max value of the specified
        ///   field from the specified table.
        /// </summary>
        /// <param name="table">
        ///   The name of the table from which to query.
        /// </param>
        /// <param name="field">
        ///   The name of the field for which to select the max value.
        /// </param>
        /// <returns>
        ///   The maximum value of the field in the table
        ///   or 0 if the value is not an int.
        /// </returns>
        private long GetMaxValue(string table, string field) {
            long max;
            string sql = "SELECT MAX(" + field + ") FROM " + table;
            using (IDbConnection con = DataTool.OpenConnection(connectionName))
            using (IDbCommand cmd = con.CreateCommand()) {
                cmd.CommandText = sql;
                object result = cmd.ExecuteScalar();
                max = result as long? ?? result as int? ?? 0;
            } // end using
            return max;
        } // end method



        /// <summary>
        ///   Gets the next value in the specified sequence.
        /// </summary>
        /// <param name="table">
        ///   The name of the table from which to query.
        /// </param>
        /// <param name="field">
        ///   The name of the field for which to select the max value.
        /// </param>
        /// <returns>
        ///   The next value in the specified sequence
        ///   (1 more than a previous call).
        /// </returns>
        public long GetNextValue(string table, string field) {
            long sequence;
            lock (sequences) {
                var key = table + '.' + field;
                if (sequences.ContainsKey(key))
                    sequence = sequences[key];
                else
                    sequence = GetMaxValue(table, field);
                sequence++;
                sequences[key] = sequence;
            } // end lock
            return sequence;
        } // end method

    } // end class
} // end namespace