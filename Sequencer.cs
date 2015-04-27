using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Data.OleDb;
using System.Threading;
using XTools;

namespace XRepository {

    /// <summary>
    ///   Used to sequence columns of database tables.
    /// </summary>
    public class Sequencer {

        private string backingTableName;
        private bool? isBackingTablePresent;
        private Func<IDbConnection> openConnection;
        private IDictionary<string, long> sequences;



        public Sequencer(Func<IDbConnection> openConnection) {
            this.openConnection = openConnection;
            this.sequences = new Dictionary<string, long>(StringComparer.OrdinalIgnoreCase);
        } // end constructor



        /// <summary>
        ///   Constructs a Sequencer using the specified connection.
        ///   <param name="connectionStringName">
        ///     The name of the connection string as definied in the App.config or Web.config.
        ///   </param>
        /// </summary>
        public Sequencer(string connectionStringName) :
            this(() => { return DataTool.OpenConnection(connectionStringName); }) {
        } // end constructor



        public string BackingTableName {
            get { return backingTableName; }
            set {
                backingTableName = value;
                isBackingTablePresent = null;
            } // end set
        } // end property



        public static Sequencer Create<T>(string connectionString) where T : IDbConnection, new() {
            return new Sequencer(() => { return DataTool.OpenConnection<T>(connectionString); });
        } // end method



        public static Sequencer Create(Func<IDbConnection> openConnection) {
            return new Sequencer(openConnection);
        } // end method



        public static Sequencer Create(string connectionStringName) {
            return new Sequencer(connectionStringName);
        } // end method



        private DataTable GetBackingTableSchema() {
            if (string.IsNullOrWhiteSpace(BackingTableName))
                return null;

            using (var con = openConnection())
            using (var cmd = con.CreateCommand()) {
                cmd.CommandText = "SELECT * FROM " + BackingTableName + " WHERE 1 = 0";
                try {
                    using (var dr = cmd.ExecuteReader())
                        return dr.GetSchemaTable();
                } catch (DbException) {
                    // This exception most likely occurs when the table does not exist.
                    // Simply return null indicating that the table does not exist.
                    return null;
                } // end try-catch
            } // end using
        } // end method



        /// <summary>
        ///   Selects the max value of the specified table and column.
        /// </summary>
        /// <param name="table">
        ///   The name of the table from which to query.
        /// </param>
        /// <param name="column">
        ///   The name of the column for which to select the max value.
        /// </param>
        /// <returns>
        ///   The maximum value of the column in the table
        ///   or 0 if the value is null or a non-integer.
        /// </returns>
        private long GetMaxValue(IDbCommand command, string table, string column) {
            if (command == null)
                using (var con = openConnection())
                using (var cmd = con.CreateCommand())
                    return GetMaxValue(cmd, table, column);

            command.CommandText = "SELECT MAX(" + column + ") FROM " + table;
            object result = command.ExecuteScalar();
            return DataTool.AsLong(result) ?? 0;
        } // end method



        /// <summary>
        ///   Gets the next value in the specified sequence and increases the seed by
        ///   the specified amount.  For example, if the seed is currently 10 and
        ///   increaseBy is 5, the value returned is 11 and the value returned from
        ///   the next call will be 16.
        /// </summary>
        /// <param name="table">
        ///   The name of the table from which to query.
        /// </param>
        /// <param name="column">
        ///   The name of the column for which to select the max value.
        /// </param>
        /// <param name="increaseBy">
        ///   The amount to increase the sequence by (affecting the next call).
        /// </param>
        /// <returns>
        ///   The next value in the specified sequence.
        /// </returns>
        public long GetNextValues(string table, string column, int increaseBy) {
            if (increaseBy < 1)
                throw new ArgumentException("The increaesBy parameter must be greater than 0.", "increaseBy");

            long sequence;
            if (IsBackingTablePresent) {
                using (var con = openConnection())
                using (var transaction = con.BeginTransaction())
                using (var cmd = con.CreateCommand()) {
                    cmd.AddParameter("TableName", table);
                    cmd.AddParameter("ColumnName", column);

                    cmd.CommandText = "UPDATE " + BackingTableName + Environment.NewLine +
                        "SET SequenceValue = SequenceValue + " + increaseBy + Environment.NewLine +
                        "WHERE TableName = " + cmd.FormatParameter("TableName") + Environment.NewLine +
                        "AND ColumnName = " + cmd.FormatParameter("ColumnName");
                    cmd.Transaction = transaction;
                    bool loop = true;
                    while (loop)
                        try {
                            cmd.ExecuteNonQuery();
                            loop = false;
                        } catch (OleDbException e) {
                            // Instead of waiting (like every other database in the world)
                            // MS Access throws this exception.  Simulate waiting by
                            // repeating the query until it works.
                            if (e.Message == "Could not update; currently locked.")
                                Thread.Sleep(100);
                            else
                                throw;
                        } // end try-catch

                    cmd.CommandText = "SELECT SequenceValue " + Environment.NewLine +
                        "FROM " + BackingTableName + Environment.NewLine +
                        "WHERE TableName = " + cmd.FormatParameter("TableName") + Environment.NewLine +
                        "AND ColumnName = " + cmd.FormatParameter("ColumnName");
                    var result = cmd.ExecuteScalar();

                    if (result == null || result == DBNull.Value) {
                        sequence = GetMaxValue(cmd, table, column) + increaseBy;
                        cmd.CommandText = "INSERT INTO " + BackingTableName + Environment.NewLine +
                            "(TableName, ColumnName, SequenceValue)" + Environment.NewLine +
                            "VALUES (" + cmd.FormatParameter("TableName") + ", " +
                            cmd.FormatParameter("ColumnName") + ", " +
                            cmd.FormatParameter("SequenceValue") + ")";
                        cmd.AddParameter("SequenceValue", sequence);
                        cmd.ExecuteNonQuery();
                    } else
                        sequence = DataTool.AsLong(result) ?? -1;

                    transaction.Commit();
                } // end using
            } else lock (sequences) {
                var key = table + '.' + column;
                if (sequences.ContainsKey(key))
                    sequence = sequences[key];
                else
                    sequence = GetMaxValue(null, table, column);
                sequence += increaseBy;
                sequences[key] = sequence;
            } // end else-lock

            return sequence - increaseBy + 1;
        } // end method



        public bool IsBackingTablePresent {
            get {
                if (isBackingTablePresent != null)
                    return isBackingTablePresent.Value;

                var schema = GetBackingTableSchema();
                isBackingTablePresent = schema != null;

                if (isBackingTablePresent.Value) {
                    ValidateBackingTableSchema(schema);
                    ValidateLocking();
                } // end if

                return isBackingTablePresent.Value;
            } // end get
        } // end method



        private void SetupValidateLockingRecord() {
            using (var con = openConnection())
            using (var cmd = con.CreateCommand()) {
                cmd.CommandText = "DELETE FROM " + BackingTableName + Environment.NewLine +
                    "WHERE TableName = " + cmd.FormatParameter("TableName") + Environment.NewLine +
                    "AND ColumnName = " + cmd.FormatParameter("ColumnName");
                cmd.AddParameter("TableName", "<Sequencer.ValidateLocking>");
                cmd.AddParameter("ColumnName", "<Sequencer.ValidateLocking>");
                cmd.ExecuteNonQuery();

                cmd.CommandText = "INSERT INTO " + BackingTableName + Environment.NewLine +
                    "(TableName, ColumnName, SequenceValue)" + Environment.NewLine +
                    "VALUES (" + cmd.FormatParameter("TableName") + ", " +
                    cmd.FormatParameter("ColumnName") + ", " +
                    cmd.FormatParameter("SequenceValue") + ")";
                cmd.AddParameter("SequenceValue", 0);
                cmd.ExecuteNonQuery();
            } // end using
        } // end method



        private void ValidateBackingTableSchema(DataTable schema) {
            var isTableNameExists = false;
            var isColumnNameExists = false;
            var isSequenceValueExists = false;
            for (int i = 0; i < schema.Rows.Count; i++) {
                var column = (string)schema.Rows[i]["ColumnName"];
                if (column.Is("TableName"))
                    isTableNameExists = true;
                if (column.Is("ColumnName"))
                    isColumnNameExists = true;
                if (column.Is("SequenceValue"))
                    isSequenceValueExists = true;
            } // end for

            if (!isTableNameExists || !isColumnNameExists || !isSequenceValueExists)
                throw new DataException("The " + BackingTableName +
                    " does not contain the required columns " +
                    "(TableName, ColumnName, and SequenceValue).");
        } // end method



        private void ValidateLocking() {
            object sequenceObject;

            SetupValidateLockingRecord();
            WaitForValidateLockingRecord();

            using (var con = openConnection())
            using (var cmd = con.CreateCommand()) {
                cmd.AddParameter("TableName", "<Sequencer.ValidateLocking>");
                cmd.AddParameter("ColumnName", "<Sequencer.ValidateLocking>");

                var thread = new Thread(() => {
                    GetNextValues("<Sequencer.ValidateLocking>", "<Sequencer.ValidateLocking>", 1);
                });
                using (var transaction = con.BeginTransaction()) {
                    cmd.CommandText = "UPDATE " + BackingTableName + Environment.NewLine +
                        "SET SequenceValue = SequenceValue + 1" + Environment.NewLine +
                        "WHERE TableName = " + cmd.FormatParameter("TableName") + Environment.NewLine +
                        "AND ColumnName = " + cmd.FormatParameter("ColumnName");
                    cmd.Transaction = transaction;
                    cmd.ExecuteNonQuery();

                    thread.Start();
                    Thread.Sleep(300);

                    cmd.CommandText = "SELECT SequenceValue " + Environment.NewLine +
                        "FROM " + BackingTableName + Environment.NewLine +
                        "WHERE TableName = " + cmd.FormatParameter("TableName") + Environment.NewLine +
                        "AND ColumnName = " + cmd.FormatParameter("ColumnName");
                    sequenceObject = cmd.ExecuteScalar();
                    transaction.Commit();
                } // end using
                thread.Join();

                cmd.CommandText = "DELETE FROM " + BackingTableName + Environment.NewLine +
                    "WHERE TableName = " + cmd.FormatParameter("TableName") + Environment.NewLine +
                    "AND ColumnName = " + cmd.FormatParameter("ColumnName");
                cmd.ExecuteNonQuery();
            } // end using

            var sequence = DataTool.AsLong(sequenceObject) ?? -1;
            if (sequence == -1)
                throw new DataException("The SequenceValue column of " + BackingTableName +
                    " does not appear to be an integer type.");
            if (sequence > 1)
                throw new DataException("The database or the table " + BackingTableName +
                    " does not support locking.");
        } // end method



        private void WaitForValidateLockingRecord() {
            bool exists = false;
            while (!exists)
            using (var con = openConnection())
            using (var cmd = con.CreateCommand()) {
                cmd.CommandText = "SELECT COUNT(*) FROM " + BackingTableName + Environment.NewLine +
                    "WHERE TableName = " + cmd.FormatParameter("TableName") + Environment.NewLine +
                    "AND ColumnName = " + cmd.FormatParameter("ColumnName");
                cmd.AddParameter("TableName", "<Sequencer.ValidateLocking>");
                cmd.AddParameter("ColumnName", "<Sequencer.ValidateLocking>");
                var count = cmd.ExecuteScalar();
                exists = (DataTool.AsLong(count) ?? 0) == 1;
                Thread.Sleep(50);
            } // end using
        } // end method

    } // end class
} // end namespace