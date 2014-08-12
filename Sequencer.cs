using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Threading;
using Xanotech.Tools;

namespace Xanotech.Repository {

    /// <summary>
    ///   Used to sequence columns of database tables.
    /// </summary>
    public class Sequencer {

        private string backingTableName;
        private bool? isBackingTablePresent;
        private Func<IDbConnection> openConnectionFunc;
        private IDictionary<string, long> sequences;



        public Sequencer(Func<IDbConnection> openConnectionFunc) {
            BackingTableName = "Sequencer";
            this.openConnectionFunc = openConnectionFunc;
            this.sequences = new Dictionary<string, long>();
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



        public static Sequencer Create(Func<IDbConnection> openConnectionFunc) {
            return new Sequencer(openConnectionFunc);
        } // end method



        public static Sequencer Create(string connectionStringName) {
            return new Sequencer(connectionStringName);
        } // end method



        private DataTable GetBackingTableSchema() {
            if (string.IsNullOrWhiteSpace(BackingTableName))
                return null;

            using (var con = openConnectionFunc())
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
                using (var con = openConnectionFunc())
                using (var cmd = con.CreateCommand())
                    return GetMaxValue(cmd, table, column);

            command.CommandText = "SELECT MAX(" + column + ") FROM " + table;
            object result = command.ExecuteScalar();
            return result as long? ?? result as int? ?? 0;
        } // end method



        /// <summary>
        ///   Gets the next value in the specified sequence.
        /// </summary>
        /// <param name="table">
        ///   The name of the table from which to query.
        /// </param>
        /// <param name="column">
        ///   The name of the column for which to select the max value.
        /// </param>
        /// <returns>
        ///   The next value in the specified sequence
        ///   (1 more than a previous call).
        /// </returns>
        public long GetNextValue(string table, string column) {
            long sequence;

            if (IsBackingTablePresent) {
                using (var con = openConnectionFunc())
                using (var transaction = con.BeginTransaction())
                using (var cmd = con.CreateCommand()) {
                    cmd.AddParameter("TableName", table);
                    cmd.AddParameter("ColumnName", column);

                    cmd.CommandText = "UPDATE " + BackingTableName + Environment.NewLine +
                        "SET SequenceValue = SequenceValue + 1" + Environment.NewLine +
                        "WHERE TableName = @TableName AND ColumnName = @ColumnName";
                    cmd.Transaction = transaction;
                    cmd.ExecuteNonQuery();

                    cmd.CommandText = "SELECT SequenceValue " + Environment.NewLine +
                        "FROM " + BackingTableName + Environment.NewLine +
                        "WHERE TableName = @TableName AND ColumnName = @ColumnName";
                    var result = cmd.ExecuteScalar();

                    if (result == null || result == DBNull.Value) {
                        sequence = GetMaxValue(cmd, table, column) + 1;
                        cmd.CommandText = "INSERT INTO " + BackingTableName + Environment.NewLine +
                            "(TableName, ColumnName, SequenceValue)" + Environment.NewLine +
                            "VALUES (@TableName, @ColumnName, @SequenceValue)";
                        cmd.AddParameter("SequenceValue", sequence);
                        cmd.ExecuteNonQuery();
                    } else
                        sequence = result as long? ?? result as int? ?? -1;

                    transaction.Commit();
                } // end using
            } else lock (sequences) {
                var key = table + '.' + column;
                if (sequences.ContainsKey(key))
                    sequence = sequences[key];
                else
                    sequence = GetMaxValue(null, table, column);
                sequence++;
                sequences[key] = sequence;
            } // end else-lock

            return sequence;
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

            using (var con = openConnectionFunc())
            using (var cmd = con.CreateCommand()) {
                cmd.AddParameter("TableName", "<Sequencer.ValidateLocking>");
                cmd.AddParameter("ColumnName", "<Sequencer.ValidateLocking>");
                cmd.AddParameter("SequenceValue", 0);

                cmd.CommandText = "DELETE FROM " + BackingTableName + Environment.NewLine +
                    "WHERE TableName = @TableName AND ColumnName = @ColumnName";
                cmd.ExecuteNonQuery();

                cmd.CommandText = "INSERT INTO " + BackingTableName + Environment.NewLine +
                    "(TableName, ColumnName, SequenceValue)" + Environment.NewLine +
                    "VALUES (@TableName, @ColumnName, @SequenceValue)";
                cmd.ExecuteNonQuery();

                var thread = new Thread(() => {
                    WaitForValidateLockingRecord();
                    GetNextValue("<Sequencer.ValidateLocking>", "<Sequencer.ValidateLocking>");
                });
                using (var transaction = con.BeginTransaction()) {
                    cmd.CommandText = "UPDATE " + BackingTableName + Environment.NewLine +
                        "SET SequenceValue = SequenceValue + 1" + Environment.NewLine +
                        "WHERE TableName = @TableName AND ColumnName = @ColumnName";
                    cmd.Transaction = transaction;
                    cmd.ExecuteNonQuery();

                    thread.Start();
                    Thread.Sleep(300);

                    cmd.CommandText = "SELECT SequenceValue " + Environment.NewLine +
                        "FROM " + BackingTableName + Environment.NewLine +
                        "WHERE TableName = @TableName AND ColumnName = @ColumnName";
                    sequenceObject = cmd.ExecuteScalar();
                    transaction.Commit();
                } // end using
                thread.Join();

                cmd.CommandText = "DELETE FROM " + BackingTableName + Environment.NewLine +
                    "WHERE TableName = @TableName AND ColumnName = @ColumnName";
                cmd.ExecuteNonQuery();
            } // end using

            var sequence = sequenceObject as long? ?? sequenceObject as int? ?? -1;
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
            using (var con = openConnectionFunc())
            using (var cmd = con.CreateCommand()) {
                cmd.AddParameter("TableName", "<Sequencer.ValidateLocking>");
                cmd.AddParameter("ColumnName", "<Sequencer.ValidateLocking>");
                cmd.CommandText = "SELECT COUNT(*) FROM " + BackingTableName + Environment.NewLine +
                    "WHERE TableName = @TableName AND ColumnName = @ColumnName";
                var count = cmd.ExecuteScalar();
                exists = (count as long? ?? count as int? ?? 0) == 1;
                Thread.Sleep(50);
            } // end using
        } // end method

    } // end class
} // end namespace