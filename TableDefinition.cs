namespace XRepository {
    public class TableDefinition {

        public TableDefinition(string schemaName, string tableName) {
            SchemaName = schemaName;
            TableName = tableName;
        } // end constructor



        private string fullName;
        public string FullName {
            get {
                if (fullName != null)
                    return fullName;

                fullName = TableName;
                if (!string.IsNullOrEmpty(SchemaName))
                    fullName = SchemaName + '.' + fullName;
                return fullName;
            } // end get
        } // end property



        private string schemaName;
        public string SchemaName {
            get { return schemaName; }
            set {
                fullName = null;
                schemaName = value;
            } // end set
        } // end property



        private string tableName;
        public string TableName {
            get { return tableName; }
            set {
                fullName = null;
                tableName = value;
            } // end set
        } // end property

    } // end class
} // end namespace