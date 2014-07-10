using System;
using System.Collections.Generic;
using System.Data;
using System.Reflection;
using Xanotech.Tools;

namespace Xanotech.Repository {
    public class ConnectionInfo {
        private IList<string> sqlLog = new List<string>();
        
        public long Id { get; set; }
        public DateTime CreationDatetime { get; set; }
        public string RepositoryCreationStack { get; set; }



        public IList<string> SqlLog {
            get {
                return sqlLog;
            } // end get
        } // end property

    } // end class
} // end namespace
