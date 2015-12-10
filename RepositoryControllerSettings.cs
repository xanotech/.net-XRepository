using System;
using System.Collections.Generic;
using System.Data;
using System.Reflection;
using XTools;

namespace XRepository {
    public class RepositoryControllerSettings {

        public Type ExecutorType { get; set; }
        public int? MaxParameters { get; set; }
        public Action<string> Log { get; set; }
        public Sequencer Sequencer { get; set; }

    } // end class
} // end namespace
