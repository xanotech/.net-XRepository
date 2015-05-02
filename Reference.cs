using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using XTools;

namespace XRepository {
    class Reference {

        internal bool IsMultiple { get; set; } // end property



        internal bool IsSingle {
            get { return !IsMultiple; }
            set { IsMultiple = !value; }
        } // end property



        internal PropertyInfo KeyProperty { get; set; }
        internal PropertyInfo ValueProperty { get; set; }
        internal Type ReferencedType { get; set; }

    } // end class
} // end namespace
