using System.Reflection;
using System;
using System.Collections.Generic;

namespace Xanotech.Repository {
    class Reference {

        internal bool IsMany {
            get { return ReferencingType != null; }
        } // end property



        internal bool IsOne {
            get { return ReferencingProperty != null; }
        } // end property



        internal PropertyInfo Property { get; set; }
        internal Type ReferencedType { get; set; }
        internal PropertyInfo ReferencingProperty { get; set; }
        internal Type ReferencingType { get; set; }

    } // end class
} // end namespace
