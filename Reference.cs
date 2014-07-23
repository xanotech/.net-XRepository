using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using Xanotech.Tools;

namespace Xanotech.Repository {
    class Reference {

        internal bool IsMany { get; set; } // end property



        internal bool IsOne {
            get { return !IsMany; }
            set { IsMany = !value; }
        } // end property



        internal PropertyInfo KeyProperty { get; set; }
        internal PropertyInfo ValueProperty { get; set; }
        internal Type ReferencedType { get; set; }

    } // end class
} // end namespace
