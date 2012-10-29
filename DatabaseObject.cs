using System.Collections.Generic;
using System.Text;

namespace wml.Tools {

    /// <summary>
    ///   The parent class for any object persisted in a table
    ///   (by means of DatabaseObjectBroker).
    /// </summary>
    public class DatabaseObject {

        /// <summary>
        ///   The Id of the record that represents this DatabaseObject.
        /// </summary>
        public long? Id { get; set; }

        internal DatabaseObjectBroker Broker { get; set; }



        public static int CompareByTypeAndId(DatabaseObject objA, DatabaseObject objB) {
            // if obj1 comes before obj2, return < 0
            // if obj1 comes after obj2, return > 0
            var result = 0;
            if (object.Equals(objA, null)) {
                if (object.Equals(objB, null))
                    result = 0;
                else
                    result = 1;
            } else {
                if (object.Equals(objB, null))
                    result = -1;
                else {
                    var type1 = objA.GetType();
                    var type2 = objB.GetType();

                    result = type1.FullName.CompareTo(type2.FullName);
                    if (result == 0) {
                        var idA = objA.Id ?? -1;
                        var idB = objB.Id ?? -1;
                        var idDiff = objA.Id - objB.Id;
                        if (idDiff > int.MaxValue)
                            idDiff = int.MaxValue;
                        else if (idDiff < int.MinValue)
                            idDiff = int.MinValue;
                        else if (idA == -1 && idDiff == 0)
                            idDiff = object.ReferenceEquals(objA, objB) ? 0 : -1;
                        result = (int)idDiff;
                    } // end if
                } // end if-else
            } // end if-else
            return result;
        } // end method



        public override int GetHashCode() {
            return base.GetHashCode();
        } // end method



        public override bool Equals(object obj) {
            var dbObj = obj as DatabaseObject;
            if (dbObj == null)
                return false;
            return CompareByTypeAndId(this, dbObj) == 0;
        } // end method



        public override string ToString() {
            StringBuilder sb = new StringBuilder();
            sb.Append(this.GetType().Name);
            if (Id.HasValue) sb.Append('(' + Id + ')');
            return sb.ToString();
        } // end method
        
        
        
        public virtual List<string> Validate() {
            return null;
        } // end method



        public static bool operator <(DatabaseObject obj1, DatabaseObject obj2) {
            return CompareByTypeAndId(obj1, obj2) < 0;
        } // end method



        public static bool operator <=(DatabaseObject obj1, DatabaseObject obj2) {
            return CompareByTypeAndId(obj1, obj2) <= 0;
        } // end method



        public static bool operator ==(DatabaseObject obj1, DatabaseObject obj2) {
            return CompareByTypeAndId(obj1, obj2) == 0;
        } // end method



        public static bool operator >=(DatabaseObject obj1, DatabaseObject obj2) {
            return CompareByTypeAndId(obj1, obj2) >= 0;
        } // end method



        public static bool operator >(DatabaseObject obj1, DatabaseObject obj2) {
            return CompareByTypeAndId(obj1, obj2) > 0;
        } // end method



        public static bool operator !=(DatabaseObject obj1, DatabaseObject obj2) {
            return CompareByTypeAndId(obj1, obj2) != 0;
        } // end method

    } // end class
} // end namespace