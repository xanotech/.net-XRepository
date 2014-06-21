using System;
using System.Collections.Generic;
using Xanotech.Tools;

namespace Xanotech.Repository {
    public static class CursorTool {

        private static Mirror intMirror = new Mirror(typeof(int));
        private static Cache<Type, Mirror> mirrorCache = new Cache<Type, Mirror>();

        
        
        public static Cursor<T> Limit<T>(this Cursor<T> cursor, long? maxObjects)
            where T : new() {
            cursor.Limit = maxObjects;
            return cursor;
        } // end method



        public static Cursor<T> Skip<T>(this Cursor<T> cursor, long? numToSkip)
            where T : new() {
            cursor.Skip = numToSkip;
            return cursor;
        } // end method



        public static Cursor<T> Sort<T>(this Cursor<T> cursor, IDictionary<string, int> sortSpecification)
            where T : new() {
            cursor.Sort = sortSpecification;
            return cursor;
        } // end method



        public static Cursor<T> Sort<T>(this Cursor<T> cursor, object sortObject)
            where T : new() {
            var sortSpecification = new Dictionary<string, int>();
            var sortType = sortObject.GetType();
            var sortMirror = mirrorCache.GetValue(sortType, () => new Mirror(sortType));
            foreach (var prop in sortMirror.GetProperties()) {
                if (!intMirror.IsAssignableFrom(prop.PropertyType))
                    continue;
                var value = Convert.ToInt32(prop.GetValue(sortObject, null));
                sortSpecification[prop.Name] = value;
            } // end foreach

            return cursor.Sort(sortSpecification);
        } // end method

    } // end class
} // end namespace