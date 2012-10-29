using System.Collections.Generic;
using System.Text;
using Xanotech.Tools;
using System.Reflection;
using System;

namespace Xanotech.Repository {
    public class Criterion {

        private static Cache<Type, Mirror> mirrorCache = new Cache<Type, Mirror>();

        public string Name { get; set; }
        public string Operation { get; set; }
        public object Value { get; set; }



        public Criterion() {
        } // end constructor



        public Criterion(string name, string operation, object val) {
            Name = name;
            Operation = operation;
            Value = val;
        } // end constructor



        internal static IEnumerable<Criterion> Create(object anonymousTypeCriteria) {
            if (anonymousTypeCriteria == null)
                return null;

            var newCriteria = new List<Criterion>();
            var criteriaType = anonymousTypeCriteria.GetType();
            var criteriaMirror = mirrorCache.GetValue(criteriaType, () => new Mirror(criteriaType));
            foreach (var prop in criteriaMirror.GetProperties()) {
                var value = prop.GetValue(anonymousTypeCriteria, null);
                var criterion = new Criterion();
                criterion.Name = prop.Name;
                if (value == null)
                    criterion.Operation = "IS";
                else if (value is IEnumerable<object>)
                    criterion.Operation = "IN";
                else
                    criterion.Operation = "=";
                criterion.Value = value;
                newCriteria.Add(criterion);
            } // end foreach
            return newCriteria;
        } // end method



        public override string ToString() {
            var whereClause = new StringBuilder();
            whereClause.Append(Name);
            whereClause.Append(' ' + (Operation ?? "=") + ' ');
            whereClause.Append(Value.ToSqlString());
            return whereClause.ToString();
        } // end method



        public string ToString(string tableName) {
            var whereClause = new StringBuilder();
            if (tableName != null)
                whereClause.Append(tableName + '.');
            whereClause.Append(ToString());
            return whereClause.ToString();
        } // end method

    } // end class
} // end namespace