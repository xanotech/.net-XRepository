using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using Xanotech.Tools;

namespace Xanotech.Repository {
    public class Criterion {

        public enum OperationType {
            EqualTo, GreaterThan, GreaterThanOrEqualTo, LessThan, LessThanOrEqualTo, NotEqualTo
        } // end enum

        private static Cache<Type, Mirror> mirrorCache = new Cache<Type, Mirror>(t => new Mirror(t));

        public string Name { get; set; }
        public OperationType Operation { get; set; }
        public object Value { get; set; }



        private static string ConvertOperationToString(OperationType operation) {
            switch (operation) {
                case OperationType.EqualTo:
                    return "=";
                case OperationType.GreaterThan:
                    return ">";
                case OperationType.GreaterThanOrEqualTo:
                    return ">=";
                case OperationType.LessThan:
                    return "<";
                case OperationType.LessThanOrEqualTo:
                    return "<=";
                case OperationType.NotEqualTo:
                    return "!=";
                default:
                    return "=";
            }
        } // end method



        private static OperationType ConvertStringToOperation(string str) {
            if (str == null)
                return OperationType.EqualTo;
            str = str.Trim();

            switch (str) {
                case "=":
                case "==":
                    return OperationType.EqualTo;
                case ">":
                    return OperationType.GreaterThan;
                case ">=":
                    return OperationType.GreaterThanOrEqualTo;
                case "<":
                    return OperationType.LessThan;
                case "<=":
                    return OperationType.LessThanOrEqualTo;
                case "<>":
                case "!=":
                    return OperationType.NotEqualTo;
            }

            throw new FormatException("OperationType string \"" + str + "\" is invalid.  " +
                "Acceptable values are: =, >, >=, <, <=, != (== and <> are also accepted).");
        } // end method



        public Criterion() {
        } // end constructor



        public Criterion(string name, OperationType operation, object val) {
            Name = name;
            Operation = operation;
            Value = val;
        } // end constructor



        public Criterion(string name, string operation, object val) {
            Name = name;
            Operation = ConvertStringToOperation(operation);
            Value = val;
        } // end constructor



        internal static IEnumerable<Criterion> Create(IDictionary<string, object> criteriaMap) {
            if (criteriaMap == null)
                return null;

            var newCriteria = new List<Criterion>();
            foreach (var key in criteriaMap.Keys)
                newCriteria.Add(new Criterion(key, "=", criteriaMap[key]));
            return newCriteria;
        } // end method



        internal static IEnumerable<Criterion> Create(object criteriaObj) {
            if (criteriaObj == null)
                return null;

            var criteriaMap = new Dictionary<string, object>();
            var criteriaType = criteriaObj.GetType();
            var criteriaMirror = mirrorCache[criteriaType];
            foreach (var prop in criteriaMirror.GetProperties()) {
                var value = prop.GetValue(criteriaObj, null);
                criteriaMap[prop.Name] = value;
            } // end foreach
            return Create(criteriaMap);
        } // end method



        public override string ToString() {
            var opStr = ConvertOperationToString(Operation);
            if (opStr == "!=")
                opStr = "<>";

            var valStr = Value.ToSqlString();
            var enumerable = Value as IEnumerable;
            if (valStr.StartsWith("(") && valStr.EndsWith(")")) {
                switch (Operation) {
                    case OperationType.GreaterThan:
                    case OperationType.GreaterThanOrEqualTo:
                        valStr = enumerable.FindMin().ToSqlString();
                        break;
                    case OperationType.LessThan:
                    case OperationType.LessThanOrEqualTo:
                        valStr = enumerable.FindMax().ToSqlString();
                        break;
                    case OperationType.NotEqualTo:
                        opStr = "NOT IN";
                        break;
                    default:
                        opStr = "IN";
                        break;
                } // end switch
            } // end else
            return Name + ' ' + opStr + ' ' + valStr;
        } // end method

    } // end class
} // end namespace