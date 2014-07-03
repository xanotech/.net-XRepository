using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Reflection;
using System.Text;
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



        private IEnumerable GetValueList() {
            var enumerable = Value as IEnumerable;
            var valStr = Value as string;
            if (valStr != null)
                enumerable = null;
            return enumerable;
        } // end method



        private string FormatValueList(IEnumerable enumerable, bool useParameters, IDbCommand cmd) {
            var isAfterFirst = false;
            var valueCount = 0;
            var valuesOnLineCount = 0;
            var sqlStr = new StringBuilder("(");
            foreach (var value in enumerable) {
                if (isAfterFirst) {
                    sqlStr.Append(',');
                    if (valuesOnLineCount == 8) {
                        sqlStr.Append(Environment.NewLine);
                        valuesOnLineCount = 0;
                    } else
                        sqlStr.Append(' ');
                } // end if

                if (useParameters) {
                    var parameterName = Name + valueCount;
                    sqlStr.Append('@' + parameterName);
                    if (cmd != null)
                        cmd.AddParameter(parameterName, value);
                } else
                    sqlStr.Append(value.ToSqlString());

                isAfterFirst = true;
                valueCount++;
                valuesOnLineCount++;
            }
            sqlStr.Append(")");
            return sqlStr.ToString();
        } // end method



        public override string ToString() {
            return ToString(null, false);
        } // end method



        internal string ToString(IDbCommand cmd, bool useParameters) {
            var opStr = ConvertOperationToString(Operation);
            if (opStr == "!=")
                opStr = "<>";

            var val = Value;
            var valList = GetValueList();
            string valStr = null;
            if (valList != null)
                switch (Operation) {
                    case OperationType.EqualTo:
                    case OperationType.NotEqualTo:
                        opStr = Operation == OperationType.EqualTo ? "IN" : "NOT IN";
                        valStr = FormatValueList(valList, useParameters, cmd);
                        break;
                    case OperationType.GreaterThan:
                    case OperationType.GreaterThanOrEqualTo:
                        val = valList.FindMin();
                        valList = null;
                        break;
                    case OperationType.LessThan:
                    case OperationType.LessThanOrEqualTo:
                        val = valList.FindMax();
                        valList = null;
                        break;
                } // end switch

            if (valList == null) {
                if (val == null) {
                    if (Operation == OperationType.EqualTo)
                        opStr = "IS";
                    else if (Operation == OperationType.NotEqualTo)
                        opStr = "IS NOT";
                } // end if

                if (useParameters) {
                    valStr = '@' + Name;
                    if (cmd != null)
                        cmd.AddParameter(Name, val);
                } else
                    valStr = val.ToSqlString();
            } // end if

            return Name + ' ' + opStr + ' ' + valStr;
        } // end method

    } // end class
} // end namespace