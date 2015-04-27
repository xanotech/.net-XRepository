using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Reflection;
using System.Text;
using XTools;

namespace XRepository {
    using IRecord = IDictionary<string, object>;
    using Record = Dictionary<string, object>;

    public class Criterion {

        public enum OperationType {
            EqualTo, GreaterThan, GreaterThanOrEqualTo, LessThan, LessThanOrEqualTo, Like, NotEqualTo, NotLike
        } // end enum

        private static Cache<Type, Mirror> mirrorCache = new Cache<Type, Mirror>(t => new Mirror(t));

        public string Name { get; set; }
        public OperationType Operation { get; set; }
        public object Value { get; set; }



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



        private static List<T> AddToList<T>(T val, List<T> list) {
            if (list == null)
                list = new List<T>();
            list.Add(val);
            return list;
        } // end method



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
                case OperationType.Like:
                    return "LIKE";
                case OperationType.NotEqualTo:
                    return "!=";
                case OperationType.NotLike:
                    return "NOT LIKE";
                default:
                    return "=";
            } // end switch
        } // end method



        private static OperationType ConvertStringToOperation(string str) {
            if (str == null)
                return OperationType.EqualTo;
            str = str.Trim().ToUpper();

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
                case "LIKE":
                    return OperationType.Like;
                case "NOT LIKE":
                    return OperationType.NotLike;
            } // end switch

            throw new FormatException("OperationType string \"" + str + "\" is invalid.  " +
                "Acceptable values are: =, >, >=, <, <=, !=, LIKE, NOT LIKE (== and <> are also accepted).");
        } // end method



        public Criterion Clone() {
            return new Criterion(Name, Operation, Value);
        } // end method



        internal static IEnumerable<Criterion> Create(IRecord criteriaMap) {
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

            var criteriaMap = new Record(StringComparer.OrdinalIgnoreCase);
            var type = criteriaObj.GetType();
            var mirror = mirrorCache[type];
            foreach (var prop in mirror.GetProperties()) {
                var value = prop.GetValue(criteriaObj, null);
                criteriaMap[prop.Name] = value;
            } // end foreach
            return Create(criteriaMap);
        } // end method



        internal void Distinctify() {
            var vals = GetValues();
            if (vals == null)
                return;

            if (Operation == OperationType.GreaterThan ||
                Operation == OperationType.GreaterThanOrEqualTo) {
                Value = vals.FindMin();
                return;
            } // end if

            if (Operation == OperationType.LessThan ||
                Operation == OperationType.LessThanOrEqualTo) {
                Value = vals.FindMax();
                return;
            } // end if

            var hasNull = false;
            List<bool> boolList = null;
            List<char> charList = null;
            List<long> longList = null;
            List<ulong> ulongList = null;
            List<double> doubleList = null;
            List<decimal> decimalList = null;
            List<DateTime> dateTimeList = null;
            List<string> stringList = null;
            foreach (var val in vals) {
                if (val == null) {
                    hasNull = true;
                    continue;
                } // end if

                switch (Type.GetTypeCode(val.GetType())) {
                    case TypeCode.Boolean: boolList = AddToList((bool)val, boolList); break;
                    case TypeCode.Char: charList = AddToList((char)val, charList); break;
                    case TypeCode.SByte: longList = AddToList((long)((sbyte)val), longList); break;
                    case TypeCode.Int16: longList = AddToList((long)((short)val), longList); break;
                    case TypeCode.Int32: longList = AddToList((long)((int)val), longList); break;
                    case TypeCode.Int64: longList = AddToList((long)val, longList); break;
                    case TypeCode.Byte: ulongList = AddToList((ulong)((byte)val), ulongList); break;
                    case TypeCode.UInt16: ulongList = AddToList((ulong)((ushort)val), ulongList); break;
                    case TypeCode.UInt32: ulongList = AddToList((ulong)((uint)val), ulongList); break;
                    case TypeCode.UInt64: ulongList = AddToList((ulong)val, ulongList); break;
                    case TypeCode.Single: doubleList = AddToList((double)((float)val), doubleList); break;
                    case TypeCode.Double: doubleList = AddToList((double)val, doubleList); break;
                    case TypeCode.Decimal: decimalList = AddToList((decimal)val, decimalList); break;
                    case TypeCode.DateTime: dateTimeList = AddToList((DateTime)val, dateTimeList); break;
                    case TypeCode.String: stringList = AddToList((string)val, stringList); break;
                } // end switch
            } // end foreach

            var distinctList = new List<object>();
            if (hasNull)
                distinctList.Add(null);
            if (boolList != null)
                distinctList.AddRange(boolList.Distinct().Cast<object>());
            if (charList != null)
                distinctList.AddRange(charList.Distinct().Cast<object>());
            if (longList != null)
                distinctList.AddRange(longList.Distinct().Cast<object>());
            if (ulongList != null)
                distinctList.AddRange(ulongList.Distinct().Cast<object>());
            if (doubleList != null)
                distinctList.AddRange(doubleList.Distinct().Cast<object>());
            if (decimalList != null)
                distinctList.AddRange(decimalList.Distinct().Cast<object>());
            if (dateTimeList != null)
                distinctList.AddRange(dateTimeList.Distinct().Cast<object>());
            if (stringList != null)
                distinctList.AddRange(stringList.Distinct().Cast<object>());
            Value = distinctList;
        } // end method



        internal IEnumerable GetValues() {
            var enumerable = Value as IEnumerable;

            // Strings implement IEnumerable so make sure that if Value
            // is null to set the returned enumerable to null
            // as it would be for other basic data types.
            var valStr = Value as string;
            if (valStr != null)
                enumerable = null;

            return enumerable;
        } // end method



        private string FormatValueList(IEnumerable enumerable, bool useParameters, IDbCommand cmd, DataRow schemaRow) {
            var isAfterFirst = false;
            var valueCount = 0;
            var valuesOnLineCount = 0;
            var sql = new StringBuilder("(");
            foreach (var value in enumerable) {
                if (isAfterFirst) {
                    sql.Append(',');
                    if (valuesOnLineCount == 8) {
                        sql.Append(Environment.NewLine);
                        valuesOnLineCount = 0;
                    } else
                        sql.Append(' ');
                } // end if

                if (useParameters) {
                    var parameterName = Name + valueCount;
                    sql.Append(cmd.FormatParameter(parameterName));
                    if (cmd != null)
                        cmd.AddParameter(parameterName, value, schemaRow);
                } else
                    sql.Append(value.ToSqlString());

                isAfterFirst = true;
                valueCount++;
                valuesOnLineCount++;
            } // end foreach
            sql.Append(")");
            return sql.ToString();
        } // end method



        public override string ToString() {
            return ToString(false, null, null);
        } // end method



        internal string ToString(bool useParameters, IDbCommand cmd, DataRow schemaRow) {
            var opStr = ConvertOperationToString(Operation);
            if (opStr == "!=")
                opStr = "<>";

            var val = Value;
            var vals = GetValues();
            string valStr = null;
            if (vals != null)
                switch (Operation) {
                    case OperationType.EqualTo:
                    case OperationType.Like:
                    case OperationType.NotEqualTo:
                    case OperationType.NotLike:
                        opStr = (Operation == OperationType.EqualTo || Operation == OperationType.Like) ? "IN" : "NOT IN";
                        valStr = FormatValueList(vals, useParameters, cmd, schemaRow);
                        break;
                    case OperationType.GreaterThan:
                    case OperationType.GreaterThanOrEqualTo:
                        val = vals.FindMin();
                        vals = null;
                        break;
                    case OperationType.LessThan:
                    case OperationType.LessThanOrEqualTo:
                        val = vals.FindMax();
                        vals = null;
                        break;
                } // end switch

            if (vals == null) {
                if (val == null) {
                    if (Operation == OperationType.EqualTo ||
                        Operation == OperationType.Like)
                        opStr = "IS NULL";
                    else if (Operation == OperationType.NotEqualTo ||
                        Operation == OperationType.NotLike)
                        opStr = "IS NOT NULL";
                } else if (useParameters) {
                    // The following logic adds a 0, 1, 2, etc to the end of Name
                    // until a match isn't alreayd present in cmd.Parameters.
                    // This is to handle the situation where multiple Criterion
                    // exist with the same Name (which is entirely valid).
                    var count = 0;
                    var name = Name;
                    while (cmd.Parameters.Contains(name))
                        name = Name + count++;

                    valStr = cmd.FormatParameter(name);
                    if (cmd != null)
                        cmd.AddParameter(name, val, schemaRow);
                } else
                    valStr = val.ToSqlString();
            } // end if

            return Name + ' ' + opStr + ' ' + valStr;
        } // end method

    } // end class
} // end namespace