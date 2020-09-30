using System;
using System.Collections.Generic;
using System.Text;

namespace DatabaseMaster2
{
    /// <summary>
    /// Database Type
    /// </summary>
    public enum DBCommandFactory
    {
        SQLServer=1,
        MySQL=2,
        Oracle=3,
        Access=4,
        DB2=5,
        SQLite = 6,
    }

    /// <summary>
    /// Insert mode
    /// </summary>
    public enum InsertType
    {
        InsertValues,
        InsertSelect
    }

    //Order mode
    public enum SortMode
    {
        Ascending=1,
        Descending=2
    }

    public enum SelectJoinType
    {
        InnerJoin = 0,
        Join = 1,
        LeftJoin = 2,
        RightJoin = 3,
        FullJoin=4,
        CrossJoin=5
    }

    public enum WhereRelation
    {
        And=1,
        Or=2,
        None=3
    }

    //Comparison
    public enum CommandComparison
    {
        Equals = 1,
        NotEquals = 2,
        Like = 3,
        NotLike = 4,
        GreaterThan = 5,
        GreaterOrEquals = 6,
        LessThan = 7,
        LessOrEquals = 8,
        In = 9,
        NotIn = 10,
        Between = 11,
        NotBetween = 12,
        Is=13,
        IsNot=14,
        Or=15,
        And=16
    }

    public class CommonService
    {
        public static String ConvertComparison(CommandComparison Comparison)
        {
            switch (Comparison)
            {
                case CommandComparison.Equals:
                    return "=";
                case CommandComparison.NotEquals:
                    return "<>";
                case CommandComparison.Like:
                    return "like";
                case CommandComparison.NotLike:
                    return "not like";
                case CommandComparison.GreaterThan:
                    return ">";
                case CommandComparison.GreaterOrEquals:
                    return ">=";
                case CommandComparison.LessThan:
                    return "<";
                case CommandComparison.LessOrEquals:
                    return "<=";
                case CommandComparison.In:
                    return "in";
                case CommandComparison.NotIn:
                    return "not in";
                case CommandComparison.Between:
                    return "between";
                case CommandComparison.NotBetween:
                    return "not between";
                case CommandComparison.Is:
                    return "is";
                case CommandComparison.IsNot:
                    return "is not";
                case CommandComparison.Or:
                    return "or";
                case CommandComparison.And:
                    return "and";
                default:
                    throw new Exception("not found comparison.");
            }

        }

        public static String ConvertJoinComparison(SelectJoinType Comparison)
        {
            switch (Comparison)
            {
                case SelectJoinType.InnerJoin:
                    return "inner join";
                case SelectJoinType.LeftJoin:
                    return "left join";
                case SelectJoinType.RightJoin:
                    return "right join";
                case SelectJoinType.Join:
                    return "join";
                case SelectJoinType.FullJoin:
                    return "full join";
                case SelectJoinType.CrossJoin:
                    return "cross join";
                default:
                    throw new Exception("not found comparison.");
            }

        }

        public static String GetWhereRelation(WhereRelation where)
        {
            if (where == WhereRelation.And)
                return "and";
            else if (where == WhereRelation.Or)
                return "or";
            else
                return "";
        }

        public static Boolean CheckValueType(object value)
        {
            String type = value.GetType().ToString();

            if (value.ToString().Equals("NULL") || value.ToString().Equals("null"))
                return false;

            if (value.ToString().StartsWith("(") && value.ToString().EndsWith(")"))
                return false;


            if (type.Contains("String")||type.Contains("Char"))
            {
                return true;
            }
            else if (type.Contains("DateTime"))
            {
                return true;
            }
            else
            {
                return false;
            }
        }
    }
}
