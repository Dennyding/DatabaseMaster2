using System;
using System.Collections.Generic;
using System.Text;

namespace DatabaseMaster2
{
    public class DeleteDBCommandBuilder
    {
        private string Command;

        private String tablename = "";
        private List<String> condition = new List<String>();
        private String Condition = "";

        private DBCommandFactory DatabaseType;

        /// <summary>
        /// set default database
        /// </summary>
        /// <param name="DataModule"></param>
        public DeleteDBCommandBuilder()
        {
            DatabaseType = DBCommandFactory.SQLServer;
        }

         /// <summary>
        /// set database
        /// </summary>
        /// <param name="DataModule"></param>
        public DeleteDBCommandBuilder(DBCommandFactory DataModule)
        {
            DatabaseType = DataModule;
        }

        #region 属性

        /// <summary>
        /// set or get Table name
        /// </summary>
        public String TableName
        {
            set
            {
                tablename = value;
            }

            get
            {
                return tablename;
            }
        }

        /// <summary>
        /// Set database type
        /// </summary>
        public String DatabaseModule
        {
            set
            {
                if (value.Contains("MSSQL"))
                    DatabaseType = DBCommandFactory.SQLServer;
                else if (value.Contains("MYSQL"))
                    DatabaseType = DBCommandFactory.MySQL;
                else if (value.Contains("Oracle"))
                    DatabaseType = DBCommandFactory.Oracle;
                else
                    DatabaseType = DBCommandFactory.Access;
                    
            }
        }


        #endregion

        #region 生成


        /// <summary>
        /// Clear now command
        /// </summary>
        public void ClearCommand()
        {
            Command = "";
            tablename = "";
            condition.Clear();
            Condition = "";
        }

        /// <summary>
        /// Build last sql command
        /// </summary>
        /// <returns></returns>
        public String BuildCommand()
        {
            Command = "";
            Condition = "";

            if (String.IsNullOrEmpty(tablename))
                throw new Exception("Table to insert to was not set.");

            Command = String.Format("delete from {0}", tablename);

            if (condition.Count > 0)
            {
                Condition += " where" + condition[0];

                for (int i = 1; i < condition.Count; i++)
                {
                    Condition += condition[i];
                }
                Condition = Condition.Replace("where and", "where");
                Command += Condition;
            }

            return Command;
        }

        #endregion

        #region 条件生成

        /// <summary>
        /// add where condition
        /// </summary>
        /// <param name="Relation"></param>
        /// <param name="ColumnName"></param>
        /// <param name="Comparison"></param>
        /// <param name="value"></param>
        public void AddWhere(WhereRelation Relation, String ColumnName, CommandComparison Comparison, object value)
        {
            if (String.IsNullOrEmpty(ColumnName))
                throw new Exception("Values is null.");

            if (CommonService.CheckValueType(value))
                condition.Add(String.Format(" {0} {1} {2} {3}",
                    CommonService.GetWhereRelation(Relation), ColumnName, CommonService.ConvertComparison(Comparison), "'" + value + "'"));
            else
                condition.Add(String.Format(" {0} {1} {2} {3}",
                    CommonService.GetWhereRelation(Relation), ColumnName, CommonService.ConvertComparison(Comparison), value));
        }




        /// <summary>
        /// add where condition with select query
        /// </summary>
        /// <param name="Relation"></param>
        /// <param name="ColumnName"></param>
        /// <param name="Comparison"></param>
        /// <param name="SelectCommand"></param>
        public void AddWhereBySelect(WhereRelation Relation, String ColumnName, CommandComparison Comparison, String SelectCommand)
        {
            if (String.IsNullOrEmpty(ColumnName))
                throw new Exception("ColumnName is null.");


            condition.Add(String.Format(" {0} {1} {2} ({3})",
                    CommonService.GetWhereRelation(Relation), ColumnName, CommonService.ConvertComparison(Comparison), SelectCommand));
        }


        /// <summary>
        /// add where condition with RelationShip
        /// </summary>
        /// <param name="Relation"></param>
        /// <param name="ColumnName"></param>
        /// <param name="Comparison"></param>
        /// <param name="ColumnName"></param>
        public void AddWhereByRelationShip(WhereRelation Relation, String LeftColumnName, CommandComparison Comparison, String RightColumnName)
        {
            if (String.IsNullOrEmpty(LeftColumnName) || String.IsNullOrEmpty(RightColumnName))
                throw new Exception("Cloumn name is null.");


            condition.Add(String.Format(" {0} {1} {2} {3}",
                        CommonService.GetWhereRelation(Relation), LeftColumnName, CommonService.ConvertComparison(Comparison), RightColumnName));

        }


        /// <summary>
        /// add where condition with list
        /// </summary>
        /// <param name="Relation"></param>
        /// <param name="ColumnName"></param>
        /// <param name="Comparison"></param>
        /// <param name="Value"></param>
        public void AddWhere(WhereRelation Relation, String ColumnName, CommandComparison Comparison, object[] Value)
        {
            String Condition;

            if (String.IsNullOrEmpty(ColumnName))
                throw new Exception("Values is null.");

            if (!(Comparison == CommandComparison.In || Comparison == CommandComparison.NotIn))
                throw new Exception("only support In comparison.");

            Condition = "(";
            foreach (object value in Value)
            {
                if (CommonService.CheckValueType(value))
                    Condition = Condition + "'" + value + "',";
                else
                    Condition = Condition + value + ",";
            }
            Condition += ")";
            Condition = Condition.Replace(",)", ")");

            condition.Add(String.Format(" {0} {1} {2} {3}",
                    CommonService.GetWhereRelation(Relation), ColumnName, CommonService.ConvertComparison(Comparison), Condition));
        }


        /// <summary>
        /// add where condition with between
        /// </summary>
        /// <param name="Relation"></param>
        /// <param name="ColumnName"></param>
        /// <param name="Comparison"></param>
        /// <param name="value1"></param>
        /// <param name="value2"></param>
        public void AddWhere(WhereRelation Relation, String ColumnName, CommandComparison Comparison, object value1, object value2)
        {
            String Condition;

            if (String.IsNullOrEmpty(ColumnName))
                throw new Exception("Values is null.");

            if (!(Comparison == CommandComparison.Between || Comparison == CommandComparison.NotBetween))
                throw new Exception("only support Between comparison.");

            if (CommonService.CheckValueType(value1))
                Condition = String.Format(" {0} {1} {2} {3}",
                      CommonService.GetWhereRelation(Relation), ColumnName, CommonService.ConvertComparison(Comparison), "'" + value1 + "'");
            else
                Condition = String.Format(" {0} {1} {2} {3}",
                     CommonService.GetWhereRelation(Relation), ColumnName, CommonService.ConvertComparison(Comparison), value1);

            if (CommonService.CheckValueType(value2))
                Condition += " and '" + value2 + "'";
            else
                Condition += " and " + value2;

            condition.Add(Condition);
        }

        #endregion

    }
}
