using System;
using System.Collections.Generic;
using System.Text;

namespace DatabaseMaster2
{
    public class SelectDBCommandBuilder
    {
        private string Command;

        private List<String> tablename = new List<String>();
        private List<String> fieldname = new List<String>();
        private List<String> condition = new List<String>();
        private List<String> joincondition = new List<String>();
        private List<String> ordercondition = new List<String>();
        private List<String> groupcondition = new List<String>();
        private List<String> havingcondition = new List<String>();

        private Boolean selectcount=false;
        private Boolean distinct = false;
        private String countname="";
        private Int32 toprecords=0;

        private String Condition = "";

        private String TempString;

        private DBCommandFactory DatabaseType;


        /// <summary>
        /// set default database
        /// </summary>
        /// <param name="DataModule"></param>
        public SelectDBCommandBuilder()
        {
            DatabaseType = DBCommandFactory.SQLServer;
        }

        /// <summary>
        /// set database
        /// </summary>
        /// <param name="DataModule"></param>
        public SelectDBCommandBuilder(DBCommandFactory DataModule)
        {
            DatabaseType = DataModule;
        }

        #region 属性


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

        /// <summary>
        /// set select count mode
        /// </summary>
        public Boolean SelectCount
        {
            set
            {
                selectcount = value;
            }

            get
            {
                return selectcount;
            }
        }

        /// <summary>
        /// set select row limit
        /// </summary>
        public String CountName
        {
            set
            {
                countname = value;
            }

            get
            {
                return countname;
            }
        }

        /// <summary>
        /// set select row number
        /// </summary>
        public Int32 TopRecords
        {
            set
            {
                if (selectcount)
                    throw new Exception("Cannot set with selectCount mode.");

                toprecords = value;
            }

            get
            {
                return toprecords;
            }
        }

        /// <summary>
        /// set select count mode
        /// </summary>
        public Boolean Distinct
        {
            set
            {
                distinct = value;
            }

            get
            {
                return distinct;
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
            tablename.Clear();
            Condition = "";
            condition.Clear();
            joincondition.Clear();
            havingcondition.Clear();
            ordercondition.Clear();
            groupcondition.Clear();
            countname="";
            fieldname.Clear();
            TempString = "";
        }

        /// <summary>
        /// Build last sql command
        /// </summary>
        /// <returns></returns>
        public String BuildCommand()
        {
            Command = "";
            Condition = "";
            TempString = "";

            if (fieldname.Count==0)
                throw new Exception("Column to select to was not set.");

            if (tablename.Count == 0)
                throw new Exception("Column to select to was not set.");

            //1.build fieldname
            if (selectcount)
            {
                if (String.IsNullOrEmpty(countname))
                {
                    TempString = "count(*)";
                }
                else
                {
                    TempString = String.Format("count({0})", countname);
                }
            }
            else
            {
                if (fieldname.Contains("*"))
                    TempString = "*";
                else
                {
                    foreach (String field in fieldname)
                    {
                        TempString += field + ",";
                    }

                    TempString = TempString.Remove(TempString.Length - 1);
                }
            }
            
            Command = String.Format("select {0} ", TempString);

            //2.build tablename
            TempString = "";
            foreach (String table in tablename)
            {
                TempString += table + ",";
            }
            TempString = TempString.Remove(TempString.Length-1);

            Command += String.Format("from {0} ", TempString);

            //3.build join
            if (joincondition.Count > 0)
            {
                TempString = "";
                foreach (String join in joincondition)
                {
                    TempString += join + " ";
                }

                Command += TempString;
            }
           

            //4.build where
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

            //6.build groupby
            if (groupcondition.Count > 0)
            {
                TempString = "";
                foreach (String group in groupcondition)
                {
                    TempString += group + ",";
                }
                TempString = TempString.Remove(TempString.Length - 1);

                Command = Command + " group by " + TempString;
            }

            //5.build orderby
            if (ordercondition.Count > 0)
            {
                TempString = "";
                foreach (String order in ordercondition)
                {
                    TempString += order + ",";
                }
                TempString = TempString.Remove(TempString.Length - 1);

                Command = Command + " order by " + TempString;
            }

            //7.build having
            if (havingcondition.Count > 0)
            {
                TempString = "";
                foreach (String having in havingcondition)
                {
                    TempString += having + " and ";
                }
                TempString = TempString.Remove(TempString.Length - 4);

                Command = Command + " having " + TempString;
            }

            //build rowcount
            if (toprecords > 0)
            {
                Command = DBCommandConvert.GetTopRecords(Command, toprecords.ToString(), DatabaseType);
            }

            //build rowcount
            if (distinct == true && selectcount == false)
            {
                Command = Command.Replace("select", "select distinct ");
            }

            return Command;
        }

        #endregion
        
        #region  插入列名

        /// <summary>
        /// add columns name
        /// </summary>
        /// <param name="ColumnName">cloumn name</param>
        /// <returns></returns>
        public void AddSelectColumn(List<String> ColumnName)
        {
            if (ColumnName.Count == 0)
                throw new Exception("Cloumn name is null.");

            if (fieldname.Contains("*"))
                throw new Exception("Already set ALL columns.");

            foreach (String column in ColumnName)
            {
                fieldname.Add(String.Format("{0}", column));
            }
        }

        /// <summary>
        /// add column name
        /// </summary>
        /// <param name="ColumnName">cloumn name</param>
        /// <param name="Value">cloumn value</param>
        /// <returns></returns>
        public void AddSelectColumn(String ColumnName)
        {
            if (String.IsNullOrEmpty(ColumnName))
                throw new Exception("Cloumn name is null.");

            if (fieldname.Contains("*"))
                throw new Exception("Already set ALL columns.");

            fieldname.Add(String.Format("{0}", ColumnName));
        }

        /// <summary>
        /// add columns name with alias
        /// </summary>
        /// <param name="ColumnName">cloumn name</param>
        /// <returns></returns>
        public void AddSelectColumn(List<String> ColumnName, List<String> AliasName)
        {
            if (ColumnName.Count == 0)
                throw new Exception("Cloumn name is null.");

            if (fieldname.Contains("*"))
                throw new Exception("Already set ALL columns.");

            if (ColumnName.Count!=AliasName.Count)
                throw new Exception("Columns name not match AliasName number.");

            for (int i = 0; i < ColumnName.Count;i++ )
            {
                fieldname.Add(String.Format("{0} {1}", ColumnName[i], AliasName[i]));
            }

        }

        /// <summary>
        /// add table name
        /// </summary>
        /// <param name="ColumnName">cloumn name</param>
        /// <returns></returns>
        public void AddSelectTable(List<String> TableName)
        {
            if (TableName.Count == 0)
                throw new Exception("Cloumn name is null.");

            foreach (String column in TableName)
            {
                tablename.Add(String.Format("{0}", column));
            }
        }

        /// <summary>
        /// add table name
        /// </summary>
        /// <param name="ColumnName">cloumn name</param>
        /// <param name="Value">cloumn value</param>
        /// <returns></returns>
        public void AddSelectTable(String TableName)
        {
            if (String.IsNullOrEmpty(TableName))
                throw new Exception("Cloumn name is null.");

            tablename.Add(String.Format("{0}", TableName));
        }

        /// <summary>
        /// add table name with alias
        /// </summary>
        /// <param name="ColumnName">cloumn name</param>
        /// <returns></returns>
        public void AddSelectTable(List<String> TableName, List<String> AliasName)
        {
            if (TableName.Count == 0)
                throw new Exception("Cloumn name is null.");


            if (TableName.Count != AliasName.Count)
                throw new Exception("Columns name not match AliasName number.");

            for (int i = 0; i < TableName.Count; i++)
            {
                tablename.Add(String.Format("{0} {1}", TableName[i], AliasName[i]));
            }

        }

        /// <summary>
        /// add column name
        /// </summary>
        /// <param name="ColumnName">cloumn name</param>
        /// <param name="Value">cloumn value</param>
        /// <returns></returns>
        public void AddSelectALLColumn()
        {
            fieldname.Add(String.Format("{0}", "*"));
        }

        /// <summary>
        /// set order by values
        /// </summary>
        /// <param name="OrderCondition">order cloumns</param>
        /// <param name="Sort">sort mode</param>
        public void AddOrderBy(String OrderColumn,SortMode Sort)
        {
            if (String.IsNullOrEmpty(OrderColumn))
                throw new Exception("order condition is null.");

            if (Sort==SortMode.Ascending)
                ordercondition.Add(String.Format("{0} {1}", OrderColumn, "asc"));
            else
                ordercondition.Add(String.Format("{0} {1}", OrderColumn, "desc"));
        }

        /// <summary>
        /// set order by values
        /// </summary>
        /// <param name="OrderCondition">order cloumns</param>
        /// <param name="Sort">sort mode</param>
        public void AddOrderBy(String OrderColumn)
        {
            if (String.IsNullOrEmpty(OrderColumn))
                throw new Exception("order condition is null.");

            ordercondition.Add(String.Format("{0}", OrderColumn));

        }

        /// <summary>
        /// set order by values
        /// </summary>
        /// <param name="OrderCondition">order cloumns</param>
        public void AddGroupBy(String GroupColumn)
        {
            if (String.IsNullOrEmpty(GroupColumn))
                throw new Exception("Group condition is null.");

            groupcondition.Add(String.Format("{0}", GroupColumn));

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

            if (!(Comparison==CommandComparison.Between||Comparison==CommandComparison.NotBetween))
                throw new Exception("only support Between comparison.");

            if (CommonService.CheckValueType(value1))
               Condition=String.Format(" {0} {1} {2} {3}",
                     CommonService.GetWhereRelation(Relation), ColumnName, CommonService.ConvertComparison(Comparison), "'" + value1 + "'");
            else
                Condition=String.Format(" {0} {1} {2} {3}",
                     CommonService.GetWhereRelation(Relation), ColumnName, CommonService.ConvertComparison(Comparison), value1);

            if (CommonService.CheckValueType(value2))
                Condition += " and '" + value2 + "'";
            else
                Condition += " and " + value2;

            condition.Add(Condition);
        }

        /// <summary>
        /// add Having condition
        /// </summary>
        /// <param name="ColumnName"></param>
        /// <param name="Comparison"></param>
        /// <param name="value"></param>
        public void AddHaving(String ColumnName, CommandComparison Comparison, object value)
        {
            if (String.IsNullOrEmpty(ColumnName))
                throw new Exception("Values is null.");

            if (groupcondition.Count==0)
                throw new Exception("Need set group by condition first.");

            if (CommonService.CheckValueType(value))
                havingcondition.Add(String.Format("{0} {1} {2}",
                    ColumnName, CommonService.ConvertComparison(Comparison), "'" + value + "'"));
            else
                havingcondition.Add(String.Format("{0} {1} {2}",
                    ColumnName, CommonService.ConvertComparison(Comparison), value));
        }

        /// <summary>
        /// add Having condition
        /// </summary>
        /// <param name="ColumnName"></param>
        /// <param name="Comparison"></param>
        /// <param name="value"></param>
        public void AddHaving(String ColumnName, CommandComparison Comparison, object value1, object value2)
        {
            String Condition;

            if (String.IsNullOrEmpty(ColumnName))
                throw new Exception("Values is null.");

            if (groupcondition.Count == 0)
                throw new Exception("Need set group by condition first.");

            if (!(Comparison == CommandComparison.Between || Comparison == CommandComparison.NotBetween))
                throw new Exception("only support Between comparison.");

            if (CommonService.CheckValueType(value1))
                Condition = String.Format("{0} {1} {2}",
                     ColumnName, CommonService.ConvertComparison(Comparison), "'" + value1 + "'");
            else
                Condition = String.Format("{0} {1} {2}",
                    ColumnName, CommonService.ConvertComparison(Comparison), value1);

            if (CommonService.CheckValueType(value2))
                Condition += "and '" + value2 + "'";
            else
                Condition += "and " + value2;

            havingcondition.Add(Condition);
        }


        /// <summary>
        /// add Having condition
        /// </summary>
        /// <param name="ColumnName"></param>
        /// <param name="Comparison"></param>
        /// <param name="value"></param>
        public void AddHaving(String ColumnName, CommandComparison Comparison, object[] Value)
        {
            String Condition;

            if (String.IsNullOrEmpty(ColumnName))
                throw new Exception("Values is null.");

            if (groupcondition.Count == 0)
                throw new Exception("Need set group by condition first.");

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

            havingcondition.Add(String.Format("{0} {1} {2}",
                    ColumnName, CommonService.ConvertComparison(Comparison), Condition));
        }

        /// <summary>
        /// add Having condition
        /// </summary>
        /// <param name="ColumnName"></param>
        /// <param name="Comparison"></param>
        /// <param name="value"></param>
        public void AddHaving(String ColumnName, CommandComparison Comparison, String SelectCommand)
        {
            if (String.IsNullOrEmpty(ColumnName))
                throw new Exception("Values is null.");

            if (groupcondition.Count == 0)
                throw new Exception("Need set group by condition first.");

            havingcondition.Add(String.Format("{0} {1} ({2})",
                    ColumnName, CommonService.ConvertComparison(Comparison), SelectCommand));

        }



        /// <summary>
        /// add Join condition
        /// </summary>
        /// <param name="type"></param>
        /// <param name="TableName"></param>
        /// <param name="ColumnName"></param>
        /// <param name="Comparison"></param>
        /// <param name="FromTableName"></param>
        /// <param name="FromColumnName"></param>
        public void AddJoin(SelectJoinType type, String TableName, String ColumnName, CommandComparison Comparison, String FromTableName, String FromColumnName)
        {
            if (String.IsNullOrEmpty(ColumnName))
                throw new Exception("Values is null.");

            joincondition.Add(String.Format(" {0} {1} ON {2}.{3} {4} {5}.{6}",CommonService.ConvertJoinComparison(type),TableName,FromTableName,FromColumnName
                ,CommonService.ConvertComparison(Comparison),TableName,ColumnName));
        }

        #endregion
    }
}
