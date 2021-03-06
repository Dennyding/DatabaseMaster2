﻿using System;
using System.Collections.Generic;
using System.Text;

namespace DatabaseMaster2
{

    public class InsertDBCommandBuilder
    {
        private string Command;

        private String tablename = "";
        private String fieldname = "";
        private String valuename = "";
        private List<String> condition = new List<String>();
        private String Condition = "";
        private String selectcommand;
        private Boolean selectidentity;

        private DatabaseType DatabaseType;

        /// <summary>
        /// set default database
        /// </summary>
        /// <param name="DataModule"></param>
        public InsertDBCommandBuilder()
        {
            DatabaseType = DatabaseType.MSSQL;
        }

        /// <summary>
        /// set database
        /// </summary>
        /// <param name="DataModule"></param>
        public InsertDBCommandBuilder(DatabaseType DataModule)
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
        /// Set or get select command
        /// </summary>
        public String SelectCommand
        {
            set
            {
                selectcommand = value;
            }

            get
            {
                return selectcommand;
            }
        }

        /// <summary>
        /// Set needn't cloumn name
        /// </summary>
        public Boolean DefaultCloumnName
        {
            set
            {
                if (value == true)
                    fieldname = "NULL";
            }
        }

        /// <summary>
        /// Set Insert Type
        /// </summary>
        public InsertType InsertCommandType
        {
            set
            {
                if (value == InsertType.InsertSelect)
                    valuename = "NULL";
            }
        }

        /// <summary>
        /// Set whether return insert identity
        /// </summary>
        public Boolean SetSelectIdentity
        {
            set
            {
                selectidentity = value;
            }
        }

        /// <summary>
        /// Set database type
        /// </summary>
        public DatabaseType DatabaseModule
        {
            set
            {
                DatabaseType = value;

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
            valuename = "";
            fieldname = "";
            Condition = "";
            selectcommand = "";
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

            if (String.IsNullOrEmpty(fieldname))
                throw new Exception("Cloumn to insert to was not set.");

            if (String.IsNullOrEmpty(valuename))
                throw new Exception("Value to insert to was not set.");

            if (valuename.Equals("NULL"))
            {
                if (String.IsNullOrEmpty(selectcommand))
                    throw new Exception("Select command was not set.");

                Command = String.Format("Insert into {0}({1}) ({2})", tablename, fieldname, selectcommand);
            }
            else
            {
                Command = String.Format("Insert into {0}({1}) values({2})", tablename, fieldname, valuename);

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
            }

            if (selectidentity)
            {
                switch (DatabaseType)
                {
                    case DatabaseType.MSSQL:
                        Command += ";select @@identity";
                	break;
                    case DatabaseType.MYSQL:
                        Command += ";select LAST_INSERT_ID()";
                    break;
                    case DatabaseType.SQLite:
                        Command += ";select last_insert_rowid();";
                        break;
                    case DatabaseType.Access:
                        Command += ";select LAST_INSERT_ID()";
                        break;
                    case DatabaseType.PostgreSQL:
                        Command += " returning id into id_;";
                        break;
                    case DatabaseType.Oracle:
                        Command += " RETURNING ID INTO newID;";
                        break;
                }


                
            }

            if (fieldname.Equals("NULL"))
                Command = Command.Replace("(NULL)", "");

            return Command;
        }

        #endregion

        #region 插入列名和值

        /// <summary>
        /// add one column name and value
        /// </summary>
        /// <param name="ColumnName">cloumn name</param>
        /// <param name="Value">cloumn value</param>
        /// <returns></returns>
        public void AddInsertColumn(String ColumnName, String Value)
        {
            if (String.IsNullOrEmpty(ColumnName))
                throw new Exception("Cloumn name is null.");

            if (valuename.Equals("NULL"))
                throw new Exception("Already set InsertType.InsertSelect Mode.");

            if (fieldname.Equals("NULL"))
                throw new Exception("Already set DefaultCloumnName Mode.");

            if (String.IsNullOrEmpty(Value))
            {
                Value = "NULL";
            }

            if (fieldname.Length > 0)
                fieldname += ",";

            fieldname += ColumnName;

            if (valuename.Length > 0)
                valuename += ",";

            valuename += Value;

        }

        /// <summary>
        /// add columns name and values name
        /// </summary>
        /// <param name="ColumnName">cloumn name</param>
        /// <param name="Value">cloumn value</param>
        /// <returns></returns>
        public void AddInsertColumn(String[] ColumnName, object[] Value)
        {
            if (ColumnName.Length == 0)
                throw new Exception("Cloumn name is null.");

            if (ColumnName.Length != Value.Length)
                throw new Exception("Cloumns number not equal values number.");

            if (valuename.Equals("NULL"))
                throw new Exception("Already set InsertType.InsertSelect Mode.");

            if (fieldname.Equals("NULL"))
                throw new Exception("Already set DefaultCloumnName Mode.");

            foreach (String column in ColumnName)
            {
                if (fieldname.Length > 0)
                    fieldname += ",";

                fieldname += column;
            }

            for (int i = 0; i < Value.Length; i++)
            {
                if (valuename.Length > 0)
                    valuename += ",";

                if (CommonService.CheckValueType(Value[i]))
                    Value[i] = String.Format("'{0}'", Value[i]);

                valuename += Value[i];
            }
        }

        /// <summary>
        /// add value without columnsname
        /// </summary>
        /// <param name="Value">cloumn value</param>
        /// <returns></returns>
        public void AddInsertColumn(object[] Value)
        {
            if (Value.Length == 0)
                throw new Exception("Values is null.");

            if (valuename.Equals("NULL"))
                throw new Exception("Already set InsertType.InsertSelect Mode.");


            for (int i = 0; i < Value.Length; i++)
            {
                if (valuename.Length > 0)
                    valuename += ",";

                if (CommonService.CheckValueType(Value[i]) && Value[i].ToString().StartsWith("(") == false && Value[i].ToString().EndsWith(")") == false)
                    Value[i] = String.Format("'{0}'", Value[i]);

                valuename += Value[i];
            }

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
