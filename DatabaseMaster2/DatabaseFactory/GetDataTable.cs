using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using DatabaseMaster;
using System.Data;

namespace DatabaseLayer
{
    public class GetDatabaseTable
    {

        /// <summary>
        /// 检查表中是否存在指定内容
        /// </summary>
        /// <returns></returns>
        public static Boolean CheckDatabaseConnect()
        {
            try
            {
                //数据库连接
                DatabaseInterface database = DBFactory.CreateDatabase(DatabaseInit.DefaultDatabase, DatabaseInit.ConnectName, DatabaseInit.EncryptType);
                database.Open();
                Int64 count = Convert.ToInt64(database.GetSpeciaRecordValue("select 1", DatabaseInit.WaitTimeout));
                database.Close();

                if (count == 0)
                    return false;
                else
                    return true;
            }
            catch (Exception)
            {
                return false;
            }

        }


        /// <summary>
        /// 得到表中所有数据
        /// </summary>
        /// <param name="TableName"></param>
        /// <returns></returns>
        public static DataTable GetAllDataTable(String TableName)
        {

            //sql生成
            SelectDBCommandBuilder sql = new SelectDBCommandBuilder();
            sql.AddSelectTable(TableName);
            sql.AddSelectALLColumn();

                        
            //数据库连接
            DatabaseInterface database = DBFactory.CreateDatabase(DatabaseInit.DefaultDatabase, DatabaseInit.ConnectName, DatabaseInit.EncryptType);
            database.Open();
            DataSet ds = database.GetDataSet(sql.BuildCommand(), DatabaseInit.WaitTimeout);
            database.Close();

            return ds.Tables[0];
        }


        /// <summary>
        /// 得到表中所有数据
        /// </summary>
        /// <param name="TableName"></param>
        /// <param name="OrderByName"></param>
        /// <returns></returns>
        public static DataTable GetAllDataTable(String TableName, String OrderByName)
        {

            //sql生成
            SelectDBCommandBuilder sql = new SelectDBCommandBuilder();
            sql.AddSelectTable(TableName);
            sql.AddSelectALLColumn();
            sql.AddOrderBy(OrderByName);


            //数据库连接
            DatabaseInterface database = DBFactory.CreateDatabase(DatabaseInit.DefaultDatabase, DatabaseInit.ConnectName, DatabaseInit.EncryptType);
            database.Open();
            DataSet ds = database.GetDataSet(sql.BuildCommand(), DatabaseInit.WaitTimeout);
            database.Close();

            return ds.Tables[0];
        }

        /// <summary>
        /// 得到指定条件的表中所有数据
        /// </summary>
        /// <param name="TableName"></param>
        /// <param name="ColumnName"></param>
        /// <param name="Value"></param>
        /// <returns></returns>
        public static DataTable GetAllDataTable(String TableName, String ColumnName, Object Value)
        {

            //sql生成
            SelectDBCommandBuilder sql = new SelectDBCommandBuilder();
            sql.AddSelectTable(TableName);
            sql.AddSelectALLColumn();
            sql.AddWhere(WhereRelation.None, ColumnName, DatabaseMaster.CommandComparison.Equals, Value);


            //数据库连接
            DatabaseInterface database = DBFactory.CreateDatabase(DatabaseInit.DefaultDatabase, DatabaseInit.ConnectName, DatabaseInit.EncryptType);
            database.Open();
            DataSet ds = database.GetDataSet(sql.BuildCommand(), DatabaseInit.WaitTimeout);
            database.Close();

            return ds.Tables[0];
        }

        /// <summary>
        /// 得到指定条件的表中所有数据
        /// </summary>
        /// <param name="TableName"></param>
        /// <param name="ColumnName"></param>
        /// <param name="Value"></param>
        /// <param name="OrderByName"></param>
        /// <returns></returns>
        public static DataTable GetAllDataTable(String TableName, String ColumnName, Object Value, String OrderByName)
        {

            //sql生成
            SelectDBCommandBuilder sql = new SelectDBCommandBuilder();
            sql.AddSelectTable(TableName);
            sql.AddSelectALLColumn();
            sql.AddWhere(WhereRelation.None, ColumnName, DatabaseMaster.CommandComparison.Equals, Value);
            sql.AddOrderBy(OrderByName,SortMode.Ascending);

            //数据库连接
            DatabaseInterface database = DBFactory.CreateDatabase(DatabaseInit.DefaultDatabase, DatabaseInit.ConnectName, DatabaseInit.EncryptType);
            database.Open();
            DataSet ds = database.GetDataSet(sql.BuildCommand(), DatabaseInit.WaitTimeout);
            database.Close();

            return ds.Tables[0];
        }

        /// <summary>
        /// 得到指定条件的表中所有数据
        /// </summary>
        /// <param name="TableName"></param>
        /// <param name="ColumnName"></param>
        /// <param name="Comparison"></param>
        /// <param name="Value"></param>
        /// <returns></returns>
        public static DataTable GetAllDataTable(String TableName, String ColumnName, DatabaseMaster.CommandComparison Comparison, Object Value)
        {

            //sql生成
            SelectDBCommandBuilder sql = new SelectDBCommandBuilder();
            sql.AddSelectTable(TableName);
            sql.AddSelectALLColumn();
            sql.AddWhere(WhereRelation.None, ColumnName, (DatabaseMaster.CommandComparison)Comparison, Value);


            //数据库连接
            DatabaseInterface database = DBFactory.CreateDatabase(DatabaseInit.DefaultDatabase, DatabaseInit.ConnectName, DatabaseInit.EncryptType);
            database.Open();
            DataSet ds = database.GetDataSet(sql.BuildCommand(), DatabaseInit.WaitTimeout);
            database.Close();

            return ds.Tables[0];
        }

        /// <summary>
        /// 得到得到多个指定条件的表中所有数据
        /// </summary>
        /// <param name="TableName"></param>
        /// <param name="ColumnName"></param>
        /// <param name="Value"></param>
        /// <returns></returns>
        public static DataTable GetAllDataTable(String TableName, String[] ColumnName, Object[] Value)
        {

            //sql生成
            SelectDBCommandBuilder sql = new SelectDBCommandBuilder();
            sql.AddSelectTable(TableName);
            sql.AddSelectALLColumn();

            for (int i = 0; i < ColumnName.Length; i++)
            {
                sql.AddWhere(WhereRelation.And, ColumnName[i], DatabaseMaster.CommandComparison.Equals, Value[i]);
            }



            //数据库连接
            DatabaseInterface database = DBFactory.CreateDatabase(DatabaseInit.DefaultDatabase, DatabaseInit.ConnectName, DatabaseInit.EncryptType);
            database.Open();
            DataSet ds = database.GetDataSet(sql.BuildCommand(), DatabaseInit.WaitTimeout);
            database.Close();

            return ds.Tables[0];
        }


        /// <summary>
        /// 得到得到多个指定条件的表中所有数据
        /// </summary>
        /// <param name="TableName"></param>
        /// <param name="ColumnName"></param>
        /// <param name="Comparison"></param>
        /// <param name="Value"></param>
        /// <returns></returns>
        public static DataTable GetAllDataTable(String TableName, String[] ColumnName, DatabaseMaster.CommandComparison[] Comparison, Object[] Value)
        {

            //sql生成
            SelectDBCommandBuilder sql = new SelectDBCommandBuilder();
            sql.AddSelectTable(TableName);
            sql.AddSelectALLColumn();

            for (int i = 0; i < ColumnName.Length; i++)
            {
                sql.AddWhere(WhereRelation.And, ColumnName[i], (DatabaseMaster.CommandComparison)Comparison[i], Value[i]);
            }


            //数据库连接
            DatabaseInterface database = DBFactory.CreateDatabase(DatabaseInit.DefaultDatabase, DatabaseInit.ConnectName, DatabaseInit.EncryptType);
            database.Open();
            DataSet ds = database.GetDataSet(sql.BuildCommand(), DatabaseInit.WaitTimeout);
            database.Close();

            return ds.Tables[0];
        }

        /// <summary>
        /// 得到得到多个指定条件的表中所有数据
        /// </summary>
        /// <param name="TableName"></param>
        /// <param name="GroupColumnName"></param>
        /// <param name="ColumnName"></param>
        /// <param name="Comparison"></param>
        /// <param name="Value"></param>
        /// <param name="GroupByName"></param>
        /// <returns></returns>
        public static DataTable GetAllDataTable(String TableName, List<String> GroupColumnName, String[] ColumnName, DatabaseMaster.CommandComparison[] Comparison, Object[] Value, String GroupByName)
        {

            //sql生成
            SelectDBCommandBuilder sql = new SelectDBCommandBuilder();
            sql.AddSelectTable(TableName);
            sql.AddSelectColumn(GroupColumnName);

            sql.AddGroupBy(GroupByName);

            for (int i = 0; i < ColumnName.Length; i++)
            {
                sql.AddWhere(WhereRelation.And, ColumnName[i], (DatabaseMaster.CommandComparison)Comparison[i], Value[i]);
            }


            //数据库连接
            DatabaseInterface database = DBFactory.CreateDatabase(DatabaseInit.DefaultDatabase, DatabaseInit.ConnectName, DatabaseInit.EncryptType);
            database.Open();
            DataSet ds = database.GetDataSet(sql.BuildCommand(), DatabaseInit.WaitTimeout);
            database.Close();

            return ds.Tables[0];
        }

        /// <summary>
        /// 得到得到多个指定条件的多表中所有数据
        /// </summary>
        /// <param name="TableName"></param>
        /// <param name="ColumnName"></param>
        /// <param name="Comparison"></param>
        /// <param name="Value"></param>
        /// <param name="Table1ColumnName"></param>
        /// <param name="Table2ColumnName"></param>
        /// <returns></returns>
        public static DataTable GetAllDataTable(List<String> TableName, String[] ColumnName, DatabaseMaster.CommandComparison[] Comparison, Object[] Value,
            String[] Table1ColumnName, String[] Table2ColumnName)
        {

            //sql生成
            SelectDBCommandBuilder sql = new SelectDBCommandBuilder();
            sql.AddSelectTable(TableName);
            sql.AddSelectALLColumn();

            for (int i = 0; i < ColumnName.Length; i++)
            {
                sql.AddWhere(WhereRelation.And, ColumnName[i], (DatabaseMaster.CommandComparison)Comparison[i], Value[i]);
            }

            for (int i = 0; i < Table1ColumnName.Length; i++)
            {
                sql.AddWhereByRelationShip(WhereRelation.And, Table1ColumnName[i], DatabaseMaster.CommandComparison.Equals, Table2ColumnName[i]);
            }


            //数据库连接
            DatabaseInterface database = DBFactory.CreateDatabase(DatabaseInit.DefaultDatabase, DatabaseInit.ConnectName, DatabaseInit.EncryptType);
            database.Open();
            DataSet ds = database.GetDataSet(sql.BuildCommand(), DatabaseInit.WaitTimeout);
            database.Close();

            return ds.Tables[0];
        }

        /// <summary>
        /// 得到得到多个指定条件的多表中所有数据
        /// </summary>
        /// <param name="TableName"></param>
        /// <param name="Table1ColumnName"></param>
        /// <param name="Table2ColumnName"></param>
        /// <returns></returns>
        public static DataTable GetAllDataTable(List<String> TableName,String[] Table1ColumnName, String[] Table2ColumnName)
        {

            //sql生成
            SelectDBCommandBuilder sql = new SelectDBCommandBuilder();
            sql.AddSelectTable(TableName);
            sql.AddSelectALLColumn();


            for (int i = 0; i < Table1ColumnName.Length; i++)
            {
                sql.AddWhereByRelationShip(WhereRelation.And, Table1ColumnName[i], DatabaseMaster.CommandComparison.Equals, Table2ColumnName[i]);
            }


            //数据库连接
            DatabaseInterface database = DBFactory.CreateDatabase(DatabaseInit.DefaultDatabase, DatabaseInit.ConnectName, DatabaseInit.EncryptType);
            database.Open();
            DataSet ds = database.GetDataSet(sql.BuildCommand(), DatabaseInit.WaitTimeout);
            database.Close();

            return ds.Tables[0];
        }

        /// <summary>
        /// 得到得到多个指定条件的表中所有数据，并排序
        /// </summary>
        /// <param name="TableName"></param>
        /// <param name="ColumnName"></param>
        /// <param name="Value"></param>
        /// <param name="OrderByName"></param>
        /// <returns></returns>
        public static DataTable GetAllDataTable(String TableName, String[] ColumnName, Object[] Value, String OrderByName)
        {

            //sql生成
            SelectDBCommandBuilder sql = new SelectDBCommandBuilder();
            sql.AddSelectTable(TableName);
            sql.AddSelectALLColumn();
            sql.AddOrderBy(OrderByName);

            for (int i = 0; i < ColumnName.Length; i++)
            {
                sql.AddWhere(WhereRelation.And, ColumnName[i], DatabaseMaster.CommandComparison.Equals, Value[i]);
            }



            //数据库连接
            DatabaseInterface database = DBFactory.CreateDatabase(DatabaseInit.DefaultDatabase, DatabaseInit.ConnectName, DatabaseInit.EncryptType);
            database.Open();
            DataSet ds = database.GetDataSet(sql.BuildCommand(), DatabaseInit.WaitTimeout);
            database.Close();

            return ds.Tables[0];
        }

        /// <summary>
        /// 得到得到多个指定条件的表中所有数据，并排序
        /// </summary>
        /// <param name="TableName"></param>
        /// <param name="ColumnName"></param>
        /// <param name="Value"></param>
        /// <param name="OrderByName"></param>
        /// <returns></returns>
        public static DataTable GetAllDataTable(String TableName, String[] ColumnName, DatabaseMaster.CommandComparison[] Comparison, Object[] Value, String OrderByName)
        {

            //sql生成
            SelectDBCommandBuilder sql = new SelectDBCommandBuilder();
            sql.AddSelectTable(TableName);
            sql.AddSelectALLColumn();
            sql.AddOrderBy(OrderByName);

            for (int i = 0; i < ColumnName.Length; i++)
            {
                sql.AddWhere(WhereRelation.And, ColumnName[i], (DatabaseMaster.CommandComparison)Comparison[i], Value[i]);
            }

            //数据库连接
            DatabaseInterface database = DBFactory.CreateDatabase(DatabaseInit.DefaultDatabase, DatabaseInit.ConnectName, DatabaseInit.EncryptType);
            database.Open();
            DataSet ds = database.GetDataSet(sql.BuildCommand(), DatabaseInit.WaitTimeout);
            database.Close();

            return ds.Tables[0];
        }

        /// <summary>
        /// 得到得到多个指定条件的表中所有数据，并排序
        /// </summary>
        /// <param name="TableName"></param>
        /// <param name="ColumnName"></param>
        /// <param name="Value"></param>
        /// <param name="OrderByName"></param>
        /// <returns></returns>
        public static DataTable GetAllDataTable(String TableName, String[] ColumnName, DatabaseMaster.CommandComparison[] Comparison, Object[] Value, String OrderByName, Boolean SortAscending)
        {

            //sql生成
            SelectDBCommandBuilder sql = new SelectDBCommandBuilder();
            sql.AddSelectTable(TableName);
            sql.AddSelectALLColumn();
            sql.AddOrderBy(OrderByName, SortAscending?SortMode.Ascending:SortMode.Descending);

            for (int i = 0; i < ColumnName.Length; i++)
            {
                sql.AddWhere(WhereRelation.And, ColumnName[i], (DatabaseMaster.CommandComparison)Comparison[i], Value[i]);
            }



            //数据库连接
            DatabaseInterface database = DBFactory.CreateDatabase(DatabaseInit.DefaultDatabase, DatabaseInit.ConnectName, DatabaseInit.EncryptType);
            database.Open();
            DataSet ds = database.GetDataSet(sql.BuildCommand(), DatabaseInit.WaitTimeout);
            database.Close();

            return ds.Tables[0];
        }



        /// <summary>
        /// 得到表中所有数据
        /// </summary>
        /// <param name="TableName"></param>
        /// <param name="ColumnName"></param>
        /// <param name="KeyColumnName"></param>
        /// <param name="KeyValue"></param>
        /// <returns></returns>
        public static DataTable GetAllDataTable(String TableName, List<String> ColumnName, String KeyColumnName, Object KeyValue)
        {

            //sql生成
            SelectDBCommandBuilder sql = new SelectDBCommandBuilder();
            sql.AddSelectTable(TableName);
            sql.AddSelectColumn(ColumnName);
            sql.AddWhere(WhereRelation.None, KeyColumnName, DatabaseMaster.CommandComparison.Equals, KeyValue);


            //数据库连接
            DatabaseInterface database = DBFactory.CreateDatabase(DatabaseInit.DefaultDatabase, DatabaseInit.ConnectName, DatabaseInit.EncryptType);
            database.Open();
            DataSet ds = database.GetDataSet(sql.BuildCommand(), DatabaseInit.WaitTimeout);
            database.Close();

            return ds.Tables[0];
        }

        /// <summary>
        /// 检查表中是否存在指定内容
        /// </summary>
        /// <param name="TableName"></param>
        /// <param name="ColumnName"></param>
        /// <param name="Value"></param>
        /// <returns></returns>
        public static Boolean CheckSpecifiedValueExist(String TableName, String ColumnName, Object Value)
        {

            //sql生成
            SelectDBCommandBuilder sql = new SelectDBCommandBuilder();
            sql.AddSelectTable(TableName);
            sql.AddSelectALLColumn();
            sql.AddWhere(WhereRelation.None, ColumnName, DatabaseMaster.CommandComparison.Equals, Value);
            sql.SelectCount = true;

            //数据库连接
            DatabaseInterface database = DBFactory.CreateDatabase(DatabaseInit.DefaultDatabase, DatabaseInit.ConnectName, DatabaseInit.EncryptType);
            database.Open();
            Int64 count =  Convert.ToInt64(database.GetSpeciaRecordValue(sql.BuildCommand(), DatabaseInit.WaitTimeout));
            database.Close();

            if (count == 0)
                return false;
            else
                return true;
        }

        /// <summary>
        /// 检查表中是否存在指定内容
        /// </summary>
        /// <param name="TableName"></param>
        /// <param name="ColumnName"></param>
        /// <param name="Value"></param>
        /// <returns></returns>
        public static Boolean CheckSpecifiedValueExist(String TableName, String[] ColumnName, Object[] Value)
        {

            //sql生成
            SelectDBCommandBuilder sql = new SelectDBCommandBuilder();
            sql.AddSelectTable(TableName);
            sql.AddSelectALLColumn();

            for (int i = 0; i < ColumnName.Length; i++)
            {
                sql.AddWhere(WhereRelation.And, ColumnName[i], DatabaseMaster.CommandComparison.Equals, Value[i]);
            }



            sql.SelectCount = true;

            //数据库连接
            DatabaseInterface database = DBFactory.CreateDatabase(DatabaseInit.DefaultDatabase, DatabaseInit.ConnectName, DatabaseInit.EncryptType);
            database.Open();
            Int64 count = Convert.ToInt64(database.GetSpeciaRecordValue(sql.BuildCommand(), DatabaseInit.WaitTimeout));
            database.Close();

            if (count == 0)
                return false;
            else
                return true;
        }

        /// <summary>
        /// 检查表中是否存在指定内容
        /// </summary>
        /// <param name="TableName"></param>
        /// <param name="ColumnName"></param>
        /// <param name="Comparison"></param>
        /// <param name="Value"></param>
        /// <returns></returns>
        public static Boolean CheckSpecifiedValueExist(String TableName, String[] ColumnName, DatabaseMaster.CommandComparison[] Comparison, Object[] Value)
        {

            //sql生成
            SelectDBCommandBuilder sql = new SelectDBCommandBuilder();
            sql.AddSelectTable(TableName);
            sql.AddSelectALLColumn();

            for (int i = 0; i < ColumnName.Length; i++)
            {
                sql.AddWhere(WhereRelation.And, ColumnName[i], (DatabaseMaster.CommandComparison)Comparison[i], Value[i]);
            }



            sql.SelectCount = true;

            //数据库连接
            DatabaseInterface database = DBFactory.CreateDatabase(DatabaseInit.DefaultDatabase, DatabaseInit.ConnectName, DatabaseInit.EncryptType);
            database.Open();
            Int64 count = Convert.ToInt64(database.GetSpeciaRecordValue(sql.BuildCommand(), DatabaseInit.WaitTimeout));
            database.Close();

            if (count == 0)
                return false;
            else
                return true;
        }


        /// <summary>
        /// 检查存储过程中是否存在指定内容
        /// </summary>
        /// <param name="ProcedureCommand"></param>
        /// <param name="parameterin"></param>
        /// <returns></returns>
        public static Boolean CheckSpecifiedValueExist(String ProcedureCommand,  ParameterClass[] parameterin)
        {
            //数据库连接
            DatabaseInterface database = DBFactory.CreateDatabase(DatabaseInit.DefaultDatabase, DatabaseInit.ConnectName, DatabaseInit.EncryptType);
            database.Open();
            Int32 count = Convert.ToInt32(database.GetSpeciaRecordValue(ProcedureCommand, parameterin, DatabaseInit.WaitTimeout));
            database.Close();

            if (count == 0)
                return false;
            else
                return true;
        }

        /// <summary>
        /// 检查存储过程中是否存在指定内容
        /// </summary>
        /// <param name="ProcedureCommand"></param>
        /// <param name="parameterin"></param>
        /// <param name="parameterout"></param>
        /// <returns></returns>
        public static String[] GetProcedureOutput(String ProcedureCommand, ParameterClass[] parameterin, ParameterOutClass[] parameterout)
        {
            String[] value;

            //数据库连接
            DatabaseInterface database = DBFactory.CreateDatabase(DatabaseInit.DefaultDatabase, DatabaseInit.ConnectName, DatabaseInit.EncryptType);
            database.Open();
            value=database.ExecueStoreProCommand(ProcedureCommand, parameterin, parameterout, DatabaseInit.WaitTimeout);
            database.Close();

            return value;
        }

        /// <summary>
        /// 检查存储过程中是否存在指定内容
        /// </summary>
        /// <param name="ProcedureCommand"></param>
        /// <param name="parameterin"></param>
        /// <param name="parameterout"></param>
        /// <returns></returns>
        public static String[] GetProcedureOutput(String ProcedureCommand, ParameterInClass[] parameterin, ParameterOutClass[] parameterout)
        {
            String[] value;

            //数据库连接
            DatabaseInterface database = DBFactory.CreateDatabase(DatabaseInit.DefaultDatabase, DatabaseInit.ConnectName, DatabaseInit.EncryptType);
            database.Open();
            value = database.ExecueStoreProCommand(ProcedureCommand, parameterin, parameterout, DatabaseInit.WaitTimeout);
            database.Close();

            return value;
        }

        /// <summary>
        /// 执行SQL语句
        /// </summary>
        /// <param name="ProcedureCommand"></param>
        /// <returns></returns>
        public static void ExecSQL(String ProcedureCommand)
        {
            //数据库连接
            DatabaseInterface database = DBFactory.CreateDatabase(DatabaseInit.DefaultDatabase, DatabaseInit.ConnectName, DatabaseInit.EncryptType);
            database.Open();
            database.ExecueCommand(ProcedureCommand, DatabaseInit.WaitTimeout);
            database.Close();

        }

        /// <summary>
        /// 执行SQL语句
        /// </summary>
        /// <param name="ProcedureCommand"></param>

        /// <returns></returns>
        public static DataSet ExecSQLCommand(String SQLCommand)
        {
            //数据库连接
            DatabaseInterface database = DBFactory.CreateDatabase(DatabaseInit.DefaultDatabase, DatabaseInit.ConnectName, DatabaseInit.EncryptType);
            database.Open();
            DataSet ds = database.GetDataSet(SQLCommand, DatabaseInit.WaitTimeout);
            database.Close();

            return ds;
        }

        /// <summary>
        /// 执行存储过程
        /// </summary>
        /// <param name="ProcedureCommand"></param>
        /// <param name="parameterin"></param>
        public static void ExecProcedure(String ProcedureCommand, ParameterClass[] parameterin)
        {
            //数据库连接
            DatabaseInterface database = DBFactory.CreateDatabase(DatabaseInit.DefaultDatabase, DatabaseInit.ConnectName, DatabaseInit.EncryptType);
            database.Open();
            database.ExecueStoreProCommand(ProcedureCommand, parameterin, DatabaseInit.WaitTimeout);
            database.Close();

        }


        /// <summary>
        /// 执行存储过程
        /// </summary>
        /// <param name="ProcedureCommand"></param>
        /// <param name="parameterin"></param>
        /// <param name="ReturnDataset"></param>
        /// <returns></returns>
        public static DataSet ExecProcedure(String ProcedureCommand, ParameterClass[] parameterin, Boolean ReturnDataset)
        {
            DataSet ds = new DataSet();

            //数据库连接
            DatabaseInterface database = DBFactory.CreateDatabase(DatabaseInit.DefaultDatabase, DatabaseInit.ConnectName, DatabaseInit.EncryptType);
            database.Open();
            ds=database.ExecueStoreProCommand(ProcedureCommand, parameterin, DatabaseInit.WaitTimeout);
            database.Close();

            return ds;
        }

        /// <summary>
        /// 得到指定条件的一个数据
        /// </summary>
        /// <param name="TableName"></param>
        /// <param name="ColumnName"></param>
        /// <param name="KeyColumnName"></param>
        /// <param name="KeyValue"></param>
        /// <returns></returns>
        public static Object GetSpeciaRecord(String TableName, String ColumnName, String KeyColumnName, Object KeyValue)
        {

            //sql生成
            SelectDBCommandBuilder sql = new SelectDBCommandBuilder();
            sql.AddSelectTable(TableName);
            sql.AddSelectColumn(ColumnName);
            sql.AddWhere(WhereRelation.None, KeyColumnName, DatabaseMaster.CommandComparison.Equals, KeyValue);

            //数据库连接
            DatabaseInterface database = DBFactory.CreateDatabase(DatabaseInit.DefaultDatabase, DatabaseInit.ConnectName, DatabaseInit.EncryptType);
            database.Open();
            Object result = database.GetSpeciaRecordValue(sql.BuildCommand(), DatabaseInit.WaitTimeout);
            database.Close();

            return result;
        }


        /// <summary>
        /// 得到指定条件的一个数据
        /// </summary>
        /// <param name="TableName"></param>
        /// <param name="ColumnName"></param>
        /// <param name="KeyColumnName"></param>
        /// <param name="KeyValue"></param>
        /// <returns></returns>
        public static Object GetSpeciaRecord(String TableName, String ColumnName, String[] KeyColumnName, Object[] KeyValue)
        {

            //sql生成
            SelectDBCommandBuilder sql = new SelectDBCommandBuilder();
            sql.AddSelectTable(TableName);
            sql.AddSelectColumn(ColumnName);

            for (int i = 0; i < KeyColumnName.Length; i++)
            {
                sql.AddWhere(WhereRelation.And, KeyColumnName[i], DatabaseMaster.CommandComparison.Equals, KeyValue[i]);
            }

            //数据库连接
            DatabaseInterface database = DBFactory.CreateDatabase(DatabaseInit.DefaultDatabase, DatabaseInit.ConnectName, DatabaseInit.EncryptType);
            database.Open();
            Object result = database.GetSpeciaRecordValue(sql.BuildCommand(), DatabaseInit.WaitTimeout);
            database.Close();

            return result;
        }

        /// <summary>
        ///  得到指定条件的行计数数据
        /// </summary>
        /// <param name="TableName"></param>
        /// <param name="ColumnName"></param>
        /// <param name="KeyColumnName"></param>
        /// <param name="KeyValue"></param>
        /// <param name="CountNumber"></param>
        /// <returns></returns>
        public static Object GetSpeciaRecord(String TableName, String ColumnName, String[] KeyColumnName, Object[] KeyValue, Boolean CountNumber)
        {

            //sql生成
            SelectDBCommandBuilder sql = new SelectDBCommandBuilder();
            sql.AddSelectTable(TableName);
            sql.AddSelectColumn(ColumnName);
            sql.SelectCount = true;

            for (int i = 0; i < KeyColumnName.Length; i++)
            {
                sql.AddWhere(WhereRelation.And, KeyColumnName[i], DatabaseMaster.CommandComparison.Equals, KeyValue[i]);
            }

            //数据库连接
            DatabaseInterface database = DBFactory.CreateDatabase(DatabaseInit.DefaultDatabase, DatabaseInit.ConnectName, DatabaseInit.EncryptType);
            database.Open();
            Object result = database.GetSpeciaRecordValue(sql.BuildCommand(), DatabaseInit.WaitTimeout);
            database.Close();

            return result;
        }

        /// <summary>
        ///  得到指定条件的行计数数据
        /// </summary>
        /// <param name="TableName"></param>
        /// <param name="ColumnName"></param>
        /// <param name="KeyColumnName"></param>
        /// <param name="KeyValue"></param>
        /// <param name="CountNumber"></param>
        /// <returns></returns>
        public static Object GetSpeciaRecord(String TableName, String ColumnName, String[] KeyColumnName, DatabaseMaster.CommandComparison[] Comparison, Object[] KeyValue, Boolean CountNumber)
        {

            //sql生成
            SelectDBCommandBuilder sql = new SelectDBCommandBuilder();
            sql.AddSelectTable(TableName);
            sql.AddSelectColumn(ColumnName);
            sql.SelectCount = true;

            for (int i = 0; i < KeyColumnName.Length; i++)
            {
                sql.AddWhere(WhereRelation.And, KeyColumnName[i], DatabaseMaster.CommandComparison.Equals, KeyValue[i]);
            }

            //数据库连接
            DatabaseInterface database = DBFactory.CreateDatabase(DatabaseInit.DefaultDatabase, DatabaseInit.ConnectName, DatabaseInit.EncryptType);
            database.Open();
            Object result = database.GetSpeciaRecordValue(sql.BuildCommand(), DatabaseInit.WaitTimeout);
            database.Close();

            return result;
        }

        /// <summary>
        /// 得到行计数数据
        /// </summary>
        /// <param name="TableName"></param>
        /// <param name="ColumnName"></param>
        /// <param name="CountNumber"></param>
        /// <returns></returns>
        public static Object GetSpeciaRecord(String TableName, String ColumnName, Boolean CountNumber)
        {

            //sql生成
            SelectDBCommandBuilder sql = new SelectDBCommandBuilder();
            sql.AddSelectTable(TableName);
            sql.AddSelectColumn(ColumnName);
            sql.SelectCount = true;

            //数据库连接
            DatabaseInterface database = DBFactory.CreateDatabase(DatabaseInit.DefaultDatabase, DatabaseInit.ConnectName, DatabaseInit.EncryptType);
            database.Open();
            Object result = database.GetSpeciaRecordValue(sql.BuildCommand(), DatabaseInit.WaitTimeout);
            database.Close();

            return result;
        }


        /// <summary>
        /// 得到指定数据的指定内容
        /// </summary>
        /// <param name="TableName"></param>
        /// <param name="ColumnName"></param>
        /// <param name="KeyColumnName"></param>
        /// <param name="KeyValue"></param>
        /// <param name="RecordNumber"></param>
        /// <returns></returns>
        public static DataTable GetTopRecordsData(String TableName, String ColumnName, String KeyColumnName, Object KeyValue, Int32 RecordNumber)
        {

            //sql生成
            SelectDBCommandBuilder sql = new SelectDBCommandBuilder();
            sql.AddSelectTable(TableName);
            sql.AddSelectColumn(ColumnName);
            sql.AddWhere(WhereRelation.None, KeyColumnName, DatabaseMaster.CommandComparison.Equals, KeyValue);
            sql.TopRecords = RecordNumber;

            //数据库连接
            DatabaseInterface database = DBFactory.CreateDatabase(DatabaseInit.DefaultDatabase, DatabaseInit.ConnectName, DatabaseInit.EncryptType);
            database.Open();
            DataTable dt = database.GetDataSet(sql.BuildCommand(), DatabaseInit.WaitTimeout).Tables[0];
            database.Close();

            return dt;

        }

        /// <summary>
        /// 得到指定数据的指定内容
        /// </summary>
        /// <param name="TableName"></param>
        /// <param name="ColumnName"></param>
        /// <param name="OrderByName"></param>
        /// <param name="RecordNumber"></param>
        /// <returns></returns>
        public static DataTable GetTopRecordsData(String TableName, String ColumnName, String OrderByName, Int32 RecordNumber)
        {

            //sql生成
            SelectDBCommandBuilder sql = new SelectDBCommandBuilder();
            sql.AddSelectTable(TableName);
            sql.AddSelectColumn(ColumnName);
            sql.AddOrderBy(OrderByName);
            sql.TopRecords = RecordNumber;

            //数据库连接
            DatabaseInterface database = DBFactory.CreateDatabase(DatabaseInit.DefaultDatabase, DatabaseInit.ConnectName, DatabaseInit.EncryptType);
            database.Open();
            DataTable dt = database.GetDataSet(sql.BuildCommand(), DatabaseInit.WaitTimeout).Tables[0];
            database.Close();

            return dt;

        }

        /// <summary>
        /// 得到指定数据的指定内容
        /// </summary>
        /// <param name="TableName"></param>
        /// <param name="ColumnName"></param>
        /// <param name="KeyColumnName"></param>
        /// <param name="KeyValue"></param>
        /// <param name="OrderByName"></param>
        /// <param name="RecordNumber"></param>
        /// <returns></returns>
        public static DataTable GetTopRecordsData(String TableName, String ColumnName, String[] KeyColumnName, Object[] KeyValue, String OrderByName, Int32 RecordNumber)
        {

            //sql生成
            SelectDBCommandBuilder sql = new SelectDBCommandBuilder();
            sql.AddSelectTable(TableName);
            sql.AddSelectColumn(ColumnName);
            sql.AddOrderBy(OrderByName);
            sql.TopRecords = RecordNumber;

            for (int i = 0; i < KeyColumnName.Length; i++)
            {
                sql.AddWhere(WhereRelation.And, KeyColumnName[i], DatabaseMaster.CommandComparison.Equals, KeyValue[i]);
            }

            //数据库连接
            DatabaseInterface database = DBFactory.CreateDatabase(DatabaseInit.DefaultDatabase, DatabaseInit.ConnectName, DatabaseInit.EncryptType);
            database.Open();
            DataTable dt = database.GetDataSet(sql.BuildCommand(), DatabaseInit.WaitTimeout).Tables[0];
            database.Close();

            return dt;

        }

        /// <summary>
        /// 得到指定数据的指定内容
        /// </summary>
        /// <param name="TableName"></param>
        /// <param name="ColumnName"></param>
        /// <param name="KeyColumnName"></param>
        /// <param name="Comparison"></param>
        /// <param name="KeyValue"></param>
        /// <param name="OrderByName"></param>
        /// <param name="RecordNumber"></param>
        /// <returns></returns>
        public static DataTable GetTopRecordsData(String TableName, String ColumnName, String[] KeyColumnName, DatabaseMaster.CommandComparison[] Comparison, Object[] KeyValue, String OrderByName, Int32 RecordNumber)
        {

            //sql生成
            SelectDBCommandBuilder sql = new SelectDBCommandBuilder();
            sql.AddSelectTable(TableName);
            sql.AddSelectColumn(ColumnName);
            sql.AddOrderBy(OrderByName);
            sql.TopRecords = RecordNumber;

            for (int i = 0; i < KeyColumnName.Length; i++)
            {
                sql.AddWhere(WhereRelation.And, KeyColumnName[i], (DatabaseMaster.CommandComparison)Comparison[i], KeyValue[i]);
            }

            //数据库连接
            DatabaseInterface database = DBFactory.CreateDatabase(DatabaseInit.DefaultDatabase, DatabaseInit.ConnectName, DatabaseInit.EncryptType);
            database.Open();
            DataTable dt = database.GetDataSet(sql.BuildCommand(), DatabaseInit.WaitTimeout).Tables[0];
            database.Close();

            return dt;

        }


        /// <summary>
        /// 得到指定数据的指定内容
        /// </summary>
        /// <param name="TableName"></param>
        /// <param name="KeyColumnName"></param>
        /// <param name="KeyValue"></param>
        /// <param name="RecordNumber"></param>
        /// <returns></returns>
        public static DataTable GetTopRecordsData(String TableName,  String[] KeyColumnName, Object[] KeyValue, Int32 RecordNumber)
        {

            //sql生成
            SelectDBCommandBuilder sql = new SelectDBCommandBuilder();
            sql.AddSelectTable(TableName);
            sql.AddSelectALLColumn();

            sql.TopRecords = RecordNumber;

            for (int i = 0; i < KeyColumnName.Length; i++)
            {
                sql.AddWhere(WhereRelation.And, KeyColumnName[i], DatabaseMaster.CommandComparison.Equals, KeyValue[i]);
            }

            //数据库连接
            DatabaseInterface database = DBFactory.CreateDatabase(DatabaseInit.DefaultDatabase, DatabaseInit.ConnectName, DatabaseInit.EncryptType);
            database.Open();
            DataTable dt = database.GetDataSet(sql.BuildCommand(), DatabaseInit.WaitTimeout).Tables[0];
            database.Close();

            return dt;

        }

        /// <summary>
        /// 得到指定数据的指定内容
        /// </summary>
        /// <param name="TableName"></param>
        /// <param name="KeyColumnName"></param>
        /// <param name="Comparison"></param>
        /// <param name="KeyValue"></param>
        /// <param name="RecordNumber"></param>
        /// <returns></returns>
        public static DataTable GetTopRecordsData(String TableName, String[] KeyColumnName, DatabaseMaster.CommandComparison[] Comparison, Object[] KeyValue, Int32 RecordNumber)
        {

            //sql生成
            SelectDBCommandBuilder sql = new SelectDBCommandBuilder();
            sql.AddSelectTable(TableName);
            sql.AddSelectALLColumn();

            sql.TopRecords = RecordNumber;

            for (int i = 0; i < KeyColumnName.Length; i++)
            {
                sql.AddWhere(WhereRelation.And, KeyColumnName[i], (DatabaseMaster.CommandComparison)Comparison[i], KeyValue[i]);
            }

            //数据库连接
            DatabaseInterface database = DBFactory.CreateDatabase(DatabaseInit.DefaultDatabase, DatabaseInit.ConnectName, DatabaseInit.EncryptType);
            database.Open();
            DataTable dt = database.GetDataSet(sql.BuildCommand(), DatabaseInit.WaitTimeout).Tables[0];
            database.Close();

            return dt;

        }



        /// <summary>
        /// 得到指定数据的指定内容
        /// </summary>
        /// <param name="TableName"></param>
        /// <param name="ColumnName"></param>
        /// <param name="KeyColumnName"></param>
        /// <param name="KeyValue"></param>
        /// <returns></returns>
        public static DataTable GetMaxRecordData(String TableName, String ColumnName, String KeyColumnName, Object KeyValue)
        {

            //sql生成
            SelectDBCommandBuilder sql = new SelectDBCommandBuilder();
            sql.AddSelectTable(TableName);
            sql.AddSelectColumn("max(" + ColumnName + ")");
            sql.AddWhere(WhereRelation.None, KeyColumnName, DatabaseMaster.CommandComparison.Equals, KeyValue);
            

            //数据库连接
            DatabaseInterface database = DBFactory.CreateDatabase(DatabaseInit.DefaultDatabase, DatabaseInit.ConnectName, DatabaseInit.EncryptType);
            database.Open();
            DataTable dt = database.GetDataSet(sql.BuildCommand(), DatabaseInit.WaitTimeout).Tables[0];
            database.Close();

            return dt;

        }

        /// <summary>
        /// 得到指定数据的指定内容
        /// </summary>
        /// <param name="TableName"></param>
        /// <param name="ColumnName"></param>
        /// <param name="KeyColumnName"></param>
        /// <param name="KeyValue"></param>
        /// <returns></returns>
        public static Int64 GetMaxRecordIntData(String TableName, String ColumnName, String KeyColumnName, Object KeyValue)
        {
            Int64 Value;

            //sql生成
            SelectDBCommandBuilder sql = new SelectDBCommandBuilder();
            sql.AddSelectTable(TableName);
            sql.AddSelectColumn("max(" + ColumnName + ")");
            sql.AddWhere(WhereRelation.None, KeyColumnName, DatabaseMaster.CommandComparison.Equals, KeyValue);


            //数据库连接
            DatabaseInterface database = DBFactory.CreateDatabase(DatabaseInit.DefaultDatabase, DatabaseInit.ConnectName, DatabaseInit.EncryptType);
            database.Open();
            Value =Convert.ToInt64( database.GetSpeciaRecordValue(sql.BuildCommand(), DatabaseInit.WaitTimeout));
            database.Close();

            return Value;

        }

        /// <summary>
        ///  得到表中一列数据
        /// </summary>
        /// <param name="TableName"></param>
        /// <param name="ColumnName"></param>
        /// <returns></returns>
        public static DataTable GetColumnDataTable(String TableName, List<String> ColumnName)
        {

            //sql生成
            SelectDBCommandBuilder sql = new SelectDBCommandBuilder();
            sql.AddSelectTable(TableName);
            sql.AddSelectColumn(ColumnName);


            //数据库连接
            DatabaseInterface database = DBFactory.CreateDatabase(DatabaseInit.DefaultDatabase, DatabaseInit.ConnectName, DatabaseInit.EncryptType);
            database.Open();
            DataSet ds = database.GetDataSet(sql.BuildCommand(), DatabaseInit.WaitTimeout);
            database.Close();

            return ds.Tables[0];
        }

       /// <summary>
       ///  得到表中一列数据
       /// </summary>
       /// <param name="TableName"></param>
       /// <param name="ColumnName"></param>
       /// <param name="OrderName"></param>
       /// <returns></returns>
        public static DataTable GetColumnDataTable(String TableName, List<String> ColumnName, String OrderName)
        {

            //sql生成
            SelectDBCommandBuilder sql = new SelectDBCommandBuilder();
            sql.AddSelectTable(TableName);
            sql.AddSelectColumn(ColumnName);
            sql.AddOrderBy(OrderName);


            //数据库连接
            DatabaseInterface database = DBFactory.CreateDatabase(DatabaseInit.DefaultDatabase, DatabaseInit.ConnectName, DatabaseInit.EncryptType);
            database.Open();
            DataSet ds = database.GetDataSet(sql.BuildCommand(), DatabaseInit.WaitTimeout);
            database.Close();

            return ds.Tables[0];
        }

        /// <summary>
        /// 得到表中一列满足条件的数据
        /// </summary>
        /// <param name="TableName"></param>
        /// <param name="ColumnName"></param>
        /// <param name="MatchColumn"></param>
        /// <param name="MatchValue"></param>
        /// <returns></returns>
        public static DataTable GetColumnDataTable(String TableName, List<String> ColumnName, String MatchColumn, Object MatchValue)
        {

            //sql生成
            SelectDBCommandBuilder sql = new SelectDBCommandBuilder();
            sql.AddSelectTable(TableName);
            sql.AddSelectColumn(ColumnName);
            sql.AddWhere(WhereRelation.None, MatchColumn, DatabaseMaster.CommandComparison.Equals, MatchValue);


            //数据库连接
            DatabaseInterface database = DBFactory.CreateDatabase(DatabaseInit.DefaultDatabase, DatabaseInit.ConnectName, DatabaseInit.EncryptType);
            database.Open();
            DataSet ds = database.GetDataSet(sql.BuildCommand(), DatabaseInit.WaitTimeout);
            database.Close();

            return ds.Tables[0];
        }

        /// <summary>
        /// 得到表中一列满足条件的数据
        /// </summary>
        /// <param name="TableName"></param>
        /// <param name="ColumnName"></param>
        /// <param name="MatchColumn"></param>
        /// <param name="Comparison"></param>
        /// <param name="MatchValue"></param>
        /// <returns></returns>
        public static DataTable GetColumnDataTable(String TableName, List<String> ColumnName, String MatchColumn, DatabaseMaster.CommandComparison Comparison ,Object MatchValue)
        {

            //sql生成
            SelectDBCommandBuilder sql = new SelectDBCommandBuilder();
            sql.AddSelectTable(TableName);
            sql.AddSelectColumn(ColumnName);
            sql.AddWhere(WhereRelation.None, MatchColumn, (DatabaseMaster.CommandComparison)Comparison, MatchValue);


            //数据库连接
            DatabaseInterface database = DBFactory.CreateDatabase(DatabaseInit.DefaultDatabase, DatabaseInit.ConnectName, DatabaseInit.EncryptType);
            database.Open();
            DataSet ds = database.GetDataSet(sql.BuildCommand(), DatabaseInit.WaitTimeout);
            database.Close();

            return ds.Tables[0];
        }

        /// <summary>
        /// 得到表中一列满足条件的数据
        /// </summary>
        /// <param name="TableName"></param>
        /// <param name="ColumnName"></param>
        /// <param name="MatchColumn"></param>
        /// <param name="Comparison"></param>
        /// <param name="MatchValue"></param>
        /// <returns></returns>
        public static DataTable GetColumnDataTable(String TableName, List<String> ColumnName, String MatchColumn, DatabaseMaster.CommandComparison Comparison, Object[] MatchValue)
        {

            //sql生成
            SelectDBCommandBuilder sql = new SelectDBCommandBuilder();
            sql.AddSelectTable(TableName);
            sql.AddSelectColumn(ColumnName);
            sql.AddWhere(WhereRelation.None, MatchColumn, (DatabaseMaster.CommandComparison)Comparison, MatchValue);


            //数据库连接
            DatabaseInterface database = DBFactory.CreateDatabase(DatabaseInit.DefaultDatabase, DatabaseInit.ConnectName, DatabaseInit.EncryptType);
            database.Open();
            DataSet ds = database.GetDataSet(sql.BuildCommand(), DatabaseInit.WaitTimeout);
            database.Close();

            return ds.Tables[0];
        }

        /// <summary>
        /// 得到表中一列满足条件的数据
        /// </summary>
        /// <param name="TableName"></param>
        /// <param name="ColumnName"></param>
        /// <param name="MatchColumn"></param>
        /// <param name="Comparison"></param>
        /// <param name="MatchValue"></param>
        /// <returns></returns>
        public static DataTable GetColumnDataTable(String TableName, List<String> ColumnName, String[] MatchColumn, DatabaseMaster.CommandComparison[] Comparison, Object[] MatchValue)
        {

            //sql生成
            SelectDBCommandBuilder sql = new SelectDBCommandBuilder();
            sql.AddSelectTable(TableName);
            sql.AddSelectColumn(ColumnName);

            for (int i = 0; i < MatchColumn.Length; i++)
            {
                sql.AddWhere(WhereRelation.And, MatchColumn[i], (DatabaseMaster.CommandComparison)Comparison[i], MatchValue[i]);
            }


            //数据库连接
            DatabaseInterface database = DBFactory.CreateDatabase(DatabaseInit.DefaultDatabase, DatabaseInit.ConnectName, DatabaseInit.EncryptType);
            database.Open();
            DataSet ds = database.GetDataSet(sql.BuildCommand(), DatabaseInit.WaitTimeout);
            database.Close();

            return ds.Tables[0];
        }

        /// <summary>
        /// 得到表中一列满足条件的数据
        /// </summary>
        /// <param name="TableName"></param>
        /// <param name="ColumnName"></param>
        /// <param name="MatchColumn"></param>
        /// <param name="Comparison"></param>
        /// <param name="MatchValue"></param>
        /// <param name="GroupByName"></param>
        /// <returns></returns>
        public static DataTable GetColumnDataTable(String TableName, List<String> ColumnName, String[] MatchColumn, DatabaseMaster.CommandComparison[] Comparison, Object[] MatchValue, String GroupByName)
        {

            //sql生成
            SelectDBCommandBuilder sql = new SelectDBCommandBuilder();
            sql.AddSelectTable(TableName);
            sql.AddSelectColumn(ColumnName);

            for (int i = 0; i < MatchColumn.Length; i++)
            {
                sql.AddWhere(WhereRelation.And, MatchColumn[i], (DatabaseMaster.CommandComparison)Comparison[i], MatchValue[i]);
            }

            sql.AddGroupBy(GroupByName);

            //数据库连接
            DatabaseInterface database = DBFactory.CreateDatabase(DatabaseInit.DefaultDatabase, DatabaseInit.ConnectName, DatabaseInit.EncryptType);
            database.Open();
            DataSet ds = database.GetDataSet(sql.BuildCommand(), DatabaseInit.WaitTimeout);
            database.Close();

            return ds.Tables[0];
        }
    }
}
