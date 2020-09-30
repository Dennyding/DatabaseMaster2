using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using DatabaseMaster;
using System.Data;

namespace DatabaseLayer
{
    public class InsertNewDataToDatabase
    {
      
        /// <summary>
        /// 插入表中新数据
        /// </summary>
        /// <param name="TableName"></param>
        /// <param name="ColumnName"></param>
        /// <param name="Value"></param>
        /// <returns></returns>
        public static int InsertNewDataToTable(String TableName, String[] ColumnName, Object[] Value)
        {

            //sql生成
            InsertDBCommandBuilder sql = new InsertDBCommandBuilder();
            sql.TableName = TableName;
            sql.AddInsertColumn(ColumnName, Value);


            //数据库连接
            DatabaseInterface database = DBFactory.CreateDatabase(DatabaseInit.DefaultDatabase, DatabaseInit.ConnectName, DatabaseInit.EncryptType);
            database.Open();
            int result = database.ExecueCommand(sql.BuildCommand(), DatabaseInit.WaitTimeout);
            database.Close();

            return result;
        }

        /// <summary>
        /// 插入表中新数据
        /// </summary>
        /// <param name="TableName"></param>
        /// <param name="ColumnName"></param>
        /// <param name="Value"></param>
        /// <returns></returns>
        public static int InsertNewDataToTable(String TableName, Object[] Value)
        {

            //sql生成
            InsertDBCommandBuilder sql = new InsertDBCommandBuilder();
            sql.TableName = TableName;
            sql.AddInsertColumn(Value);


            //数据库连接
            DatabaseInterface database = DBFactory.CreateDatabase(DatabaseInit.DefaultDatabase, DatabaseInit.ConnectName, DatabaseInit.EncryptType);
            database.Open();
            int result = database.ExecueCommand(sql.BuildCommand(), DatabaseInit.WaitTimeout);
            database.Close();

            return result;
        }

        /// <summary>
        /// 插入表中新数据并返回主键
        /// </summary>
        /// <param name="TableName"></param>
        /// <param name="ColumnName"></param>
        /// <param name="Value"></param>
        /// <param name="ReturnID"></param>
        /// <returns></returns>
        public static Object InsertNewDataToTable(String TableName, String[] ColumnName, String DatabaseModule,  Object[] Value, Boolean ReturnID)
        {

            //sql生成
            InsertDBCommandBuilder sql = new InsertDBCommandBuilder();
            sql.TableName = TableName;
            sql.DatabaseModule = DatabaseModule;
            sql.SetSelectIdentity = true;
            sql.AddInsertColumn(ColumnName, Value);


            //数据库连接
            DatabaseInterface database = DBFactory.CreateDatabase(DatabaseInit.DefaultDatabase, DatabaseInit.ConnectName, DatabaseInit.EncryptType);
            database.Open();
            Object result = database.GetSpeciaRecordValue(sql.BuildCommand(), DatabaseInit.WaitTimeout);
            database.Close();

            return result;
        }

        /// <summary>
        /// 插入表中新数据并返回主键
        /// </summary>
        /// <param name="TableName"></param>
        /// <param name="ColumnName"></param>
        /// <param name="Value"></param>
        /// <param name="ReturnID"></param>
        /// <returns></returns>
        public static Object InsertNewDataToTable(String TableName, String DatabaseModule, Object[] Value, Boolean ReturnID)
        {

            //sql生成
            InsertDBCommandBuilder sql = new InsertDBCommandBuilder();
            sql.TableName = TableName;
            sql.DatabaseModule = DatabaseModule;
            sql.SetSelectIdentity = true;
            sql.AddInsertColumn(Value);


            //数据库连接
            DatabaseInterface database = DBFactory.CreateDatabase(DatabaseInit.DefaultDatabase, DatabaseInit.ConnectName, DatabaseInit.EncryptType);
            database.Open();
            Object result = database.GetSpeciaRecordValue(sql.BuildCommand(), DatabaseInit.WaitTimeout);
            database.Close();

            return result;
        }

        /// <summary>
        /// 插入表中新数据
        /// </summary>
        /// <param name="TableName"></param>
        /// <param name="ColumnName"></param>
        /// <param name="Value"></param>
        /// <returns></returns>
        public static int InsertNewDataToTable(String TableName, String[] ColumnName, List<Object[]> Value)
        {
            String InsertBatch="";


            //sql生成
            InsertDBCommandBuilder sql = new InsertDBCommandBuilder();

            for (int i = 0; i < Value.Count; i++)
            {
                sql.ClearCommand();
                sql.TableName = TableName;
                sql.AddInsertColumn(ColumnName, Value[i]);

                InsertBatch += sql.BuildCommand() + ";";
            }


            //数据库连接
            DatabaseInterface database = DBFactory.CreateDatabase(DatabaseInit.DefaultDatabase, DatabaseInit.ConnectName, DatabaseInit.EncryptType);
            database.Open();
            int result = database.ExecueCommand(InsertBatch, DatabaseInit.WaitTimeout);
            database.Close();

            return result;
        }
    }
}
