using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using DatabaseMaster;
using System.Data;

namespace DatabaseLayer
{
    public class UpdateNewDataToDatabase
    {
     

        /// <summary>
        /// 更新表中新数据
        /// </summary>
        /// <param name="TableName"></param>
        /// <param name="ColumnName"></param>
        /// <param name="Value"></param>
        /// <returns></returns>
        public static int UpdateNewDataToTable(String TableName, String[] ColumnName, Object[] Value, String KeyColumnName, Object KeyValue)
        {

            //sql生成
            UpdateDBCommandBuilder sql = new UpdateDBCommandBuilder();
            sql.TableName = TableName;
            sql.AddUpdateColumn(ColumnName, Value);
            sql.AddWhere(WhereRelation.None, KeyColumnName, DatabaseMaster.CommandComparison.Equals, KeyValue);

            //数据库连接
            DatabaseInterface database = DBFactory.CreateDatabase(DatabaseInit.DefaultDatabase, DatabaseInit.ConnectName, DatabaseInit.EncryptType);
            database.Open();
            int result = database.ExecueCommand(sql.BuildCommand(), DatabaseInit.WaitTimeout);
            database.Close();

            return result;
        }

        /// <summary>
        /// 执行支持事务提交的多条指令
        /// </summary>
        /// <param name="TableName"></param>
        /// <param name="ColumnName"></param>
        /// <param name="Value"></param>
        /// <returns></returns>
        public static int ExecueTransactionCommand(String[] Command)
        {

            //数据库连接
            DatabaseInterface database = DBFactory.CreateDatabase(DatabaseInit.DefaultDatabase, DatabaseInit.ConnectName, DatabaseInit.EncryptType);
            database.Open();
            int result = database.ExecueTransactionCommand(Command, DatabaseInit.WaitTimeout);
            database.Close();

            return result;
        }

        /// <summary>
        /// 更新表中新数据
        /// </summary>
        /// <param name="TableName"></param>
        /// <param name="ColumnName"></param>
        /// <param name="Value"></param>
        /// <returns></returns>
        public static int UpdateNewDataToTable(String TableName, String[] ColumnName, Object[] Value, String[] KeyColumnName, Object[] KeyValue)
        {

            //sql生成
            UpdateDBCommandBuilder sql = new UpdateDBCommandBuilder();
            sql.TableName = TableName;
            sql.AddUpdateColumn(ColumnName, Value);

            for (int i = 0; i < KeyColumnName.Length; i++)
            {
                sql.AddWhere(WhereRelation.And, KeyColumnName[i], DatabaseMaster.CommandComparison.Equals, KeyValue[i]);
            }


            //数据库连接
            DatabaseInterface database = DBFactory.CreateDatabase(DatabaseInit.DefaultDatabase, DatabaseInit.ConnectName, DatabaseInit.EncryptType);
            database.Open();
            int result = database.ExecueCommand(sql.BuildCommand(), DatabaseInit.WaitTimeout);
            database.Close();

            return result;
        }

        /// <summary>
        /// 更新表中新数据
        /// </summary>
        /// <param name="TableName"></param>
        /// <param name="ColumnName"></param>
        /// <param name="Value"></param>
        /// <returns></returns>
        public static int UpdateNewDataToTable(String TableName, String[] ColumnName, Object[] Value, String[] KeyColumnName, DatabaseMaster.CommandComparison[] comparison, Object[] KeyValue)
        {

            //sql生成
            UpdateDBCommandBuilder sql = new UpdateDBCommandBuilder();
            sql.TableName = TableName;
            sql.AddUpdateColumn(ColumnName, Value);

            for (int i = 0; i < KeyColumnName.Length; i++)
            {
                sql.AddWhere(WhereRelation.And, KeyColumnName[i], (DatabaseMaster.CommandComparison)comparison[i], KeyValue[i]);
            }


            //数据库连接
            DatabaseInterface database = DBFactory.CreateDatabase(DatabaseInit.DefaultDatabase, DatabaseInit.ConnectName, DatabaseInit.EncryptType);
            database.Open();
            int result = database.ExecueCommand(sql.BuildCommand(), DatabaseInit.WaitTimeout);
            database.Close();

            return result;
        }
    }
}
