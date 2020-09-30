using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data;

namespace DatabaseMaster2
{
    public class DatabaseUpdateData
    {
        private ConnectionConfig _connectionConfig;
        private DatabaseInterface _database;

        public DatabaseUpdateData(ConnectionConfig config, DatabaseInterface database)
        {
            _connectionConfig = config;
            _database = database;
        }

        /// <summary>
        /// update new data
        /// 更新数据
        /// </summary>
        /// <param name="TableName"></param>
        /// <param name="ColumnName"></param>
        /// <param name="Value"></param>
        /// <returns></returns>
        public int Data(string TableName, string[] ColumnName, object[] Value, string KeyColumnName,
            object KeyValue)
        {
            //sql生成
            var sql = new UpdateDBCommandBuilder();
            sql.TableName = TableName;
            sql.AddUpdateColumn(ColumnName, Value);
            sql.AddWhere(WhereRelation.None, KeyColumnName, CommandComparison.Equals, KeyValue);

            //数据库连接
            if (_connectionConfig.IsAutoCloseConnection == false)
                if (_database.CheckStatus() == false)
                    throw new Exception("databse connect not open");

            if (_connectionConfig.IsAutoCloseConnection == true) _database.Open();

            var result = _database.ExecueCommand(sql.BuildCommand(), _connectionConfig.WaitTimeout);
            if (_connectionConfig.IsAutoCloseConnection == true) _database.Close();

            return result;
        }

        /// <summary>
        /// Execue Transaction
        /// 执行支持事务提交的多条指令
        /// </summary>
        /// <param name="TableName"></param>
        /// <param name="ColumnName"></param>
        /// <param name="Value"></param>
        /// <returns></returns>
        public int ExecueTransactionCommand(string[] Command)
        {
            //数据库连接
            if (_connectionConfig.IsAutoCloseConnection == false)
                if (_database.CheckStatus() == false)
                    throw new Exception("databse connect not open");

            if (_connectionConfig.IsAutoCloseConnection == true) _database.Open();

            var result = _database.ExecueTransactionCommand(Command, _connectionConfig.WaitTimeout);
            if (_connectionConfig.IsAutoCloseConnection == true) _database.Close();

            return result;
        }

        /// <summary>
        /// update many new data
        /// 更新多个数据
        /// </summary>
        /// <param name="TableName"></param>
        /// <param name="ColumnName"></param>
        /// <param name="Value"></param>
        /// <returns></returns>
        public int Data(string TableName, string[] ColumnName, object[] Value, string[] KeyColumnName,
            object[] KeyValue)
        {
            //sql生成
            var sql = new UpdateDBCommandBuilder();
            sql.TableName = TableName;
            sql.AddUpdateColumn(ColumnName, Value);

            for (var i = 0; i < KeyColumnName.Length; i++)
                sql.AddWhere(WhereRelation.And, KeyColumnName[i], CommandComparison.Equals, KeyValue[i]);


            //数据库连接
            if (_connectionConfig.IsAutoCloseConnection == false)
                if (_database.CheckStatus() == false)
                    throw new Exception("databse connect not open");

            if (_connectionConfig.IsAutoCloseConnection == true) _database.Open();

            var result = _database.ExecueCommand(sql.BuildCommand(), _connectionConfig.WaitTimeout);
            if (_connectionConfig.IsAutoCloseConnection == true) _database.Close();

            return result;
        }

        /// <summary>
        /// update many new data
        /// 更新多个数据
        /// </summary>
        /// <param name="TableName"></param>
        /// <param name="ColumnName"></param>
        /// <param name="Value"></param>
        /// <returns></returns>
        public int Data(string TableName, string[] ColumnName, object[] Value, string[] KeyColumnName,
            CommandComparison[] comparison, object[] KeyValue)
        {
            //sql生成
            var sql = new UpdateDBCommandBuilder();
            sql.TableName = TableName;
            sql.AddUpdateColumn(ColumnName, Value);

            for (var i = 0; i < KeyColumnName.Length; i++)
                sql.AddWhere(WhereRelation.And, KeyColumnName[i], (CommandComparison) comparison[i], KeyValue[i]);

            //数据库连接
            if (_connectionConfig.IsAutoCloseConnection == false)
                if (_database.CheckStatus() == false)
                    throw new Exception("databse connect not open");

            if (_connectionConfig.IsAutoCloseConnection == true) _database.Open();

            var result = _database.ExecueCommand(sql.BuildCommand(), _connectionConfig.WaitTimeout);
            if (_connectionConfig.IsAutoCloseConnection == true) _database.Close();

            return result;
        }
    }
}