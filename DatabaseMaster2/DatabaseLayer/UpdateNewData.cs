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
        private UpdateDBCommandBuilder sql = new UpdateDBCommandBuilder();

        public DatabaseUpdateData(ConnectionConfig config, DatabaseInterface database, String
            TableName)
        {
            _connectionConfig = config;
            _database = database;

            sql = new UpdateDBCommandBuilder((DatabaseType)Enum.Parse(typeof(DatabaseType), config.DBType));

            if (!String.IsNullOrEmpty(TableName))
                sql.TableName = TableName;

        }

        /// <summary>
        /// clear filter
        /// 清除过滤条件
        /// </summary>
        /// <returns></returns>
        public DatabaseUpdateData Clear()
        {
            sql.ClearCommand();

            return this;
        }

        /// <summary>
        /// update new data
        /// 更新数据
        /// </summary>
        /// <returns></returns>
        public Int32 ExecuteCommand()
        {
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
        /// update new data
        /// 更新数据
        /// </summary>
        /// <returns></returns>
        public Int32 ExecuteCommand(ParameterINClass[] Parameter)
        {
            //数据库连接
            if (_connectionConfig.IsAutoCloseConnection == false)
                if (_database.CheckStatus() == false)
                    throw new Exception("databse connect not open");

            if (_connectionConfig.IsAutoCloseConnection == true) _database.Open();

            var result = _database.ExecueCommand(sql.BuildCommand(),Parameter, _connectionConfig.WaitTimeout);
            if (_connectionConfig.IsAutoCloseConnection == true) _database.Close();


            return result;
        }

        /// <summary>
        /// update new data
        /// 更新数据
        /// </summary>
        /// <returns></returns>
        public Boolean ExecuteCommandChanged()
        {
            return ExecuteCommand() > 0;
        }


        /// <summary>
        /// Execue Transaction
        /// 执行支持事务提交的多条指令
        /// </summary>
        /// <param name="TableName"></param>
        /// <param name="ColumnName"></param>
        /// <param name="Value"></param>
        /// <returns></returns>
        public Int32 ExecueTransactionCommand(string[] Command)
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
        /// update new data
        /// 更新数据内容
        /// </summary>
        /// <param name="ColumnName"></param>
        /// <param name="Value"></param>
        /// <returns></returns>
        public DatabaseUpdateData Data(string[] ColumnName, object[] Value)
        {
            sql.AddUpdateColumn(ColumnName, Value);

            return this;
        }

        /// <summary>
        /// update new data
        /// 更新数据条件
        /// </summary>
        /// <param name="KeyColumnName"></param>
        /// <param name="KeyValue"></param>
        /// <returns></returns>
        public DatabaseUpdateData Where(string KeyColumnName,object KeyValue)
        {
            sql.AddWhere(WhereRelation.And, KeyColumnName, CommandComparison.Equals, KeyValue);

           
            return this;
        }


        /// <summary>
        /// update new data
        /// 更新数据条件
        /// </summary>
        /// <param name="KeyColumnName"></param>
        /// <param name="KeyValue"></param>
        /// <returns></returns>
        public DatabaseUpdateData Where(string[] KeyColumnName, object[] KeyValue)
        {
            for (var i = 0; i < KeyColumnName.Length; i++)
                sql.AddWhere(WhereRelation.And, KeyColumnName[i], CommandComparison.Equals, KeyValue[i]);

            return this;
        }

        /// <summary>
        /// update new data
        /// 更新数据条件
        /// </summary>
        /// <param name="KeyColumnName"></param>
        /// <param name="comparison"></param>
        /// <param name="KeyValue"></param>
        /// <returns></returns>
        public DatabaseUpdateData Where(string[] KeyColumnName,CommandComparison[] comparison, object[] KeyValue)
        {
            for (var i = 0; i < KeyColumnName.Length; i++)
                sql.AddWhere(WhereRelation.And, KeyColumnName[i], comparison[i], KeyValue[i]);

            return this;
        }
    }
}