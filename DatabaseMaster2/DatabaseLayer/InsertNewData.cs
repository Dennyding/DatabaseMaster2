using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data;

namespace DatabaseMaster2
{
    public class DatabaseInsertData
    {
        private ConnectionConfig _connectionConfig;
        private DatabaseInterface _database;
        private InsertDBCommandBuilder sql = new InsertDBCommandBuilder();

        public DatabaseInsertData(ConnectionConfig config, DatabaseInterface database, String
            TableName)
        {
            _connectionConfig = config;
            _database = database;

            sql = new InsertDBCommandBuilder((DatabaseType)Enum.Parse(typeof(DatabaseType), config.DBType));

            if (!String.IsNullOrEmpty(TableName))
                sql.TableName = TableName;
        }

        /// <summary>
        /// clear filter
        /// 清除过滤条件
        /// </summary>
        /// <returns></returns>
        public DatabaseInsertData Clear()
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
        public Int32 ExecuteParameterCommand(ParameterINClass[] Parameter)
        {
            //数据库连接
            if (_connectionConfig.IsAutoCloseConnection == false)
                if (_database.CheckStatus() == false)
                    throw new Exception("databse connect not open");

            if (_connectionConfig.IsAutoCloseConnection == true) _database.Open();

            String s = sql.BuildCommand();
            var result = _database.ExecueCommand(sql.BuildCommand(), Parameter, _connectionConfig.WaitTimeout);
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
        /// insert many new data
        /// 插入多个新数据
        /// </summary>
        /// <param name="ColumnName"></param>
        /// <param name="Value"></param>
        /// <returns></returns>
        public DatabaseInsertData Data(string[] ColumnName, object[] Value)
        {
            sql.AddInsertColumn(ColumnName, Value);

            return this;
        }

        /// <summary>
        /// insert many new data
        /// 插入多个新数据
        /// </summary>
        /// <param name="Value"></param>
        /// <returns></returns>
        public DatabaseInsertData Data(object[] Value)
        {
            sql.AddInsertColumn(Value);

            return this;
        }

        /// <summary>
        /// insert many new data
        /// 插入多个新数据
        /// </summary>
        /// <param name="ColumnName"></param>
        /// <param name="DatabaseModule"></param>
        /// <param name="Value"></param>
        /// <param name="ReturnID"></param>
        /// <returns></returns>
        public DatabaseInsertData Data(string[] ColumnName, DatabaseType DatabaseModule, object[] Value,
            bool ReturnID)
        {
            sql.DatabaseModule = DatabaseModule;
            sql.SetSelectIdentity = true;
            sql.AddInsertColumn(ColumnName, Value);

            return this;
        }

        /// <summary>
        /// insert many new data
        /// 插入多个新数据
        /// </summary>
        /// <param name="DatabaseModule"></param>
        /// <param name="Value"></param>
        /// <param name="ReturnID"></param>
        /// <returns></returns>
        public DatabaseInsertData Data(DatabaseType DatabaseModule, object[] Value, bool ReturnID)
        {
            sql.DatabaseModule = DatabaseModule;
            sql.SetSelectIdentity = true;
            sql.AddInsertColumn(Value);

            return this;
        }

        /// <summary>
        /// insert many new data
        /// 插入多个新数据
        /// </summary>
        /// <param name="ColumnName"></param>
        /// <param name="Value"></param>
        /// <returns></returns>
        public Int32 ExecuteCommand(string[] ColumnName, List<object[]> Value)
        {
            var InsertBatch = "";

            for (var i = 0; i < Value.Count; i++)
            {
                InsertDBCommandBuilder sql1 = new InsertDBCommandBuilder();
                sql1.TableName = sql.TableName;
                sql1.AddInsertColumn(ColumnName, Value[i]);

                InsertBatch += sql1.BuildCommand() + ";";
            }

            //数据库连接
            if (_connectionConfig.IsAutoCloseConnection == false)
                if (_database.CheckStatus() == false)
                    throw new Exception("databse connect not open");

            if (_connectionConfig.IsAutoCloseConnection == true) _database.Open();

            var result = _database.ExecueCommand(InsertBatch, _connectionConfig.WaitTimeout);
            if (_connectionConfig.IsAutoCloseConnection == true) _database.Close();


            return result;
        }

        /// <summary>
        /// insert many new data
        /// 插入多个新数据
        /// </summary>
        /// <param name="ColumnName"></param>
        /// <param name="Value"></param>
        /// <returns></returns>
        public Int32 ExecueTransactionCommand(string[] ColumnName, List<object[]> Value)
        {
            var InsertBatch = "";

            for (var i = 0; i < Value.Count; i++)
            {
                InsertDBCommandBuilder sql1 = new InsertDBCommandBuilder();
                sql1.TableName = sql.TableName;
                sql1.AddInsertColumn(ColumnName, Value[i]);

                InsertBatch += sql1.BuildCommand() + ";";
            }

            //数据库连接
            if (_connectionConfig.IsAutoCloseConnection == false)
                if (_database.CheckStatus() == false)
                    throw new Exception("databse connect not open");

            if (_connectionConfig.IsAutoCloseConnection == true) _database.Open();

            var result = _database.ExecueTransactionCommand(new String[] { InsertBatch }, _connectionConfig.WaitTimeout);
            if (_connectionConfig.IsAutoCloseConnection == true) _database.Close();


            return result;
        }
    }
}