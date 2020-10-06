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

        public DatabaseInsertData(ConnectionConfig config, DatabaseInterface database)
        {
            _connectionConfig = config;
            _database = database;
        }

        /// <summary>
        /// insert new data
        /// 插入新数据
        /// </summary>
        /// <param name="TableName"></param>
        /// <param name="ColumnName"></param>
        /// <param name="Value"></param>
        /// <returns></returns>
        public int Data(string TableName, string[] ColumnName, object[] Value)
        {
            //sql生成
            var sql = new InsertDBCommandBuilder();
            sql.TableName = TableName;
            sql.AddInsertColumn(ColumnName, Value);


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
        /// insert new data
        /// 插入新数据
        /// </summary>
        /// <param name="TableName"></param>
        /// <param name="ColumnName"></param>
        /// <param name="Value"></param>
        /// <returns></returns>
        public int Data(string TableName, object[] Value)
        {
            //sql生成
            var sql = new InsertDBCommandBuilder();
            sql.TableName = TableName;
            sql.AddInsertColumn(Value);


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
        /// insert new data
        /// 插入新数据
        /// </summary>
        /// <param name="TableName"></param>
        /// <param name="ColumnName"></param>
        /// <param name="Value"></param>
        /// <param name="ReturnID"></param>
        /// <returns></returns>
        public object Data(string TableName, string[] ColumnName, DatabaseType DatabaseModule, object[] Value,
            bool ReturnID)
        {
            //sql生成
            var sql = new InsertDBCommandBuilder();
            sql.TableName = TableName;
            sql.DatabaseModule = DatabaseModule;
            sql.SetSelectIdentity = true;
            sql.AddInsertColumn(ColumnName, Value);


            //数据库连接
            if (_connectionConfig.IsAutoCloseConnection == false)
                if (_database.CheckStatus() == false)
                    throw new Exception("databse connect not open");

            if (_connectionConfig.IsAutoCloseConnection == true) _database.Open();

            object result = _database.ExecueCommand(sql.BuildCommand(), _connectionConfig.WaitTimeout);
            if (_connectionConfig.IsAutoCloseConnection == true) _database.Close();

            return result;
        }

        /// <summary>
        /// insert new data
        /// 插入新数据
        /// </summary>
        /// <param name="TableName"></param>
        /// <param name="ColumnName"></param>
        /// <param name="Value"></param>
        /// <param name="ReturnID"></param>
        /// <returns></returns>
        public object Data(string TableName, DatabaseType DatabaseModule, object[] Value, bool ReturnID)
        {
            //sql生成
            var sql = new InsertDBCommandBuilder();
            sql.TableName = TableName;
            sql.DatabaseModule = DatabaseModule;
            sql.SetSelectIdentity = true;
            sql.AddInsertColumn(Value);


            //数据库连接
            if (_connectionConfig.IsAutoCloseConnection == false)
                if (_database.CheckStatus() == false)
                    throw new Exception("databse connect not open");

            if (_connectionConfig.IsAutoCloseConnection == true) _database.Open();

            object result = _database.ExecueCommand(sql.BuildCommand(), _connectionConfig.WaitTimeout);
            if (_connectionConfig.IsAutoCloseConnection == true) _database.Close();

            return result;
        }

        /// <summary>
        /// insert many new data
        /// 插入多个新数据
        /// </summary>
        /// <param name="TableName"></param>
        /// <param name="ColumnName"></param>
        /// <param name="Value"></param>
        /// <returns></returns>
        public int Data(string TableName, string[] ColumnName, List<object[]> Value)
        {
            var InsertBatch = "";


            //sql生成
            var sql = new InsertDBCommandBuilder();

            for (var i = 0; i < Value.Count; i++)
            {
                sql.ClearCommand();
                sql.TableName = TableName;
                sql.AddInsertColumn(ColumnName, Value[i]);

                InsertBatch += sql.BuildCommand() + ";";
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
    }
}