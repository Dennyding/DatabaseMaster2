using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data;

namespace DatabaseMaster2
{
    public class DatabaseDeleteData
    {
        private ConnectionConfig _connectionConfig;
        private DatabaseInterface _database;

        public DatabaseDeleteData(ConnectionConfig config, DatabaseInterface database)
        {
            _connectionConfig = config;
            _database = database;
        }

        /// <summary>
        /// delete data by  key value
        /// 删除指定条件数据
        /// </summary>
        /// <param name="TableName"></param>
        /// <param name="ColumnName"></param>
        /// <param name="Value"></param>
        /// <returns></returns>
        public int Data(string TableName, string KeyColumnName, object KeyValue)
        {
            //sql生成
            var sql = new DeleteDBCommandBuilder();
            sql.TableName = TableName;
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
        /// delete data by many key value
        /// 删除多个指定条件数据
        /// </summary>
        /// <param name="TableName"></param>
        /// <param name="ColumnName"></param>
        /// <param name="Value"></param>
        /// <returns></returns>
        public int Data(string TableName, string[] KeyColumnName, object[] KeyValue)
        {
            //sql生成
            var sql = new DeleteDBCommandBuilder();
            sql.TableName = TableName;

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
        /// delete data by many key value
        /// 删除多个指定条件数据
        /// </summary>
        /// <param name="TableName"></param>
        /// <param name="ColumnName"></param>
        /// <param name="Value"></param>
        /// <returns></returns>
        public int Data(string TableName, string[] KeyColumnName, CommandComparison[] comparison,
            object[] KeyValue)
        {
            //sql生成
            var sql = new DeleteDBCommandBuilder();
            sql.TableName = TableName;

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