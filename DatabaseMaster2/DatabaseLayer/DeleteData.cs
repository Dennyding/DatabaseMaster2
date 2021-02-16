using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data;
using System.Threading.Tasks;

namespace DatabaseMaster2
{
    public class DatabaseDeleteData
    {
        private ConnectionConfig _connectionConfig;
        private DatabaseInterface _database;
        private DeleteDBCommandBuilder sql = new DeleteDBCommandBuilder();

        public DatabaseDeleteData(ConnectionConfig config, DatabaseInterface database, String
            TableName)
        {
            _connectionConfig = config;
            _database = database;
            if (!String.IsNullOrEmpty(TableName))
                sql.TableName=TableName;
        }

        /// <summary>
        /// clear filter
        /// 清除过滤条件
        /// </summary>
        /// <returns></returns>
        public DatabaseDeleteData Clear()
        {
            sql.ClearCommand();

            return this;
        }

        /// <summary>
        /// delete data by key value
        /// 删除指定条件数据
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
        /// delete data by key value
        /// 删除指定条件数据
        /// </summary>
        /// <returns></returns>
        public Boolean ExecuteCommandChanged()
        {
           return ExecuteCommand()>0;
        }

       
        /// <summary>
        /// data by key value
        /// 指定条件数据
        /// </summary>
        /// <param name="ColumnName"></param>
        /// <param name="Value"></param>
        /// <returns></returns>
        public DatabaseDeleteData Where(string KeyColumnName, object KeyValue)
        {
            sql.AddWhere(WhereRelation.And, KeyColumnName, CommandComparison.Equals, KeyValue);

            return this;
        }

        /// <summary>
        /// data by key value
        /// 指定条件数据
        /// </summary>
        /// <param name="ColumnName"></param>
        /// <param name="Value"></param>
        /// <returns></returns>
        public DatabaseDeleteData Where(string[] KeyColumnName, object[] KeyValue)
        {
            for (var i = 0; i < KeyColumnName.Length; i++)
                sql.AddWhere(WhereRelation.And, KeyColumnName[i], CommandComparison.Equals, KeyValue[i]);

            return this;
        }

        /// <summary>
        /// data by key value
        /// 指定条件数据
        /// </summary>
        /// <param name="ColumnName"></param>
        /// <param name="Value"></param>
        /// <returns></returns>
        public DatabaseDeleteData Where(string[] KeyColumnName, CommandComparison[] comparison,
            object[] KeyValue)
        {
            for (var i = 0; i < KeyColumnName.Length; i++)
                sql.AddWhere(WhereRelation.And, KeyColumnName[i], (CommandComparison) comparison[i], KeyValue[i]);

            return this;
        }
    }
}