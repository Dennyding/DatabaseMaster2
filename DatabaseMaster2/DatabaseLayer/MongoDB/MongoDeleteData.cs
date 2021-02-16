using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data;
using System.Collections;
using MongoDB.Bson;
using MongoDB.Driver;

namespace DatabaseMaster2
{
    public class MongoDeleteData
    {
        private ConnectionConfig _connectionConfig;
        private MongoDBDatabase _database;
        private string _databasename;

        private String _TableName;
        private FilterDefinition<BsonDocument> filterDefinition;

        public MongoDeleteData(ConnectionConfig config, MongoDBDatabase database, String DatabaseName, String TableName)
        {
            _connectionConfig = config;
            _database = database;
            _databasename = DatabaseName;
            _TableName = TableName;
        }

        /// <summary>
        /// clear filter
        /// 清除过滤条件
        /// </summary>
        /// <returns></returns>
        public MongoDeleteData Clear()
        {
            filterDefinition = null;

            return this;
        }

        /// <summary>
        /// delete data by key value
        /// 删除指定条件数据
        /// </summary>
        /// <returns></returns>
        public int ExecuteCommand()
        {
            //数据库连接
            if (_connectionConfig.IsAutoCloseConnection == false)
                if (_database.CheckStatus() == false)
                    throw new Exception("databse connect not open");
            if (_connectionConfig.IsAutoCloseConnection == true) _database.Open();

            Int32 count = 0;

            if(filterDefinition != null)
                count = _database.DeleteData(_databasename, _TableName, filterDefinition);
            else
                count = _database.DeleteData(_databasename, _TableName);

            if (_connectionConfig.IsAutoCloseConnection == true) _database.Close();

            return count;
        }


        /// <summary>
        /// delete data by key value
        /// 删除指定条件数据
        /// </summary>
        /// <param name="KeyColumnName"></param>
        /// <param name="KeyValue"></param>
        /// <returns></returns>
        public MongoDeleteData Where(string KeyColumnName, object KeyValue)
        {
           filterDefinition= MongoDBOP.GetFilterOP(KeyColumnName, KeyValue, CommandComparison.Equals);

            return this;
        }

        /// <summary>
        /// delete data by key value
        /// 删除指定条件数据
        /// </summary>
        /// <param name="KeyColumnName"></param>
        /// <param name="KeyValue"></param>
        /// <returns></returns>
        public MongoDeleteData Where(string KeyColumnName, CommandComparison comparison,
            object KeyValue)
        {
            filterDefinition = MongoDBOP.GetFilterOP(KeyColumnName, KeyValue, comparison);

            return this;
        }

        /// <summary>
        /// delete data by many key value
        /// 删除多个指定条件数据
        /// </summary>
        /// <param name="KeyColumnName"></param>
        /// <param name="KeyValue"></param>
        /// <returns></returns>
        public MongoDeleteData Where(string[] KeyColumnName, object[] KeyValue)
        {
            if (KeyColumnName.Length != KeyValue.Length) throw new Exception("Column number not Equals Value number");

            filterDefinition = MongoDBOP.GetWhere(KeyColumnName, KeyValue);

            return this;
        }

        /// <summary>
        /// delete data by many key value
        /// 删除多个指定条件数据
        /// </summary>
        /// <param name="KeyColumnName"></param>
        /// <param name="KeyValue"></param>
        /// <returns></returns>
        public MongoDeleteData Where(string[] KeyColumnName, CommandComparison[] comparison,
            object[] KeyValue, WhereRelation[] relation)
        {
            if (KeyColumnName.Length != KeyValue.Length) throw new Exception("Column number not Equals Value number");

            filterDefinition = MongoDBOP.GetWhere(KeyColumnName, KeyValue, comparison, relation);

            return this;
        }


        /// <summary>
        /// delete table
        /// 删除表
        /// </summary>
        /// <returns></returns>
        public int Delete()
        {
            //数据库连接
            if (_connectionConfig.IsAutoCloseConnection == false)
                if (_database.CheckStatus() == false)
                    throw new Exception("databse connect not open");
            if (_connectionConfig.IsAutoCloseConnection == true) _database.Open();

            _database.DeleteTable(_databasename, _TableName);

            if (_connectionConfig.IsAutoCloseConnection == true) _database.Close();

            return 0;
        }
    }
}