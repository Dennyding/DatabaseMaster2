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

        public MongoDeleteData(ConnectionConfig config, MongoDBDatabase database, String DatabaseName)
        {
            _connectionConfig = config;
            _database = database;
            _databasename = DatabaseName;
        }


        /// <summary>
        /// delete data by key value
        /// 删除指定条件数据
        /// </summary>
        /// <param name="TableName"></param>
        /// <param name="KeyColumnName"></param>
        /// <param name="KeyValue"></param>
        /// <returns></returns>
        public int Data(string TableName, string KeyColumnName, object KeyValue)
        {
            //数据库连接
            if (_connectionConfig.IsAutoCloseConnection == false)
                if (_database.CheckStatus() == false)
                    throw new Exception("databse connect not open");
            if (_connectionConfig.IsAutoCloseConnection == true) _database.Open();

            var count = _database.DeleteData(_databasename, TableName,
                MongoDBOP.GetFilterOP(KeyColumnName, KeyValue, CommandComparison.Equals));

            if (_connectionConfig.IsAutoCloseConnection == true) _database.Close();

            return count;
        }

        /// <summary>
        /// delete data by key value
        /// 删除指定条件数据
        /// </summary>
        /// <param name="TableName"></param>
        /// <param name="KeyColumnName"></param>
        /// <param name="KeyValue"></param>
        /// <returns></returns>
        public int Data(string TableName, string KeyColumnName, CommandComparison comparison,
            object KeyValue)
        {
            //数据库连接
            if (_connectionConfig.IsAutoCloseConnection == false)
                if (_database.CheckStatus() == false)
                    throw new Exception("databse connect not open");
            if (_connectionConfig.IsAutoCloseConnection == true) _database.Open();

            var count = _database.DeleteData(_databasename, TableName,
                MongoDBOP.GetFilterOP(KeyColumnName, KeyValue, comparison));

            if (_connectionConfig.IsAutoCloseConnection == true) _database.Close();

            return count;
        }

        /// <summary>
        /// delete data by many key value
        /// 删除多个指定条件数据
        /// </summary>
        /// <param name="TableName"></param>
        /// <param name="KeyColumnName"></param>
        /// <param name="KeyValue"></param>
        /// <returns></returns>
        public int Data(string TableName, string[] KeyColumnName, object[] KeyValue)
        {
            if (KeyColumnName.Length != KeyValue.Length) throw new Exception("Column number not Equals Value number");

            //数据库连接
            if (_connectionConfig.IsAutoCloseConnection == false)
                if (_database.CheckStatus() == false)
                    throw new Exception("databse connect not open");
            if (_connectionConfig.IsAutoCloseConnection == true) _database.Open();

            var count = _database.DeleteData(_databasename, TableName,
                MongoDBOP.GetWhere(KeyColumnName, KeyValue));

            if (_connectionConfig.IsAutoCloseConnection == true) _database.Close();

            return count;
        }

        /// <summary>
        /// delete data by many key value
        /// 删除多个指定条件数据
        /// </summary>
        /// <param name="TableName"></param>
        /// <param name="KeyColumnName"></param>
        /// <param name="KeyValue"></param>
        /// <returns></returns>
        public int Data(string TableName, string[] KeyColumnName, CommandComparison[] comparison,
            object[] KeyValue, WhereRelation[] relation)
        {
            if (KeyColumnName.Length != KeyValue.Length) throw new Exception("Column number not Equals Value number");

            //数据库连接
            if (_connectionConfig.IsAutoCloseConnection == false)
                if (_database.CheckStatus() == false)
                    throw new Exception("databse connect not open");
            if (_connectionConfig.IsAutoCloseConnection == true) _database.Open();

            var count = _database.DeleteData(_databasename, TableName,
                MongoDBOP.GetWhere(KeyColumnName, KeyValue, comparison, relation));

            if (_connectionConfig.IsAutoCloseConnection == true) _database.Close();

            return count;
        }

        /// <summary>
        /// delete all data 
        /// 删除所有数据
        /// </summary>
        /// <param name="TableName"></param>
        /// <param name="KeyColumnName"></param>
        /// <param name="KeyValue"></param>
        /// <returns></returns>
        public int DataAll(string TableName, bool All)
        {
            if (All == false) return 0;

            //数据库连接
            if (_connectionConfig.IsAutoCloseConnection == false)
                if (_database.CheckStatus() == false)
                    throw new Exception("databse connect not open");
            if (_connectionConfig.IsAutoCloseConnection == true) _database.Open();

            var count = _database.DeleteData(_databasename, TableName);

            if (_connectionConfig.IsAutoCloseConnection == true) _database.Close();

            return count;
        }

        /// <summary>
        /// delete table
        /// 删除表
        /// </summary>
        /// <param name="TableName"></param>
        /// <param name="KeyColumnName"></param>
        /// <param name="KeyValue"></param>
        /// <returns></returns>
        public int Table(string TableName)
        {
            //数据库连接
            if (_connectionConfig.IsAutoCloseConnection == false)
                if (_database.CheckStatus() == false)
                    throw new Exception("databse connect not open");
            if (_connectionConfig.IsAutoCloseConnection == true) _database.Open();

            _database.DeleteTable(_databasename, TableName);

            if (_connectionConfig.IsAutoCloseConnection == true) _database.Close();

            return 0;
        }
    }
}