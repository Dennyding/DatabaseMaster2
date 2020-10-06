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
    public class MongoInsertData
    {
        private ConnectionConfig _connectionConfig;
        private MongoDBDatabase _database;
        private string _databasename;

        public MongoInsertData(ConnectionConfig config, MongoDBDatabase database, String DatabaseName)
        {
            _connectionConfig = config;
            _database = database;
            _databasename = DatabaseName;
        }


        /// <summary>
        /// insert new data
        /// 插入新数据
        /// </summary>
        /// <param name="TableName"></param>
        /// <param name="ColumnName"></param>
        /// <param name="Value"></param>
        public void Data(String TableName, String[] ColumnName, Object[] Value)
        {
            if (ColumnName.Length != Value.Length)
            {
                throw new Exception("Column number not Equals Value number");
            }

            Hashtable ht = new Hashtable();

            for (int i = 0; i < ColumnName.Length; i++)
            {
                ht.Add(ColumnName[i], Value[i]);
            }

            //数据库连接
            if (_connectionConfig.IsAutoCloseConnection == false)
                if (_database.CheckStatus() == false)
                    throw new Exception("databse connect not open");
            if (_connectionConfig.IsAutoCloseConnection == true) _database.Open();
            _database.InsertData(_databasename, TableName, MongoDB.Bson.Serialization.BsonSerializer.Deserialize<BsonDocument>(MongoDBOP.HashtableToJson(ht)));

            if (_connectionConfig.IsAutoCloseConnection == true) _database.Close();

            return;
        }


        /// <summary>
        /// insert new data
        /// 插入新数据
        /// </summary>
        /// <param name="TableName"></param>
        /// <param name="ColumnName"></param>
        /// <param name="Value"></param>
        public void Data(String TableName, Hashtable ht)
        {


            if (ht.Count == 0)
            {
                throw new Exception("Column number not Equals Value number");
            }

            //数据库连接
            if (_connectionConfig.IsAutoCloseConnection == false)
                if (_database.CheckStatus() == false)
                    throw new Exception("databse connect not open");
            if (_connectionConfig.IsAutoCloseConnection == true) _database.Open();
            _database.InsertData(_databasename, TableName, MongoDB.Bson.Serialization.BsonSerializer.Deserialize<BsonDocument>(MongoDBOP.HashtableToJson(ht)));

            if (_connectionConfig.IsAutoCloseConnection == true) _database.Close();

            return;
        }

        /// <summary>
        /// insert new data and return key
        /// 插入数据并返回唯一字符串主键
        /// </summary>
        /// <param name="TableName"></param>
        /// <param name="ColumnName"></param>
        /// <param name="Value"></param>
        /// <param name="KeyColumnName"></param>
        /// <param name="GUIDValue"></param>
        /// <returns></returns>
        public String Data(String TableName, String[] ColumnName, Object[] Value, String KeyColumnName, String GUIDValue)
        {


            if (ColumnName.Length != Value.Length)
            {
                throw new Exception("Column number not Equals Value number");
            }

            Hashtable ht = new Hashtable();

            for (int i = 0; i < ColumnName.Length; i++)
            {
                ht.Add(ColumnName[i], Value[i]);
            }

            //数据库连接
            if (_connectionConfig.IsAutoCloseConnection == false)
                if (_database.CheckStatus() == false)
                    throw new Exception("databse connect not open");
            if (_connectionConfig.IsAutoCloseConnection == true) _database.Open();
            String s = _database.InsertData(_databasename, TableName, MongoDB.Bson.Serialization.BsonSerializer.Deserialize<BsonDocument>(MongoDBOP.HashtableToJson(ht))
                , KeyColumnName, GUIDValue);

            if (_connectionConfig.IsAutoCloseConnection == true) _database.Close();

            return s;
        }


        /// <summary>
        /// insert new data and return key
        /// 插入数据并返回唯一字符串主键
        /// </summary>
        /// <param name="TableName"></param>
        /// <param name="ht"></param>
        /// <param name="KeyColumnName"></param>
        /// <param name="GUIDValue"></param>
        /// <returns></returns>
        public String Data(String TableName, Hashtable ht, String KeyColumnName, String GUIDValue)
        {


            if (ht.Count == 0)
            {
                throw new Exception("Column number not Equals Value number");
            }

            //数据库连接
            if (_connectionConfig.IsAutoCloseConnection == false)
                if (_database.CheckStatus() == false)
                    throw new Exception("databse connect not open");
            if (_connectionConfig.IsAutoCloseConnection == true) _database.Open();
            String s = _database.InsertData(_databasename, TableName, MongoDB.Bson.Serialization.BsonSerializer.Deserialize<BsonDocument>(MongoDBOP.HashtableToJson(ht))
                 , KeyColumnName, GUIDValue);

            if (_connectionConfig.IsAutoCloseConnection == true) _database.Close();

            return s;
        }

        /// <summary>
        /// insert many new data
        /// 插入多个新数据
        /// </summary>
        /// <param name="TableName"></param>
        /// <param name="ColumnName"></param>
        /// <param name="Value"></param>
        public void Data(String TableName, List<Hashtable> ht)
        {


            if (ht.Count == 0)
            {
                throw new Exception("Column number not Equals Value number");
            }

            BsonDocument[] document = new BsonDocument[ht.Count];
            for (int i = 0; i < ht.Count; i++)
            {
                document[i] = MongoDB.Bson.Serialization.BsonSerializer.Deserialize<BsonDocument>(MongoDBOP.HashtableToJson(ht[i]));
            }
            //数据库连接
            if (_connectionConfig.IsAutoCloseConnection == false)
                if (_database.CheckStatus() == false)
                    throw new Exception("databse connect not open");
            if (_connectionConfig.IsAutoCloseConnection == true) _database.Open();
            _database.InsertData(_databasename, TableName, document);

            if (_connectionConfig.IsAutoCloseConnection == true) _database.Close();

            return;
        }
    }


}
