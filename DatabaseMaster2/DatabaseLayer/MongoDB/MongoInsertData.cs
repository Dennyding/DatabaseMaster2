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

        private String _TableName;
        Hashtable htData = new Hashtable();

        public MongoInsertData(ConnectionConfig config, MongoDBDatabase database, String DatabaseName, String TableName)
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
        public MongoInsertData Clear()
        {
            htData.Clear();

            return this;
        }

        /// <summary>
        /// insert new data
        /// 插入新数据
        /// </summary>
        public void ExecuteCommand()
        {
          
            //数据库连接
            if (_connectionConfig.IsAutoCloseConnection == false)
                if (_database.CheckStatus() == false)
                    throw new Exception("databse connect not open");
            if (_connectionConfig.IsAutoCloseConnection == true) _database.Open();
            _database.InsertData(_databasename, _TableName, MongoDB.Bson.Serialization.BsonSerializer.Deserialize<BsonDocument>(MongoDBOP.HashtableToJson(htData)));

            if (_connectionConfig.IsAutoCloseConnection == true) _database.Close();

            return;
        }

        /// <summary>
        /// insert new data
        /// 插入新数据
        /// </summary>
        public String ExecuteCommand(String KeyColumnName, String GUIDValue)
        {

            //数据库连接
            if (_connectionConfig.IsAutoCloseConnection == false)
                if (_database.CheckStatus() == false)
                    throw new Exception("databse connect not open");
            if (_connectionConfig.IsAutoCloseConnection == true) _database.Open();
            String s = _database.InsertData(_databasename, _TableName, MongoDB.Bson.Serialization.BsonSerializer.Deserialize<BsonDocument>(MongoDBOP.HashtableToJson(htData))
                , KeyColumnName, GUIDValue);

            if (_connectionConfig.IsAutoCloseConnection == true) _database.Close();

            return s;
        }

        /// <summary>
        /// insert new data
        /// 插入新数据
        /// </summary>
        /// <param name="ColumnName"></param>
        /// <param name="Value"></param>
        public MongoInsertData Data(String[] ColumnName, Object[] Value)
        {
            if (ColumnName.Length != Value.Length)
            {
                throw new Exception("Column number not Equals Value number");
            }

            for (int i = 0; i < ColumnName.Length; i++)
            {
                htData.Add(ColumnName[i], Value[i]);
            }

            return this;
        }


        /// <summary>
        /// insert new data
        /// 插入新数据
        /// </summary>       
        /// <param name="ColumnName"></param>
        /// <param name="Value"></param>
        public MongoInsertData Data(Hashtable ht)
        {

            if (ht.Count == 0)
            {
                throw new Exception("Column number not Equals Value number");
            }

            htData = ht;

            return this;
        }




        /// <summary>
        /// insert many new data
        /// 插入多个新数据
        /// </summary>
        /// <param name="ht"></param>
        public void ExecuteCommand(List<Hashtable> ht)
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
            _database.InsertData(_databasename, _TableName, document);

            if (_connectionConfig.IsAutoCloseConnection == true) _database.Close();

            return;
        }
    }


}
