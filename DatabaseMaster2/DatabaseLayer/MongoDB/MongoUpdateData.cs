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
    public class MongoUpdateData
    {
        private ConnectionConfig _connectionConfig;
        private MongoDBDatabase _database;
        private string _databasename;

        private String _TableName;
        private FilterDefinition<BsonDocument> filterDefinition;
        Hashtable htData = new Hashtable();

        public MongoUpdateData(ConnectionConfig config, MongoDBDatabase database, String DatabaseName, String TableName)
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
        public MongoUpdateData Clear()
        {
            htData.Clear();
            filterDefinition = null;

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
            Int32 count = _database.UpdateData(_databasename, _TableName,filterDefinition, htData);

            if (_connectionConfig.IsAutoCloseConnection == true) _database.Close();

            return count;
        }

        /// <summary>
        /// update new data
        /// 更新数据
        /// </summary>
        /// <param name="ColumnName"></param>
        /// <param name="Value"></param>
        /// <returns></returns>
        public MongoUpdateData Data(String[] ColumnName, Object[] Value)
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
        /// update new data
        /// 更新数据
        /// </summary>
        /// <param name="KeyColumnName"></param>
        /// <param name="KeyValue"></param>
        /// <returns></returns>
        public MongoUpdateData Where(String KeyColumnName,Object KeyValue)
        {

            filterDefinition = MongoDBOP.GetFilterOP(KeyColumnName, KeyValue, CommandComparison.Equals);

            return this;
        }

        /// <summary>
        /// update new data
        /// 更新数据
        /// </summary>
        /// <param name="KeyColumnName"></param>
        /// <param name="comparison"></param>
        /// <param name="KeyValue"></param>
        /// <returns></returns>
        public MongoUpdateData Where(String KeyColumnName,
            CommandComparison comparison, Object KeyValue)
        {

            filterDefinition = MongoDBOP.GetFilterOP(KeyColumnName, KeyValue, comparison);

            return this;
        }


        /// <summary>
        /// update many new data
        /// 更新多个数据
        /// </summary>
        /// <param name="KeyColumnName"></param>
        /// <param name="KeyValue"></param>
        /// <returns></returns>
        public MongoUpdateData Where(String[] KeyColumnName,
            Object[] KeyValue)
        {

           filterDefinition= MongoDBOP.GetWhere(KeyColumnName, KeyValue);

            return this;
        }

        /// <summary>
        /// update many new data
        /// 更新多个数据
        /// </summary>
        /// <param name="KeyColumnName"></param>
        /// <param name="comparison"></param>
        /// <param name="KeyValue"></param>
        /// <param name="relation"></param>
        /// <returns></returns>
        public MongoUpdateData Where(String[] KeyColumnName,
            CommandComparison[] comparison, Object[] KeyValue, WhereRelation[] relation)
        {

           filterDefinition= MongoDBOP.GetWhere(KeyColumnName, KeyValue, comparison, relation);

            return this;
        }

    }

}
