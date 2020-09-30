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

        public MongoUpdateData(ConnectionConfig config, MongoDBDatabase database, String DatabaseName)
        {
            _connectionConfig = config;
            _database = database;
            _databasename = DatabaseName;
        }

        /// <summary>
        /// use database name
        /// 切换数据库名称
        /// </summary>
        public string SetDatabaseName
        {
            set => _databasename = value;
            get => _databasename;
        }


        /// <summary>
        /// update new data
        /// 更新数据
        /// </summary>
        /// <param name="TableName"></param>
        /// <param name="ColumnName"></param>
        /// <param name="Value"></param>
        /// <param name="KeyColumnName"></param>
        /// <param name="KeyValue"></param>
        /// <returns></returns>
        public Int32 Data(String TableName, String[] ColumnName, Object[] Value, String KeyColumnName,
            Object KeyValue)
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
            Int32 count = _database.UpdateData(_databasename, TableName,
                MongoDBOP.GetFilterOP(KeyColumnName, KeyValue, CommandComparison.Equals), ht);

            if (_connectionConfig.IsAutoCloseConnection == true) _database.Close();

            return count;
        }

        /// <summary>
        /// update new data
        /// 更新数据
        /// </summary>
        /// <param name="TableName"></param>
        /// <param name="ColumnName"></param>
        /// <param name="Value"></param>
        /// <param name="KeyColumnName"></param>
        /// <param name="KeyValue"></param>
        /// <returns></returns>
        public Int32 Data(String TableName, String[] ColumnName, Object[] Value, String KeyColumnName,
            CommandComparison comparison, Object KeyValue)
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
            Int32 count = _database.UpdateData(_databasename, TableName,
                MongoDBOP.GetFilterOP(KeyColumnName, KeyValue, comparison), ht);

            if (_connectionConfig.IsAutoCloseConnection == true) _database.Close();

            return count;
        }


        /// <summary>
        /// update many new data
        /// 更新多个数据
        /// </summary>
        /// <param name="TableName"></param>
        /// <param name="ColumnName"></param>
        /// <param name="Value"></param>
        /// <param name="KeyColumnName"></param>
        /// <param name="KeyValue"></param>
        /// <returns></returns>
        public Int32 Data(String TableName, String[] ColumnName, Object[] Value, String[] KeyColumnName,
            Object[] KeyValue)
        {


            if (ColumnName.Length != Value.Length)
            {
                throw new Exception("Column number not Equals Value number");
            }

            if (KeyColumnName.Length != KeyValue.Length)
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
            Int32 count = _database.UpdateData(_databasename, TableName,
                MongoDBOP.GetWhere(KeyColumnName, KeyValue), ht);

            if (_connectionConfig.IsAutoCloseConnection == true) _database.Close();

            return count;
        }

        /// <summary>
        /// update many new data
        /// 更新多个数据
        /// </summary>
        /// <param name="TableName"></param>
        /// <param name="ColumnName"></param>
        /// <param name="Value"></param>
        /// <param name="KeyColumnName"></param>
        /// <param name="KeyValue"></param>
        /// <returns></returns>
        public Int32 Data(String TableName, String[] ColumnName, Object[] Value, String[] KeyColumnName,
            CommandComparison[] comparison, Object[] KeyValue, WhereRelation[] relation)
        {


            if (ColumnName.Length != Value.Length)
            {
                throw new Exception("Column number not Equals Value number");
            }

            if (KeyColumnName.Length != KeyValue.Length)
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
            Int32 count = _database.UpdateData(_databasename, TableName,
                MongoDBOP.GetWhere(KeyColumnName, KeyValue, comparison, relation),
                ht);

            if (_connectionConfig.IsAutoCloseConnection == true) _database.Close();

            return count;
        }

    }

}
