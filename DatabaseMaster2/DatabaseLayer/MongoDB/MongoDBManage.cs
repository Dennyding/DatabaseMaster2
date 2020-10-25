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
    public class MongoDBManage
    {
        private  ConnectionConfig _connectionConfig;
        private  MongoDBDatabase _database;

        public MongoDBManage(ConnectionConfig config, MongoDBDatabase database)
        {
            _connectionConfig = config;
            _database = database;
        }

        /// <summary>
        /// get all database name
        /// </summary>
        /// <returns></returns>
        public List<String> GetDatabaseName()
        {
            List<String> s = new List<String>();

            //数据库连接
            if (_connectionConfig.IsAutoCloseConnection == false)
                if (_database.CheckStatus() == false)
                    throw new Exception("databse connect not open");
            if (_connectionConfig.IsAutoCloseConnection == true) _database.Open();

            s = _database.GetDatabaseName();

            if (_connectionConfig.IsAutoCloseConnection == true) _database.Close();

            return s;
        }

        /// <summary>
        /// get all database name
        /// </summary>
        /// <returns></returns>
        public List<String> GetCollectionName(String DatabaseName)
        {
            List<String> s = new List<String>();
            //数据库连接
            if (_connectionConfig.IsAutoCloseConnection == false)
                if (_database.CheckStatus() == false)
                    throw new Exception("databse connect not open");
            if (_connectionConfig.IsAutoCloseConnection == true) _database.Open();
            s = _database.GetCollectionName(DatabaseName);

            if (_connectionConfig.IsAutoCloseConnection == true) _database.Close();

            return s;
        }

        /// <summary>
        /// delete database
        /// </summary>
        /// <returns></returns>
        public void DropDatabase(String DatabaseName)
        {
            //数据库连接
            if (_connectionConfig.IsAutoCloseConnection == false)
                if (_database.CheckStatus() == false)
                    throw new Exception("databse connect not open");
            if (_connectionConfig.IsAutoCloseConnection == true) _database.Open();
            _database.DropDatabase(DatabaseName);

            if (_connectionConfig.IsAutoCloseConnection == true) _database.Close();

        }

        /// <summary>
        /// delete Collection
        /// </summary>
        /// <returns></returns>
        public void DropCollection(String DatabaseName, String TableName)
        {
            //数据库连接
            if (_connectionConfig.IsAutoCloseConnection == false)
                if (_database.CheckStatus() == false)
                    throw new Exception("databse connect not open");
            if (_connectionConfig.IsAutoCloseConnection == true) _database.Open();
            _database.DropCollection(DatabaseName, TableName);

            if (_connectionConfig.IsAutoCloseConnection == true) _database.Close();

        }

        /// <summary>
        /// create Collection
        /// </summary>
        /// <returns></returns>
        public void CreateCollection(String DatabaseName, String TableName)
        {
            //数据库连接
            if (_connectionConfig.IsAutoCloseConnection == false)
                if (_database.CheckStatus() == false)
                    throw new Exception("databse connect not open");
            if (_connectionConfig.IsAutoCloseConnection == true) _database.Open();
            _database.CreateCollection(DatabaseName, TableName);

            if (_connectionConfig.IsAutoCloseConnection == true) _database.Close();

        }

        /// <summary>
        /// rename Collection
        /// </summary>
        /// <returns></returns>
        public void RenameCollection(String DatabaseName, String OldTableName, String NewTableName)
        {
            //数据库连接
            if (_connectionConfig.IsAutoCloseConnection == false)
                if (_database.CheckStatus() == false)
                    throw new Exception("databse connect not open");
            if (_connectionConfig.IsAutoCloseConnection == true) _database.Open();
            _database.RenameCollection(DatabaseName, OldTableName, NewTableName);

            if (_connectionConfig.IsAutoCloseConnection == true) _database.Close();

        }

        /// <summary>
        /// rename Collection
        /// </summary>
        /// <returns></returns>
        public Hashtable RunCommand(String DatabaseName, String command)
        {
            //数据库连接
            if (_connectionConfig.IsAutoCloseConnection == false)
                if (_database.CheckStatus() == false)
                    throw new Exception("databse connect not open");
            if (_connectionConfig.IsAutoCloseConnection == true) _database.Open();
            Hashtable ht = _database.RunCommand(DatabaseName, command);

            if (_connectionConfig.IsAutoCloseConnection == true) _database.Close();

            return ht;

        }
    }
}
