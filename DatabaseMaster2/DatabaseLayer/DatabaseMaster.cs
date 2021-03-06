﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DatabaseMaster2
{
    /// <summary>
    /// SQL关系数据库
    /// </summary>
    public class DatabaseMaster
    {
        private ConnectionConfig _connectionConfig;
        private DatabaseInterface database;

        public DatabaseMaster(ConnectionConfig config)
        {
            _connectionConfig = config;

            database = DBFactory.CreateDatabase(_connectionConfig.DBType, _connectionConfig.ConnectionString);
        }

        /// <summary>
        /// open connect
        /// 长连接数据库
        /// </summary>
        public void Connect()
        {
            database.Open();
        }


        /// <summary>
        /// close connect
        /// 长关闭数据库
        /// </summary>
        public void Close()
        {
            database.Close();
        }

        /// <summary>
        /// Query data
        /// 查询数据
        /// </summary>
        /// <param name="TableName">TableName 数据表名称</param>
        /// <returns></returns>
        public DatabaseGetData Queryable(String TableName)
        {
            return new DatabaseGetData(_connectionConfig, database, TableName);
        }

        /// <summary>
        /// Query data
        /// 查询数据
        /// </summary>
        /// <param name="TableName">TableName 数据表名称</param>
        /// <returns></returns>
        public DatabaseGetData Queryable(List<string> TableName)
        {
            return new DatabaseGetData(_connectionConfig, database, TableName);
        }

        /// <summary>
        /// Update data
        /// 更新数据
        /// </summary>
        /// <param name="TableName">TableName 数据表名称</param>
        /// <returns></returns>
        public DatabaseUpdateData Updateable(String TableName)
        {
            return new DatabaseUpdateData(_connectionConfig, database, TableName);
        }

        /// <summary>
        /// Delete data
        /// 删除数据
        /// </summary>
        /// <param name="TableName">TableName 数据表名称</param>
        /// <returns></returns>
        public DatabaseDeleteData Deleteable(String TableName)
        {
            return new DatabaseDeleteData(_connectionConfig, database, TableName);
        }

        /// <summary>
        /// Insert data
        /// 插入数据
        /// </summary>
        /// <param name="TableName">TableName 数据表名称</param>
        /// <returns></returns>
        public DatabaseInsertData Insertable(String TableName)
        {
            return new DatabaseInsertData(_connectionConfig, database, TableName);
        }
    }

    /// <summary>
    /// MongoDB数据库
    /// </summary>
    public class DatabaseMasterMongoDB
    {
        private ConnectionConfig _connectionConfig;
        private MongoDBDatabase database;

        public DatabaseMasterMongoDB(ConnectionConfig config)
        {
            _connectionConfig = config;

            database = new MongoDBDatabase(_connectionConfig.ConnectionString, true);

        }

        /// <summary>
        /// Query data
        /// 查询数据
        /// </summary>
        /// <param name="databaseName">database-name 数据库名称</param>
        /// <param name="TableName">TableName 数据表名称</param>
        /// <returns></returns>
        public MongoGetData Queryable(String databaseName, String TableName)
        {
            return new MongoGetData(_connectionConfig, database, databaseName, TableName);
        }

        /// <summary>
        /// Update data
        /// 更新数据
        /// </summary>
        /// <param name="databaseName">database-name 数据库名称</param>
        /// <param name="TableName">TableName 数据表名称</param>
        /// <returns></returns>
        public MongoUpdateData Updateable(String databaseName, String TableName)
        {
            return new MongoUpdateData(_connectionConfig, database, databaseName, TableName);
        }

        /// <summary>
        /// Delete data
        /// 删除数据
        /// </summary>
        /// <param name="databaseName">database-name 数据库名称</param>
        /// <param name="TableName">TableName 数据表名称</param>
        /// <returns></returns>
        public MongoDeleteData Deleteable(String databaseName, String TableName)
        {
            return new MongoDeleteData(_connectionConfig, database, databaseName, TableName);
        }

        /// <summary>
        /// Insert data
        /// 插入数据
        /// </summary>
        /// <param name="databaseName">database-name 数据库名称</param>
        /// <param name="TableName">TableName 数据表名称</param>
        /// <returns></returns>
        public MongoInsertData Insertable(String databaseName, String TableName)
        {
            return new MongoInsertData(_connectionConfig, database, databaseName, TableName);
        }

        /// <summary>
        /// File manage
        /// 文件管理
        /// </summary>
        /// <param name="databaseName">database-name 数据库名称</param>
        /// <returns></returns>
        public MongoFileManage FileManage(String databaseName)
        {
            return new MongoFileManage(_connectionConfig, database, databaseName);
        }

        /// <summary>
        /// DBManage
        /// 数据库管理
        /// </summary>
        /// <param name="databaseName">database-name 数据库名称</param>
        /// <returns></returns>
        public MongoDBManage DBManage()
        {
            return new MongoDBManage(_connectionConfig, database);
        }


        /// <summary>
        /// open connect
        /// 长连接数据库
        /// </summary>
        public void Connect()
        {
            database.Open();
        }


        /// <summary>
        /// close connect
        /// 长关闭数据库
        /// </summary>
        public void Close()
        {
            database.Close();
        }

        /// <summary>
        /// Start Transaction
        /// 开始事务
        /// </summary>
        public void StartTransaction()
        {
            if (database.CheckStatus()==false)
            {
                throw new Exception("Database not open,call Open()");
            }

            database.StartTransaction();
        }

        /// <summary>
        /// Commit Transaction
        /// 提交事务
        /// </summary>
        public void CommitTransaction()
        {
            if (database.CheckStatus() == false)
            {
                throw new Exception("Database not open,call Open()");
            }

            database.CommitTransaction();
        }

        /// <summary>
        /// CloseTransaction
        /// 关闭事务
        /// </summary>
        public void CloseTransaction()
        {
            if (database.CheckStatus() == false)
            {
                throw new Exception("Database not open,call Open()");
            }

            database.CloseTransaction();
        }

    }

    /// <summary>
    /// Redis数据库
    /// </summary>
    public class DatabaseMasterRedis
    {
        private ConnectionConfig _connectionConfig;
        private RedisDBDatabase database;

        public DatabaseMasterRedis(ConnectionConfig config)
        {
            _connectionConfig = config;

            database = new RedisDBDatabase(_connectionConfig.ConnectionString, true);

        }

        /// <summary>
        /// Query data
        /// 查询数据
        /// </summary>
        /// <returns></returns>
        public RedisGetData Readable()
        {
            return new RedisGetData(_connectionConfig, database);
        }

        /// <summary>
        /// Query data
        /// 查询数据
        /// </summary>
        /// <returns></returns>
        public RedisSetData Writeable()
        {
            return new RedisSetData(_connectionConfig, database);
        }


        /// <summary>
        /// open connect
        /// 长连接数据库
        /// </summary>
        public void Connect()
        {
            database.Open();
        }


        /// <summary>
        /// close connect
        /// 长关闭数据库
        /// </summary>
        public void Close()
        {
            database.Close();
        }

    
    }
}