using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DatabaseMaster2
{
    public class DatabaseMaster
    {
        private ConnectionConfig _connectionConfig;
        private DatabaseInterface database;
        /// <summary>
        /// delete operate
        /// 删除操作
        /// </summary>
        public DatabaseDeleteData Deleteable;
        /// <summary>
        /// insert operate
        /// 插入操作
        /// </summary>
        public DatabaseInsertData Insertable;
        /// <summary>
        /// update operate
        /// 更新操作
        /// </summary>
        public DatabaseUpdateData Updateable;
        /// <summary>
        /// select operate
        /// 查询操作
        /// </summary>
        public DatabaseGetData Queryable;


        public DatabaseMaster(ConnectionConfig config)
        {
            _connectionConfig = config;

            database = DBFactory.CreateDatabase(_connectionConfig.DBType, _connectionConfig.ConnectionString);
            Deleteable = new DatabaseDeleteData(_connectionConfig, database);
            Insertable = new DatabaseInsertData(_connectionConfig, database);
            Updateable = new DatabaseUpdateData(_connectionConfig, database);
            Queryable = new DatabaseGetData(_connectionConfig, database);
        }
    }

    public class DatabaseMasterNOSQL
    {
        private ConnectionConfig _connectionConfig;
        private MongoDBDatabase database;

        /// <summary>
        /// update operate
        /// 更新操作
        /// </summary>
        public MongoUpdateData Updateable;
        /// <summary>
        /// select operate
        /// 查询操作
        /// </summary>
        public MongoGetData Queryable;
        /// <summary>
        /// delete operate
        /// 删除操作
        /// </summary>
        public MongoDeleteData Deleteable;
        /// <summary>
        /// insert operate
        /// 插入操作
        /// </summary>
        public MongoInsertData Insertable;
        /// <summary>
        /// file manage
        /// 文件管理
        /// </summary>
        public MongoFileManage FileManage;

        public DatabaseMasterNOSQL(ConnectionConfig config)
        {
            _connectionConfig = config;

            database = new MongoDBDatabase(_connectionConfig.ConnectionString, true);
            Updateable = new MongoUpdateData(_connectionConfig, database, config.DatabaseName);
            Queryable = new MongoGetData(_connectionConfig, database, config.DatabaseName);
            Deleteable = new MongoDeleteData(_connectionConfig, database, config.DatabaseName);
            Insertable = new MongoInsertData(_connectionConfig, database, config.DatabaseName);
            FileManage = new MongoFileManage(_connectionConfig, database, config.DatabaseName);
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
}