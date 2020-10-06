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
    public class MongoFileManage
    {
        private  ConnectionConfig _connectionConfig;
        private  MongoDBDatabase _database;
        private  string _databasename;

        public MongoFileManage(ConnectionConfig config, MongoDBDatabase database, String DatabaseName)
        {
            _connectionConfig = config;
            _database = database;
            _databasename = DatabaseName;
        }

        /// <summary>
        /// upload file
        /// 上传文件
        /// </summary>
        /// <param name="FilePath"></param>
        /// <param name="GridFSName"></param>
        /// <returns></returns>
        public String UploadFile(String FilePath, String GridFSName = "")
        {
            if (System.IO.File.Exists(FilePath) == false)
                throw new Exception("file path not found");

            //数据库连接
            if (_connectionConfig.IsAutoCloseConnection == false)
                if (_database.CheckStatus() == false)
                    throw new Exception("databse connect not open");
            if (_connectionConfig.IsAutoCloseConnection == true) _database.Open();
            String s = _database.UploadFile(_databasename, FilePath, GridFSName);

            if (_connectionConfig.IsAutoCloseConnection == true) _database.Close();

            return s;
        }

        /// <summary>
        /// download file
        /// 下载文件
        /// </summary>
        /// <param name="ID"></param>
        /// <param name="FilePath"></param>
        /// <param name="GridFSName"></param>
        public void DownloadFile(String ID, String FilePath, String GridFSName = "")
        {
            //数据库连接
            if (_connectionConfig.IsAutoCloseConnection == false)
                if (_database.CheckStatus() == false)
                    throw new Exception("databse connect not open");
            if (_connectionConfig.IsAutoCloseConnection == true) _database.Open();

            _database.DownloadFile(_databasename, ID, FilePath, GridFSName);

            if (_connectionConfig.IsAutoCloseConnection == true) _database.Close();

            return;
        }

        /// <summary>
        /// delete file
        /// 删除文件
        /// </summary>
        /// <param name="ID"></param>
        /// <param name="GridFSName"></param>
        public void DeleteFile(String ID, String GridFSName = "")
        {
            //数据库连接
            if (_connectionConfig.IsAutoCloseConnection == false)
                if (_database.CheckStatus() == false)
                    throw new Exception("databse connect not open");
            if (_connectionConfig.IsAutoCloseConnection == true) _database.Open();
            _database.DeleteFile(_databasename, ID, GridFSName);

            if (_connectionConfig.IsAutoCloseConnection == true) _database.Close();

            return;
        }

        /// <summary>
        /// delete file
        /// 改名文件
        /// </summary>
        /// <param name="ID"></param>
        /// <param name="FileName"></param>
        /// <param name="GridFSName"></param>
        public void ReNameFile(String ID, String FileName, String GridFSName = "")
        {
            //数据库连接
            if (_connectionConfig.IsAutoCloseConnection == false)
                if (_database.CheckStatus() == false)
                    throw new Exception("databse connect not open");
            if (_connectionConfig.IsAutoCloseConnection == true) _database.Open();
            _database.ReNameFile(_databasename, ID, FileName, GridFSName);

            if (_connectionConfig.IsAutoCloseConnection == true) _database.Close();

            return;
        }

        /// <summary>
        /// delete GridFS
        /// 删除文件库
        /// </summary>
        /// <param name="GridFSName"></param>
        public void DeleteGridFS(String GridFSName = "")
        {
            //数据库连接
            if (_connectionConfig.IsAutoCloseConnection == false)
                if (_database.CheckStatus() == false)
                    throw new Exception("databse connect not open");
            if (_connectionConfig.IsAutoCloseConnection == true) _database.Open();
            _database.DeleteGridFS(_databasename, GridFSName);

            if (_connectionConfig.IsAutoCloseConnection == true) _database.Close();

            return;
        }
    }
}
