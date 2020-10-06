﻿using System;
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
        private ConnectionConfig _connectionConfig;
        private MongoDBDatabase _database;
        private string _databasename;

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
        /// <returns></returns>
        public String UploadFile(String FilePath)
        {
            if (System.IO.File.Exists(FilePath) == false)
                throw new Exception("file path not found");

            //数据库连接
            if (_connectionConfig.IsAutoCloseConnection == false)
                if (_database.CheckStatus() == false)
                    throw new Exception("databse connect not open");
            if (_connectionConfig.IsAutoCloseConnection == true) _database.Open();
            String s = _database.UploadFile(_databasename, FilePath);

            if (_connectionConfig.IsAutoCloseConnection == true) _database.Close();

            return s;
        }

        /// <summary>
        /// download file
        /// 下载文件
        /// </summary>
        /// <param name="ID"></param>
        /// <param name="FilePath"></param>
        public void DownloadFile(String ID, String FilePath)
        {
            //数据库连接
            if (_connectionConfig.IsAutoCloseConnection == false)
                if (_database.CheckStatus() == false)
                    throw new Exception("databse connect not open");
            if (_connectionConfig.IsAutoCloseConnection == true) _database.Open();

            _database.DownloadFile(_databasename, ID, FilePath);

            if (_connectionConfig.IsAutoCloseConnection == true) _database.Close();

            return;
        }

        /// <summary>
        /// delete file
        /// 删除文件
        /// </summary>
        /// <param name="ID"></param>
        public void DeleteFile(String ID)
        {
            //数据库连接
            if (_connectionConfig.IsAutoCloseConnection == false)
                if (_database.CheckStatus() == false)
                    throw new Exception("databse connect not open");
            if (_connectionConfig.IsAutoCloseConnection == true) _database.Open();
            _database.DeleteFile(_databasename, ID);

            if (_connectionConfig.IsAutoCloseConnection == true) _database.Close();

            return;
        }
    }
}
