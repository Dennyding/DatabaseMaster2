using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data;
using System.Collections;
using System.Net.Http.Headers;
using MongoDB.Bson;
using MongoDB.Driver;

namespace DatabaseMaster2
{

    public class RedisSetData
    {
        private ConnectionConfig _connectionConfig;
        private RedisDBDatabase _database;

        public RedisSetData(ConnectionConfig config, RedisDBDatabase database)
        {
            _connectionConfig = config;
            _database = database;
        }


        /// <summary>
        /// check connect status
        /// 检查数据库连接
        /// </summary>
        /// <returns></returns>
        public Boolean Status()
        {
            try
            {
                //数据库连接
                if (_database.CheckStatus() == false)
                    return false;
                else
                    return true;
            }
            catch (Exception)
            {
                return false;
            }

        }


        /// <summary>
        /// set key data
        /// 写入值数据
        /// </summary>
        /// <param name="KeyName"></param>
        /// <param name="Value"></param>
        /// <returns></returns>
        public Boolean Data(String KeyName, Object Value)
        {

            //数据库连接
            if (_connectionConfig.IsAutoCloseConnection == false)
                if (_database.CheckStatus() == false)
                    throw new Exception("databse connect not open");
            if (_connectionConfig.IsAutoCloseConnection == true) _database.Open();
            Boolean ds = _database.SetStringValue(KeyName,Value);

            if (_connectionConfig.IsAutoCloseConnection == true) _database.Close();

            return ds;
        }

        /// <summary>
        /// set Hash key data
        /// 写入Hash值数据
        /// </summary>
        /// <param name="KeyName"></param>
        /// <param name="Field"></param>
        /// <param name="Value"></param>
        /// <returns></returns>
        public Boolean Data(String KeyName, String Field, Object Value)
        {

            //数据库连接
            if (_connectionConfig.IsAutoCloseConnection == false)
                if (_database.CheckStatus() == false)
                    throw new Exception("databse connect not open");
            if (_connectionConfig.IsAutoCloseConnection == true) _database.Open();
            Boolean ds = _database.SetHashValue(KeyName, Field, Value);

            if (_connectionConfig.IsAutoCloseConnection == true) _database.Close();

            return ds;
        }

        /// <summary>
        /// set key data
        /// 写入值数据
        /// </summary>
        /// <param name="KeyName"></param>
        /// <param name="Index"></param>
        /// <param name="Value"></param>
        /// <returns></returns>
        public Boolean Data(String KeyName, Int32 Index, Object Value)
        {

            //数据库连接
            if (_connectionConfig.IsAutoCloseConnection == false)
                if (_database.CheckStatus() == false)
                    throw new Exception("databse connect not open");
            if (_connectionConfig.IsAutoCloseConnection == true) _database.Open();
            Boolean ds = _database.SetListValue(KeyName, Index, Value);

            if (_connectionConfig.IsAutoCloseConnection == true) _database.Close();

            return ds;
        }

        /// <summary>
        /// set list key data
        /// 写入链表值数据
        /// </summary>
        /// <param name="KeyName"></param>
        /// <param name="Value"></param>
        /// <returns></returns>
        public Int32 First(String KeyName,Object[] Value)
        {

            //数据库连接
            if (_connectionConfig.IsAutoCloseConnection == false)
                if (_database.CheckStatus() == false)
                    throw new Exception("databse connect not open");
            if (_connectionConfig.IsAutoCloseConnection == true) _database.Open();
            Int32 ds = _database.SetFirstListValue(KeyName, Value);

            if (_connectionConfig.IsAutoCloseConnection == true) _database.Close();

            return ds;
        }

        /// <summary>
        /// set list key data
        /// 写入链表值数据
        /// </summary>
        /// <param name="KeyName"></param>
        /// <param name="Value"></param>
        /// <returns></returns>
        public Int32 Last(String KeyName, Object[] Value)
        {

            //数据库连接
            if (_connectionConfig.IsAutoCloseConnection == false)
                if (_database.CheckStatus() == false)
                    throw new Exception("databse connect not open");
            if (_connectionConfig.IsAutoCloseConnection == true) _database.Open();
            Int32 ds = _database.SetLastListValue(KeyName, Value);

            if (_connectionConfig.IsAutoCloseConnection == true) _database.Close();

            return ds;
        }

        /// <summary>
        /// insert after list
        /// 链表元素后插入值
        /// </summary>
        /// <param name="KeyName"></param>
        /// <param name="Now"></param>
        /// <param name="Value"></param>
        /// <returns></returns>
        public Int32 InsertAfter(String KeyName, Object Now, Object Value)
        {

            //数据库连接
            if (_connectionConfig.IsAutoCloseConnection == false)
                if (_database.CheckStatus() == false)
                    throw new Exception("databse connect not open");
            if (_connectionConfig.IsAutoCloseConnection == true) _database.Open();
            Int32 ds = _database.InsertListValueAfter(KeyName, Now, Value);

            if (_connectionConfig.IsAutoCloseConnection == true) _database.Close();

            return ds;
        }

        /// <summary>
        /// insert before list
        /// 链表元素前插入值
        /// </summary>
        /// <param name="KeyName"></param>
        /// <param name="Now"></param>
        /// <param name="Value"></param>
        /// <returns></returns>
        public Int32 InsertBefore(String KeyName, Object Now, Object Value)
        {

            //数据库连接
            if (_connectionConfig.IsAutoCloseConnection == false)
                if (_database.CheckStatus() == false)
                    throw new Exception("databse connect not open");
            if (_connectionConfig.IsAutoCloseConnection == true) _database.Open();
            Int32 ds = _database.InsertListValueBefore(KeyName, Now, Value);

            if (_connectionConfig.IsAutoCloseConnection == true) _database.Close();

            return ds;
        }

        /// <summary>
        /// delete key
        /// 删除键值
        /// </summary>
        /// <param name="KeyName"></param>
        /// <returns></returns>
        public Int32 Delete(String[] KeyName)
        {

            //数据库连接
            if (_connectionConfig.IsAutoCloseConnection == false)
                if (_database.CheckStatus() == false)
                    throw new Exception("databse connect not open");
            if (_connectionConfig.IsAutoCloseConnection == true) _database.Open();
            Int32 ds = _database.DeleteValue(KeyName);

            if (_connectionConfig.IsAutoCloseConnection == true) _database.Close();

            return ds;
        }

        /// <summary>
        /// delete Hash key
        /// 删除Hash键值
        /// </summary>
        /// <param name="KeyName"></param>
        /// <param name="Field"></param>
        /// <returns></returns>
        public Int32 Delete(String KeyName, String[] Field)
        {

            //数据库连接
            if (_connectionConfig.IsAutoCloseConnection == false)
                if (_database.CheckStatus() == false)
                    throw new Exception("databse connect not open");
            if (_connectionConfig.IsAutoCloseConnection == true) _database.Open();
            Int32 ds = _database.DeleteHashValue(KeyName, Field);

            if (_connectionConfig.IsAutoCloseConnection == true) _database.Close();

            return ds;
        }

    }

}
