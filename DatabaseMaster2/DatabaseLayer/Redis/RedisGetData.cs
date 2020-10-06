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

    public class RedisGetData
    {
        private ConnectionConfig _connectionConfig;
        private RedisDBDatabase _database;

        public RedisGetData(ConnectionConfig config, RedisDBDatabase database)
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
        /// check key data
        /// 键值是否存在
        /// </summary>
        /// <param name="KeyName"></param>
        /// <returns></returns>
        public Boolean Exist(String KeyName)
        {

            //数据库连接
            if (_connectionConfig.IsAutoCloseConnection == false)
                if (_database.CheckStatus() == false)
                    throw new Exception("databse connect not open");
            if (_connectionConfig.IsAutoCloseConnection == true) _database.Open();
            Boolean ds = _database.ExistKey(KeyName);

            if (_connectionConfig.IsAutoCloseConnection == true) _database.Close();

            return ds;
        }

        /// <summary>
        /// check Hash key data
        /// Hash键值是否存在
        /// </summary>
        /// <param name="KeyName"></param>
        /// <returns></returns>
        public Boolean Exist(String KeyName, String Field)
        {

            //数据库连接
            if (_connectionConfig.IsAutoCloseConnection == false)
                if (_database.CheckStatus() == false)
                    throw new Exception("databse connect not open");
            if (_connectionConfig.IsAutoCloseConnection == true) _database.Open();
            Boolean ds = _database.ExistKey(KeyName, Field);

            if (_connectionConfig.IsAutoCloseConnection == true) _database.Close();

            return ds;
        }

        /// <summary>
        /// get key data
        /// 得到值数据
        /// </summary>
        /// <param name="KeyName"></param>
        /// <returns></returns>
        public T Data<T>(String KeyName)
        {

            //数据库连接
            if (_connectionConfig.IsAutoCloseConnection == false)
                if (_database.CheckStatus() == false)
                    throw new Exception("databse connect not open");
            if (_connectionConfig.IsAutoCloseConnection == true) _database.Open();
            T ds = _database.GetValue<T>(KeyName);

            if (_connectionConfig.IsAutoCloseConnection == true) _database.Close();

            return ds;
        }

        /// <summary>
        /// get Hash key data
        /// 得到Hash值数据
        /// </summary>
        /// <param name="KeyName"></param>
        /// <returns></returns>
        public T Data<T>(String KeyName, String Field)
        {

            //数据库连接
            if (_connectionConfig.IsAutoCloseConnection == false)
                if (_database.CheckStatus() == false)
                    throw new Exception("databse connect not open");
            if (_connectionConfig.IsAutoCloseConnection == true) _database.Open();
            T ds = _database.GetHashValue<T>(KeyName, Field);

            if (_connectionConfig.IsAutoCloseConnection == true) _database.Close();

            return ds;
        }

        /// <summary>
        /// get hash key data
        /// 得到hash值数据
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="KeyName"></param>
        /// <param name="Field"></param>
        /// <returns></returns>
        public Dictionary<String, T> DataDictionary<T>(String KeyName, String Field)
        {

            //数据库连接
            if (_connectionConfig.IsAutoCloseConnection == false)
                if (_database.CheckStatus() == false)
                    throw new Exception("databse connect not open");
            if (_connectionConfig.IsAutoCloseConnection == true) _database.Open();
            Dictionary<String, T> ds = _database.GetHashValue<T>(KeyName);

            if (_connectionConfig.IsAutoCloseConnection == true) _database.Close();

            return ds;
        }

        /// <summary>
        /// get list first key data
        /// 得到链表头值数据
        /// </summary>
        /// <param name="KeyName"></param>
        /// <returns></returns>
        public T First<T>(String KeyName)
        {

            //数据库连接
            if (_connectionConfig.IsAutoCloseConnection == false)
                if (_database.CheckStatus() == false)
                    throw new Exception("databse connect not open");
            if (_connectionConfig.IsAutoCloseConnection == true) _database.Open();
            T ds = _database.GetFirstListValue<T>(KeyName);

            if (_connectionConfig.IsAutoCloseConnection == true) _database.Close();

            return ds;
        }

        /// <summary>
        /// get list last key data
        /// 得到链表尾值数据
        /// </summary>
        /// <param name="KeyName"></param>
        /// <returns></returns>
        public T Last<T>(String KeyName)
        {

            //数据库连接
            if (_connectionConfig.IsAutoCloseConnection == false)
                if (_database.CheckStatus() == false)
                    throw new Exception("databse connect not open");
            if (_connectionConfig.IsAutoCloseConnection == true) _database.Open();
            T ds = _database.GetLastListValue<T>(KeyName);

            if (_connectionConfig.IsAutoCloseConnection == true) _database.Close();

            return ds;
        }

        /// <summary>
        /// get key data
        /// 得到值数据
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="KeyName"></param>
        /// <param name="Index"></param>
        /// <returns></returns>
        public T Data<T>(String KeyName, Int32 Index)
        {

            //数据库连接
            if (_connectionConfig.IsAutoCloseConnection == false)
                if (_database.CheckStatus() == false)
                    throw new Exception("databse connect not open");
            if (_connectionConfig.IsAutoCloseConnection == true) _database.Open();
            T ds = _database.GetIndexListValue<T>(KeyName, Index);

            if (_connectionConfig.IsAutoCloseConnection == true) _database.Close();

            return ds;
        }

        /// <summary>
        /// ge tlist key data length
        /// 得到值链表数据长度
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="KeyName"></param>
        /// <param name="Index"></param>
        /// <returns></returns>
        public Int32 Length(String KeyName, Int32 Start, Int32 End)
        {

            //数据库连接
            if (_connectionConfig.IsAutoCloseConnection == false)
                if (_database.CheckStatus() == false)
                    throw new Exception("databse connect not open");
            if (_connectionConfig.IsAutoCloseConnection == true) _database.Open();
            Int32 ds = _database.GetListLen(KeyName);

            if (_connectionConfig.IsAutoCloseConnection == true) _database.Close();

            return ds;
        }

        /// <summary>
        /// get key data
        /// 得到链表范围值数据
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="KeyName"></param>
        /// <param name="Start"></param>
        /// <param name="End"></param>
        /// <returns></returns>
        public T[] Data<T>(String KeyName, Int32 Start, Int32 End)
        {

            //数据库连接
            if (_connectionConfig.IsAutoCloseConnection == false)
                if (_database.CheckStatus() == false)
                    throw new Exception("databse connect not open");
            if (_connectionConfig.IsAutoCloseConnection == true) _database.Open();
            T[] ds = _database.GetRangeListValue<T>(KeyName, Start, End);

            if (_connectionConfig.IsAutoCloseConnection == true) _database.Close();

            return ds;
        }

    }

}
