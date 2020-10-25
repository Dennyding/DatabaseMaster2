using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data;
using MongoDB.Driver;
using MongoDB.Bson;
using System.Collections;
using Newtonsoft.Json;
using System.Text.RegularExpressions;
using System.IO;
using CSRedis;
using MongoDB.Driver.GridFS;

namespace DatabaseMaster2
{
    public class RedisDBDatabase
    {
        CSRedisClient conn = null;
        String ConnectString = "";

        /// <GetConnectionString>
        /// GetConnectionString
        /// </GetConnectionString>
        /// <returns></returns>
        public RedisDBDatabase()
        {
            ConnectString = XmlStream.getXmlValue(AppDomain.CurrentDomain.BaseDirectory + "DatabaseConfig.xml", "MongoDB", "ConnectString");
        }

        /// <GetConnectionString>
        /// GetConnectionString
        /// </GetConnectionString>
        /// <returns></returns>
        public RedisDBDatabase(String ConnString, Boolean ConnStr)
        {
            ConnectString = ConnString;
        }

        /// <GetConnectionString>
        /// GetConnectionString
        /// </GetConnectionString>
        /// <returns></returns>
        public RedisDBDatabase(String ConnectName)
        {
            ConnectString = XmlStream.getXmlValue(AppDomain.CurrentDomain.BaseDirectory + "DatabaseConfig.xml", ConnectName, "ConnectString");
        }

        /// <GetConnectionString>
        /// GetConnectionString
        /// </GetConnectionString>
        /// <returns></returns>
        public RedisDBDatabase(String ConnectName, StringEncrypt.EncryptType type)
        {
            String EncryptString = XmlStream.getXmlValue(AppDomain.CurrentDomain.BaseDirectory + "DatabaseConfig.xml", ConnectName, "ConnectString");

            if (type != StringEncrypt.EncryptType.None)
                ConnectString = StringEncrypt.DataDecrypt(type, EncryptString);
            else
                ConnectString = EncryptString;
        }


        /// <summary>
        /// Open database with connectstring
        /// </summary>
        public void Open()
        {
            if (conn != null)
            {
                ;
            }

            conn = new CSRedisClient(ConnectString);
        }


        /// <summary>
        /// close database with connectstring
        /// </summary>
        public void Close()
        {
            if (conn == null)
            {
                ;
            }
            conn = null;
        }


        /// <summary>
        /// check conn is open
        /// </summary>
        public Boolean CheckStatus()
        {
            if (conn == null)
            {
                return false;
            }

            try
            {
               return conn.Ping();
            }
            catch (Exception)
            {
                return false;
            }


            return true;
        }


        /// <summary>
        /// get value by key
        /// </summary>
        /// <param name="KeyName"></param>
        /// <returns></returns>
        public T GetValue<T>(String KeyName)
        {
            if (conn == null)
            {
                throw new Exception("Connection has not initialize.");
            }

            return conn.Get<T>(KeyName);
        }


        /// <summary>
        /// get value by key
        /// </summary>
        /// <param name="KeyName"></param>
        /// <param name="Field"></param>
        /// <returns></returns>
        public T GetHashValue<T>(String KeyName, String Field)
        {
            if (conn == null)
            {
                throw new Exception("Connection has not initialize.");
            }

            return conn.HGet<T>(KeyName, Field);
        }

        /// <summary>
        /// get value by key
        /// </summary>
        /// <param name="KeyName"></param>
        /// <returns></returns>
        public Dictionary<String, T> GetHashValue<T>(String KeyName)
        {
            if (conn == null)
            {
                throw new Exception("Connection has not initialize.");
            }

            return conn.HGetAll<T>(KeyName);
        }

        /// <summary>
        /// get value by key
        /// </summary>
        /// <param name="KeyName"></param>
        /// <returns></returns>
        public T GetFirstListValue<T>(String KeyName)
        {
            if (conn == null)
            {
                throw new Exception("Connection has not initialize.");
            }

            return conn.LPop<T>(KeyName);
        }

        /// <summary>
        /// get value by key
        /// </summary>
        /// <param name="KeyName"></param>
        /// <returns></returns>
        public T GetLastListValue<T>(String KeyName)
        {
            if (conn == null)
            {
                throw new Exception("Connection has not initialize.");
            }

            return conn.RPop<T>(KeyName);
        }

        /// <summary>
        /// get value by key
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="KeyName"></param>
        /// <param name="Index"></param>
        /// <returns></returns>
        public T GetIndexListValue<T>(String KeyName, Int32 Index)
        {
            if (conn == null)
            {
                throw new Exception("Connection has not initialize.");
            }

            return conn.LIndex<T>(KeyName, Index);
        }

        /// <summary>
        /// get value by key
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="KeyName"></param>
        /// <param name="Start"></param>
        /// <param name="End"></param>
        /// <returns></returns>
        public T[] GetRangeListValue<T>(String KeyName, Int32 Start, Int32 End)
        {
            if (conn == null)
            {
                throw new Exception("Connection has not initialize.");
            }

            return conn.LRange<T>(KeyName, Start, End);
        }

        /// <summary>
        /// get value by key
        /// </summary>
        /// <param name="KeyName"></param>
        /// <returns></returns>
        public Int32 GetListLen(String KeyName)
        {
            if (conn == null)
            {
                throw new Exception("Connection has not initialize.");
            }

            return (int)conn.LLen(KeyName);
        }

        /// <summary>
        /// set value by key
        /// </summary>
        /// <param name="KeyName"></param>
        /// <param name="Value"></param>
        /// <returns></returns>
        public Boolean SetStringValue(String KeyName, Object Value)
        {
            if (conn == null)
            {
                throw new Exception("Connection has not initialize.");
            }

            return conn.Set(KeyName, Value);
        }

        /// <summary>
        /// set value by hash
        /// </summary>
        /// <param name="KeyName"></param>
        /// <param name="Field"></param>
        /// <param name="Value"></param>
        /// <returns></returns>
        public Boolean SetHashValue(String KeyName,String Field, Object Value)
        {
            if (conn == null)
            {
                throw new Exception("Connection has not initialize.");
            }

            return conn.HSet(KeyName, Field, Value);
        }

        /// <summary>
        /// set value by list
        /// </summary>
        /// <param name="KeyName"></param>
        /// <param name="Value"></param>
        /// <returns></returns>
        public Int32 SetFirstListValue(String KeyName,  Object[] Value)
        {
            if (conn == null)
            {
                throw new Exception("Connection has not initialize.");
            }

            return (int)conn.LPush(KeyName, Value);
        }

        /// <summary>
        /// set value by list
        /// </summary>
        /// <param name="KeyName"></param>
        /// <param name="Value"></param>
        /// <returns></returns>
        public Int32 SetLastListValue(String KeyName, Object[] Value)
        {
            if (conn == null)
            {
                throw new Exception("Connection has not initialize.");
            }

            return (int)conn.RPush(KeyName, Value);
        }


        /// <summary>
        ///  set value by list
        /// </summary>
        /// <param name="KeyName"></param>
        /// <param name="Index"></param>
        /// <param name="Value"></param>
        /// <returns></returns>
        public Boolean SetListValue(String KeyName, Int32 Index, Object Value)
        {
            if (conn == null)
            {
                throw new Exception("Connection has not initialize.");
            }

            return conn.LSet(KeyName, Index, Value);
        }

        /// <summary>
        ///  set value by list
        /// </summary>
        /// <param name="KeyName"></param>
        /// <param name="Now"></param>
        /// <param name="Value"></param>
        /// <returns></returns>
        public Int32 InsertListValueAfter(String KeyName, Object Now, Object Value)
        {
            if (conn == null)
            {
                throw new Exception("Connection has not initialize.");
            }

            return (int)conn.LInsertAfter(KeyName, Now, Value);
        }

        /// <summary>
        ///  set value by list
        /// </summary>
        /// <param name="KeyName"></param>
        /// <param name="Now"></param>
        /// <param name="Value"></param>
        /// <returns></returns>
        public Int32 InsertListValueBefore(String KeyName, Object Now, Object Value)
        {
            if (conn == null)
            {
                throw new Exception("Connection has not initialize.");
            }

            return (int)conn.LInsertBefore(KeyName, Now, Value);
        }

        /// <summary>
        /// delete value by key
        /// </summary>
        /// <param name="KeyName"></param>
        /// <param name="Field"></param>
        /// <returns></returns>
        public Int32 DeleteHashValue(String KeyName, String[] Field)
        {
            if (conn == null)
            {
                throw new Exception("Connection has not initialize.");
            }

            return (int)conn.HDel(KeyName, Field);
        }


        /// <summary>
        /// delete value by key
        /// </summary>
        /// <param name="KeyName"></param>
        /// <returns></returns>
        public Int32 DeleteValue(String[] KeyName)
        {
            if (conn == null)
            {
                throw new Exception("Connection has not initialize.");
            }

            return (int)conn.Del(KeyName);
        }


        /// <summary>
        /// exist value by key
        /// </summary>
        /// <param name="KeyName"></param>
        /// <returns></returns>
        public Boolean ExistKey(String KeyName)
        {
            if (conn == null)
            {
                throw new Exception("Connection has not initialize.");
            }

            return conn.Exists(KeyName);
        }


        /// <summary>
        /// exist value by key
        /// </summary>
        /// <param name="KeyName"></param>
        /// <returns></returns>
        public Boolean ExistKey(String KeyName, String Field)
        {
            if (conn == null)
            {
                throw new Exception("Connection has not initialize.");
            }

            return conn.HExists(KeyName, Field);
        }

    }


}
