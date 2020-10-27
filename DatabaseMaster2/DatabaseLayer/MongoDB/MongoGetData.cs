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

    public class MongoGetData
    {
        private ConnectionConfig _connectionConfig;
        private MongoDBDatabase _database;
        private string _databasename;

        public MongoGetData(ConnectionConfig config, MongoDBDatabase database, String DatabaseName)
        {
            _connectionConfig = config;
            _database = database;
            _databasename = DatabaseName;
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
        /// get new guid
        /// 生成唯一标识号
        /// </summary>
        /// <param name="ObjectIDFormat"></param>
        /// <returns></returns>
        public String NewGUIDString(Boolean ObjectIDFormat = false)
        {
            if (ObjectIDFormat)
                return Guid.NewGuid().ToString("N");
            else
                return Guid.NewGuid().ToString();
        }

        /// <summary>
        /// get new objectid
        /// 生成唯一标识号
        /// </summary>
        /// <param name="ObjectIDFormat"></param>
        /// <returns></returns>
        public String NewObjectID(Boolean ObjectIDFormat = false)
        {
            if (ObjectIDFormat)
                return "ObjectId(\"" + ObjectId.GenerateNewId().ToString() + "\")";
            else
                return ObjectId.GenerateNewId().ToString();
        }

       

        /// <summary>
        /// get all data
        /// 得到表中所有数据
        /// </summary>
        /// <param name="TableName">集合名称 Collection</param>
        /// <returns></returns>
        public IHashTable Data(String TableName)
        {

            //数据库连接
            if (_connectionConfig.IsAutoCloseConnection == false)
                if (_database.CheckStatus() == false)
                    throw new Exception("databse connect not open");
            if (_connectionConfig.IsAutoCloseConnection == true) _database.Open();
            List<Hashtable> hr = _database.GetDataSet(_databasename, TableName, new BsonDocument());

            if (_connectionConfig.IsAutoCloseConnection == true) _database.Close();


            IHashTable DT = new IHashTable(hr);
            return DT;
        }


        /// <summary>
        /// get data by filter
        /// 得到指定条件的表中所有数据
        /// </summary>
        /// <param name="TableName">集合名称 Collection</param>
        /// <param name="ColumnName">筛选字段名 Filed</param>
        /// <param name="Value">筛选值 Filter value</param>
        /// <returns></returns>
        public IHashTable Data(String TableName, String ColumnName, Object Value)
        {
            //数据库连接
            if (_connectionConfig.IsAutoCloseConnection == false)
                if (_database.CheckStatus() == false)
                    throw new Exception("databse connect not open");
            if (_connectionConfig.IsAutoCloseConnection == true) _database.Open();

            List<Hashtable> hr = _database.GetDataSet(_databasename, TableName,
                MongoDBOP.GetFilterOP(ColumnName, Value, CommandComparison.Equals));

            if (_connectionConfig.IsAutoCloseConnection == true) _database.Close();

            IHashTable DT = new IHashTable(hr);
            return DT;
        }



        /// <summary>
        /// get data by filter
        /// 得到指定条件的表中所有数据
        /// </summary>
        /// <param name="TableName">集合名称 Collection</param>
        /// <param name="ColumnName">筛选字段名 Filed</param>
        /// <param name="Value">筛选值 Filter value</param>
        /// <param name="Comparison">筛选条件 Filter where</param>
        /// <returns></returns>
        public IHashTable Data(String TableName, String ColumnName, Object Value,
            CommandComparison Comparison)
        {

            //数据库连接
            if (_connectionConfig.IsAutoCloseConnection == false)
                if (_database.CheckStatus() == false)
                    throw new Exception("databse connect not open");
            if (_connectionConfig.IsAutoCloseConnection == true) _database.Open();
            List<Hashtable> hr = _database.GetDataSet(_databasename, TableName,
                MongoDBOP.GetFilterOP(ColumnName, Value, Comparison));

            if (_connectionConfig.IsAutoCloseConnection == true) _database.Close();

            IHashTable DT = new IHashTable(hr);
            return DT;
        }


        /// <summary>
        /// get data by filter
        /// 得到指定条件的表中所有数据
        /// </summary>
        /// <param name="TableName">集合名称 Collection</param>
        /// <param name="ColumnName">筛选字段名 Filed</param>
        /// <param name="Value">筛选值 Filter value</param>
        /// <returns></returns>
        public IHashTable Data(String TableName, String[] ColumnName, Object[] Value)
        {


            if (ColumnName.Length != Value.Length)
            {
                throw new Exception("Column number not Equals Value number");
            }

            //数据库连接
            if (_connectionConfig.IsAutoCloseConnection == false)
                if (_database.CheckStatus() == false)
                    throw new Exception("databse connect not open");
            if (_connectionConfig.IsAutoCloseConnection == true) _database.Open();
            List<Hashtable> hr = _database.GetDataSet(_databasename, TableName,
                MongoDBOP.GetWhere(ColumnName, Value));

            if (_connectionConfig.IsAutoCloseConnection == true) _database.Close();

            IHashTable DT = new IHashTable(hr);
            return DT;
        }



        /// <summary>
        /// get data by filter
        /// 得到指定条件的表中所有数据
        /// </summary>
        /// <param name="TableName">集合名称 Collection</param>
        /// <param name="ColumnName">筛选字段名 Filed</param>
        /// <param name="Comparison">筛选条件 Filter where</param>
        /// <param name="Value">筛选值 Filter value</param>
        /// <param name="relation">筛选关系 relation</param>
        /// <returns></returns>
        public IHashTable Data(String TableName, String[] ColumnName, CommandComparison[] Comparison,
            Object[] Value, WhereRelation[] relation)
        {
            if (ColumnName.Length != Value.Length && ColumnName.Length != Comparison.Length &&
                ColumnName.Length != relation.Length)
            {
                throw new Exception("Column number not Equals Value number");
            }

            //数据库连接
            if (_connectionConfig.IsAutoCloseConnection == false)
                if (_database.CheckStatus() == false)
                    throw new Exception("databse connect not open");
            if (_connectionConfig.IsAutoCloseConnection == true) _database.Open();
            List<Hashtable> hr = _database.GetDataSet(_databasename, TableName,
                MongoDBOP.GetWhere(ColumnName, Value, Comparison, relation));

            if (_connectionConfig.IsAutoCloseConnection == true) _database.Close();

            IHashTable DT = new IHashTable(hr);
            return DT;
        }

        /// <summary>
        /// check data exist by filter
        ///  检查表中是否存在指定内容
        /// </summary>
        /// <param name="TableName">集合名称 Collection</param>
        /// <param name="ColumnName">筛选字段名 Filed</param>
        /// <param name="Value">筛选值 Filter value</param>
        /// <returns></returns>
        public Boolean Exist(String TableName, String ColumnName, Object Value)
        {
            //数据库连接
            if (_connectionConfig.IsAutoCloseConnection == false)
                if (_database.CheckStatus() == false)
                    throw new Exception("databse connect not open");
            if (_connectionConfig.IsAutoCloseConnection == true) _database.Open();
            long Count = _database.GetDataCount(_databasename, TableName,
                MongoDBOP.GetFilterOP(ColumnName, Value, CommandComparison.Equals));

            if (_connectionConfig.IsAutoCloseConnection == true) _database.Close();

            if (Count == 0)
                return false;
            else
                return true;
        }

        /// <summary>
        /// check data exist by filter
        ///  检查表中是否存在指定内容
        /// </summary>
        /// <param name="TableName">集合名称 Collection</param>
        /// <param name="ColumnName">筛选字段名 Filed</param>
        /// <param name="Comparison">筛选条件 Filter where</param>
        /// <param name="Value">筛选值 Filter value</param>
        /// <returns></returns>
        public Boolean Exist(String TableName, String ColumnName,
            CommandComparison Comparison, Object Value)
        {
            //数据库连接
            if (_connectionConfig.IsAutoCloseConnection == false)
                if (_database.CheckStatus() == false)
                    throw new Exception("databse connect not open");
            if (_connectionConfig.IsAutoCloseConnection == true) _database.Open();
            long Count = _database.GetDataCount(_databasename, TableName,
                MongoDBOP.GetFilterOP(ColumnName, Value, Comparison));

            if (_connectionConfig.IsAutoCloseConnection == true) _database.Close();

            if (Count == 0)
                return false;
            else
                return true;
        }

        /// <summary>
        /// check data exist by filter
        ///  检查表中是否存在指定内容
        /// </summary>
        /// <param name="TableName">集合名称 Collection</param>
        /// <param name="ColumnName">筛选字段名 Filed</param>
        /// <param name="Value">筛选值 Filter value</param>
        /// <returns></returns>
        public Boolean Exist(String TableName, String[] ColumnName, Object[] Value)
        {
            if (ColumnName.Length != Value.Length)
            {
                throw new Exception("Column number not Equals Value number");
            }

            //数据库连接
            if (_connectionConfig.IsAutoCloseConnection == false)
                if (_database.CheckStatus() == false)
                    throw new Exception("databse connect not open");
            if (_connectionConfig.IsAutoCloseConnection == true) _database.Open();
            long Count = _database.GetDataCount(_databasename, TableName,
                MongoDBOP.GetWhere(ColumnName, Value));

            if (_connectionConfig.IsAutoCloseConnection == true) _database.Close();

            if (Count == 0)
                return false;
            else
                return true;
        }


        /// <summary>
        /// check data exist by filter
        ///  检查表中是否存在指定内容
        /// </summary>
        /// <param name="TableName">集合名称 Collection</param>
        /// <param name="ColumnName">筛选字段名 Filed</param>
        /// <param name="Comparison">筛选条件 Filter where</param>
        /// <param name="Value">筛选值 Filter value</param>
        /// <param name="relation">筛选关系 relation</param>
        /// <returns></returns>
        public Boolean Exist(String TableName, String[] ColumnName,
            CommandComparison[] Comparison, Object[] Value, WhereRelation[] relation)
        {
            if (ColumnName.Length != Value.Length && ColumnName.Length != Comparison.Length &&
                ColumnName.Length != relation.Length)
            {
                throw new Exception("Column number not Equals Value number");
            }

            //数据库连接
            if (_connectionConfig.IsAutoCloseConnection == false)
                if (_database.CheckStatus() == false)
                    throw new Exception("databse connect not open");
            if (_connectionConfig.IsAutoCloseConnection == true) _database.Open();
            long Count = _database.GetDataCount(_databasename, TableName,
                MongoDBOP.GetWhere(ColumnName, Value, Comparison, relation));

            if (_connectionConfig.IsAutoCloseConnection == true) _database.Close();

            if (Count == 0)
                return false;
            else
                return true;
        }



        /// <summary>
        /// get one data by filter
        /// 得到指定条件的一个数据
        /// </summary>
        /// <param name="TableName">集合名称 Collection</param>
        /// <param name="ColumnName">筛选字段名 Filed</param>
        /// <param name="KeyColumnName">筛选字段名 Filter field</param>
        /// <param name="KeyValue">筛选值 Filter value</param>
        /// <returns></returns>
        public Object First(String TableName, String ColumnName, String KeyColumnName, Object KeyValue)
        {
            //数据库连接
            if (_connectionConfig.IsAutoCloseConnection == false)
                if (_database.CheckStatus() == false)
                    throw new Exception("databse connect not open");
            if (_connectionConfig.IsAutoCloseConnection == true) _database.Open();
            Object result = _database.GetSpeciaRecordValue(_databasename, TableName,
                MongoDBOP.GetFilterOP(KeyColumnName, KeyValue, CommandComparison.Equals), ColumnName);

            if (_connectionConfig.IsAutoCloseConnection == true) _database.Close();

            return result;
        }


        /// <summary>
        /// get one data by filter
        /// 得到指定条件的一个数据
        /// </summary>
        /// <param name="TableName">集合名称 Collection</param>
        /// <param name="ColumnName">筛选字段名 Filed</param>
        /// <param name="KeyColumnName">筛选字段名 Filter field</param>
        /// <param name="KeyValue">筛选值 Filter value</param>
        /// <returns></returns>
        public Object First(String TableName, String ColumnName, String[] KeyColumnName,
            Object[] KeyValue)
        {
            if (KeyColumnName.Length != KeyValue.Length)
            {
                throw new Exception("Column number not Equals Value number");
            }

            //数据库连接
            if (_connectionConfig.IsAutoCloseConnection == false)
                if (_database.CheckStatus() == false)
                    throw new Exception("databse connect not open");
            if (_connectionConfig.IsAutoCloseConnection == true) _database.Open();
            Object result = _database.GetSpeciaRecordValue(_databasename, TableName,
                MongoDBOP.GetWhere(KeyColumnName, KeyValue), ColumnName);

            if (_connectionConfig.IsAutoCloseConnection == true) _database.Close();

            return result;
        }


        /// <summary>
        /// get count by filter
        /// 得到指定条件的行计数数据
        /// </summary>
        /// <param name="TableName">集合名称 Collection</param>
        /// <param name="ColumnName">筛选字段名 Filed</param>
        /// <param name="KeyColumnName">筛选字段名 Filter field</param>
        /// <param name="KeyValue">筛选值 Filter value</param>
        /// <returns></returns>
        public Object First(String TableName, String ColumnName, String[] KeyColumnName,
            Object[] KeyValue, Boolean CountNumber)
        {
            if (KeyColumnName.Length != KeyValue.Length)
            {
                throw new Exception("Column number not Equals Value number");
            }

            //数据库连接
            if (_connectionConfig.IsAutoCloseConnection == false)
                if (_database.CheckStatus() == false)
                    throw new Exception("databse connect not open");
            if (_connectionConfig.IsAutoCloseConnection == true) _database.Open();
            Object result = _database.GetDataCount(_databasename, TableName,
                MongoDBOP.GetWhere(KeyColumnName, KeyValue));

            if (_connectionConfig.IsAutoCloseConnection == true) _database.Close();

            return result;
        }


        /// <summary>
        /// get count by filter
        /// 得到指定条件的行计数数据
        /// </summary>
        /// <param name="TableName">集合名称 Collection</param>
        /// <param name="ColumnName">筛选字段名 Filed</param>
        /// <param name="KeyColumnName">筛选字段名 Filter field</param>
        /// <param name="Comparison">筛选条件 Filter where</param>
        /// <param name="KeyValue">筛选值 Filter value</param>
        /// <param name="relation">筛选关系 relation</param>
      
        /// <returns></returns>
        public Object First(String TableName, String ColumnName, String[] KeyColumnName,
            CommandComparison[] Comparison, Object[] KeyValue, WhereRelation[] relation,
            Boolean CountNumber)
        {
            if (KeyColumnName.Length != KeyValue.Length && KeyColumnName.Length != Comparison.Length &&
                KeyColumnName.Length != relation.Length)
            {
                throw new Exception("Column number not Equals Value number");
            }

            //数据库连接
            if (_connectionConfig.IsAutoCloseConnection == false)
                if (_database.CheckStatus() == false)
                    throw new Exception("databse connect not open");
            if (_connectionConfig.IsAutoCloseConnection == true) _database.Open();
            Object result = _database.GetDataCount(_databasename, TableName,
                MongoDBOP.GetWhere(KeyColumnName, KeyValue, Comparison, relation));

            if (_connectionConfig.IsAutoCloseConnection == true) _database.Close();

            return result;
        }


        /// <summary>
        /// get count by filter
        /// 得到指定条件的行计数数据
        /// </summary>
        /// <param name="TableName">集合名称 Collection</param>
        /// <returns></returns>
        public Object First(String TableName)
        {
            //数据库连接
            if (_connectionConfig.IsAutoCloseConnection == false)
                if (_database.CheckStatus() == false)
                    throw new Exception("databse connect not open");
            if (_connectionConfig.IsAutoCloseConnection == true) _database.Open();
            Object result = _database.GetDataCount(_databasename, TableName,
                new BsonDocument());

            if (_connectionConfig.IsAutoCloseConnection == true) _database.Close();

            return result;
        }

        /// <summary>
        /// get recordnumber data by filter
        /// 得到指定数据的指定内容
        /// </summary>
        /// <param name="TableName">集合名称 Collection</param>
        /// <param name="RecordNumber">记录数 Record number</param>
        /// <param name="OrderByName">排序字段名 Sort Field</param>
        /// <param name="sortMode">排序模式 Sort mode</param>
        /// <returns></returns>
        public IHashTable Data(String TableName, Int32 RecordNumber)
        {
            //数据库连接
            if (_connectionConfig.IsAutoCloseConnection == false)
                if (_database.CheckStatus() == false)
                    throw new Exception("databse connect not open");
            if (_connectionConfig.IsAutoCloseConnection == true) _database.Open();

            List<Hashtable> hr = _database.GetTopRecordsData(_databasename, TableName,
                new BsonDocument(), RecordNumber);

            if (_connectionConfig.IsAutoCloseConnection == true) _database.Close();

            IHashTable DT = new IHashTable(hr);
            return DT;
        }

        /// <summary>
        /// get recordnumber data by filter
        /// 得到指定数据的指定内容
        /// </summary>
        /// <param name="TableName">集合名称 Collection</param>
        /// <param name="ColumnName">筛选字段名 Filed</param>
        /// <param name="Value">筛选值 Filter value</param>
        /// <param name="RecordNumber">记录数 Record number</param>
        /// <returns></returns>
        public IHashTable Data(String TableName, String ColumnName, Object Value, Int32 RecordNumber)
        {
            //数据库连接
            if (_connectionConfig.IsAutoCloseConnection == false)
                if (_database.CheckStatus() == false)
                    throw new Exception("databse connect not open");
            if (_connectionConfig.IsAutoCloseConnection == true) _database.Open();

            List<Hashtable> hr = _database.GetTopRecordsData(_databasename, TableName,
                MongoDBOP.GetFilterOP(ColumnName, Value, CommandComparison.Equals), RecordNumber);

            if (_connectionConfig.IsAutoCloseConnection == true) _database.Close();

            IHashTable DT = new IHashTable(hr);
            return DT;
        }



        /// <summary>
        /// get recordnumber data by filter
        /// 得到指定数据的指定内容
        /// </summary>
        /// <param name="TableName">集合名称 Collection</param>
        /// <param name="ColumnName">筛选字段名 Filed</param>
        /// <param name="Comparison">筛选条件 Filter where</param>
        /// <param name="Value">筛选值 Filter value</param>
        /// <param name="RecordNumber">记录数 Record number</param>
        /// <returns></returns>
        public IHashTable Data(String TableName, String ColumnName, CommandComparison Comparison,
            Object Value, Int32 RecordNumber)
        {

            //数据库连接
            if (_connectionConfig.IsAutoCloseConnection == false)
                if (_database.CheckStatus() == false)
                    throw new Exception("databse connect not open");
            if (_connectionConfig.IsAutoCloseConnection == true) _database.Open();
            List<Hashtable> hr = _database.GetTopRecordsData(_databasename, TableName,
                MongoDBOP.GetFilterOP(ColumnName, Value, Comparison), RecordNumber);

            if (_connectionConfig.IsAutoCloseConnection == true) _database.Close();

            IHashTable DT = new IHashTable(hr);
            return DT;
        }


        /// <summary>
        /// get recordnumber data by filter
        /// 得到指定数据的指定内容
        /// </summary>
        /// <param name="TableName">集合名称 Collection</param>
        /// <param name="ColumnName">筛选字段名 Filed</param>
        /// <param name="Value">筛选值 Filter value</param>
        /// <param name="RecordNumber">记录数 Record number</param>
        /// <returns></returns>
        public IHashTable Data(String TableName, String[] ColumnName, Object[] Value,
            Int32 RecordNumber)
        {
            if (ColumnName.Length != Value.Length)
            {
                throw new Exception("Column number not Equals Value number");
            }

            //数据库连接
            if (_connectionConfig.IsAutoCloseConnection == false)
                if (_database.CheckStatus() == false)
                    throw new Exception("databse connect not open");
            if (_connectionConfig.IsAutoCloseConnection == true) _database.Open();
            List<Hashtable> hr = _database.GetTopRecordsData(_databasename, TableName,
                MongoDBOP.GetWhere(ColumnName, Value), RecordNumber);

            if (_connectionConfig.IsAutoCloseConnection == true) _database.Close();

            IHashTable DT = new IHashTable(hr);
            return DT;
        }



        /// <summary>
        /// get recordnumber data by filter
        /// 得到指定数据的指定内容
        /// </summary>
        /// <param name="TableName">集合名称 Collection</param>
        /// <param name="ColumnName">筛选字段名 Filed</param>
        /// <param name="Comparison">筛选条件 Filter where</param>
        /// <param name="Value">筛选值 Filter value</param>
        /// <param name="relation">筛选关系 relation</param>
        /// <param name="RecordNumber">记录数 Record number</param>
        /// <returns></returns>
        public IHashTable Data(String TableName, String[] ColumnName, CommandComparison[] Comparison,
            Object[] Value, WhereRelation[] relation, Int32 RecordNumber)
        {
            if (ColumnName.Length != Value.Length && ColumnName.Length != Comparison.Length &&
                ColumnName.Length != relation.Length)
            {
                throw new Exception("Column number not Equals Value number");
            }

            //数据库连接
            if (_connectionConfig.IsAutoCloseConnection == false)
                if (_database.CheckStatus() == false)
                    throw new Exception("databse connect not open");
            if (_connectionConfig.IsAutoCloseConnection == true) _database.Open();
            List<Hashtable> hr = _database.GetTopRecordsData(_databasename, TableName,
                MongoDBOP.GetWhere(ColumnName, Value, Comparison, relation), RecordNumber);

            if (_connectionConfig.IsAutoCloseConnection == true) _database.Close();

            IHashTable DT = new IHashTable(hr);
            return DT;
        }


        /// <summary>
        /// get page data by filter
        /// 得到指定数据的分页指定内容
        /// </summary>
        /// <param name="TableName">集合名称 Collection</param>
        /// <param name="StartNumber">开始行 Start line</param>
        /// <param name="RecordNumber">记录数 Record number</param>
        /// <returns></returns>
        public IHashTable Data(String TableName, Int32 StartNumber, Int32 RecordNumber)
        {
            //数据库连接
            if (_connectionConfig.IsAutoCloseConnection == false)
                if (_database.CheckStatus() == false)
                    throw new Exception("databse connect not open");
            if (_connectionConfig.IsAutoCloseConnection == true) _database.Open();

            List<Hashtable> hr = _database.GetTopPageRecordsData(_databasename, TableName,
                new BsonDocument(), StartNumber, RecordNumber);

            if (_connectionConfig.IsAutoCloseConnection == true) _database.Close();

            IHashTable DT = new IHashTable(hr);
            return DT;
        }

        /// <summary>
        /// get page data by filter
        /// 得到指定数据的分页指定内容
        /// </summary>
        /// <param name="TableName">集合名称 Collection</param>
        /// <param name="ColumnName">筛选字段名 Filed</param>
        /// <param name="Value">筛选值 Filter value</param>
        /// <param name="StartNumber">开始行 Start line</param>
        /// <param name="RecordNumber">记录数 Record number</param>
        /// <returns></returns>
        public IHashTable Data(String TableName, String ColumnName, Object Value,
            Int32 StartNumber, Int32 RecordNumber)
        {
            //数据库连接
            if (_connectionConfig.IsAutoCloseConnection == false)
                if (_database.CheckStatus() == false)
                    throw new Exception("databse connect not open");
            if (_connectionConfig.IsAutoCloseConnection == true) _database.Open();

            List<Hashtable> hr = _database.GetTopPageRecordsData(_databasename, TableName,
                MongoDBOP.GetFilterOP(ColumnName, Value, CommandComparison.Equals), StartNumber, RecordNumber);

            if (_connectionConfig.IsAutoCloseConnection == true) _database.Close();

            IHashTable DT = new IHashTable(hr);
            return DT;
        }


        /// <summary>
        /// get page data by filter
        /// 得到指定数据的分页指定内容
        /// </summary>
        /// <param name="TableName">集合名称 Collection</param>
        /// <param name="ColumnName">筛选字段名 Filed</param>
        /// <param name="Comparison">筛选条件 Filter where</param>
        /// <param name="Value">筛选值 Filter value</param>
        /// <param name="StartNumber">开始行 Start line</param>
        /// <param name="RecordNumber">记录数 Record number</param>
        /// <returns></returns>
        public IHashTable Data(String TableName, String ColumnName, CommandComparison Comparison,
            Object Value, Int32 StartNumber, Int32 RecordNumber)
        {

            //数据库连接
            if (_connectionConfig.IsAutoCloseConnection == false)
                if (_database.CheckStatus() == false)
                    throw new Exception("databse connect not open");
            if (_connectionConfig.IsAutoCloseConnection == true) _database.Open();
            List<Hashtable> hr = _database.GetTopPageRecordsData(_databasename, TableName,
                MongoDBOP.GetFilterOP(ColumnName, Value, Comparison), StartNumber, RecordNumber);

            if (_connectionConfig.IsAutoCloseConnection == true) _database.Close();

            IHashTable DT = new IHashTable(hr);
            return DT;
        }


        /// <summary>
        /// get page data by filter
        /// 得到指定数据的分页指定内容
        /// </summary>
        /// <param name="TableName">集合名称 Collection</param>
        /// <param name="ColumnName">筛选字段名 Filed</param>
        /// <param name="Value">筛选值 Filter value</param>
        /// <param name="StartNumber">开始行 Start line</param>
        /// <param name="RecordNumber">记录数 Record number</param>
        /// <returns></returns>
        public IHashTable Data(String TableName, String[] ColumnName, Object[] Value,
            Int32 StartNumber, Int32 RecordNumber)
        {
            if (ColumnName.Length != Value.Length)
            {
                throw new Exception("Column number not Equals Value number");
            }

            //数据库连接
            if (_connectionConfig.IsAutoCloseConnection == false)
                if (_database.CheckStatus() == false)
                    throw new Exception("databse connect not open");
            if (_connectionConfig.IsAutoCloseConnection == true) _database.Open();
            List<Hashtable> hr = _database.GetTopPageRecordsData(_databasename, TableName,
                MongoDBOP.GetWhere(ColumnName, Value), StartNumber, RecordNumber);

            if (_connectionConfig.IsAutoCloseConnection == true) _database.Close();

            IHashTable DT = new IHashTable(hr);
            return DT;
        }



        /// <summary>
        /// get page data by filter
        /// 得到指定数据的分页指定内容
        /// </summary>
        /// <param name="TableName">集合名称 Collection</param>
        /// <param name="ColumnName">筛选字段名 Filed</param>
        /// <param name="Comparison">筛选条件 Filter where</param>
        /// <param name="Value">筛选值 Filter value</param>
        /// <param name="relation">筛选关系 relation</param>
        /// <param name="StartNumber">开始行 Start line</param>
        /// <param name="RecordNumber">记录数 Record number</param>
        /// <returns></returns>
        public IHashTable Data(String TableName, String[] ColumnName,
            CommandComparison[] Comparison, Object[] Value, WhereRelation[] relation, Int32 StartNumber,
            Int32 RecordNumber)
        {
            if (ColumnName.Length != Value.Length && ColumnName.Length != Comparison.Length &&
                ColumnName.Length != relation.Length)
            {
                throw new Exception("Column number not Equals Value number");
            }

            //数据库连接
            if (_connectionConfig.IsAutoCloseConnection == false)
                if (_database.CheckStatus() == false)
                    throw new Exception("databse connect not open");
            if (_connectionConfig.IsAutoCloseConnection == true) _database.Open();
            List<Hashtable> hr = _database.GetTopPageRecordsData(_databasename, TableName,
                MongoDBOP.GetWhere(ColumnName, Value, Comparison, relation), StartNumber, RecordNumber);

            if (_connectionConfig.IsAutoCloseConnection == true) _database.Close();

            IHashTable DT = new IHashTable(hr);
            return DT;
        }


        /// <summary>
        /// get columns data by filter
        /// 得到表中列满足条件的数据
        /// </summary>
        /// <param name="TableName">集合名称 Collection</param>
        /// <param name="IncludeColumnName">包含列 include field</param>
        /// <param name="ExcludeColumnName">排除列 exclude field</param>
        /// <returns></returns>
        public IHashTable Data(String TableName, List<String> IncludeColumnName,
            List<String> ExcludeColumnName)
        {

            //数据库连接
            if (_connectionConfig.IsAutoCloseConnection == false)
                if (_database.CheckStatus() == false)
                    throw new Exception("databse connect not open");
            if (_connectionConfig.IsAutoCloseConnection == true) _database.Open();
            List<Hashtable> hr = _database.GetColumnDataSet(_databasename, TableName, IncludeColumnName,
                ExcludeColumnName,
                new BsonDocument());


            if (_connectionConfig.IsAutoCloseConnection == true) _database.Close();

            IHashTable DT = new IHashTable(hr);
            return DT;
        }


        /// <summary>
        /// get columns data by filter
        /// 得到表中列满足条件的数据
        /// </summary>
        /// <param name="TableName">集合名称 Collection</param>
        /// <param name="IncludeColumnName">包含列 include field</param>
        /// <param name="ExcludeColumnName">排除列 exclude field</param>
        /// <param name="KeyColumnName">筛选字段名 Filter field</param>
        /// <param name="KeyValue">筛选值 Filter value</param>
        /// <returns></returns>
        public IHashTable Data(String TableName, List<String> IncludeColumnName,
            List<String> ExcludeColumnName, String KeyColumnName, Object KeyValue)
        {
            //数据库连接
            if (_connectionConfig.IsAutoCloseConnection == false)
                if (_database.CheckStatus() == false)
                    throw new Exception("databse connect not open");
            if (_connectionConfig.IsAutoCloseConnection == true) _database.Open();
            List<Hashtable> hr = _database.GetColumnDataSet(_databasename, TableName, IncludeColumnName,
                ExcludeColumnName,
                MongoDBOP.GetFilterOP(KeyColumnName, KeyValue, CommandComparison.Equals));

            if (_connectionConfig.IsAutoCloseConnection == true) _database.Close();

            IHashTable DT = new IHashTable(hr);
            return DT;
        }



        /// <summary>
        /// get columns data by filter
        /// 得到表中列满足条件的数据
        /// </summary>
        /// <param name="TableName">集合名称 Collection</param>
        /// <param name="IncludeColumnName">包含列 include field</param>
        /// <param name="ExcludeColumnName">排除列 exclude field</param>
        /// <param name="KeyColumnName">筛选字段名 Filter field</param>
        /// <param name="Comparison">筛选条件 Filter where</param>
        /// <param name="KeyValue">筛选值 Filter value</param>
        /// <returns></returns>
        public IHashTable Data(String TableName, List<String> IncludeColumnName,
            List<String> ExcludeColumnName, String KeyColumnName, CommandComparison Comparison, Object KeyValue)
        {

            //数据库连接
            if (_connectionConfig.IsAutoCloseConnection == false)
                if (_database.CheckStatus() == false)
                    throw new Exception("databse connect not open");
            if (_connectionConfig.IsAutoCloseConnection == true) _database.Open();
            List<Hashtable> hr = _database.GetColumnDataSet(_databasename, TableName, IncludeColumnName,
                ExcludeColumnName,
                MongoDBOP.GetFilterOP(KeyColumnName, KeyValue, Comparison));

            if (_connectionConfig.IsAutoCloseConnection == true) _database.Close();

            IHashTable DT = new IHashTable(hr);
            return DT;
        }


        /// <summary>
        /// get columns data by filter
        /// 得到表中列满足条件的数据
        /// </summary>
        /// <param name="TableName">集合名称 Collection</param>
        /// <param name="IncludeColumnName">包含列 include field</param>
        /// <param name="ExcludeColumnName">排除列 exclude field</param>
        /// <param name="KeyColumnName">筛选字段名 Filter field</param>
        /// <param name="KeyValue">筛选值 Filter value</param>
        /// <returns></returns>
        public IHashTable Data(String TableName, List<String> IncludeColumnName,
            List<String> ExcludeColumnName, String[] KeyColumnName, Object[] KeyValue)
        {
            if (KeyColumnName.Length != KeyValue.Length)
            {
                throw new Exception("Column number not Equals Value number");
            }

            //数据库连接
            if (_connectionConfig.IsAutoCloseConnection == false)
                if (_database.CheckStatus() == false)
                    throw new Exception("databse connect not open");
            if (_connectionConfig.IsAutoCloseConnection == true) _database.Open();
            List<Hashtable> hr = _database.GetColumnDataSet(_databasename, TableName, IncludeColumnName,
                ExcludeColumnName,
                MongoDBOP.GetWhere(KeyColumnName, KeyValue));

            if (_connectionConfig.IsAutoCloseConnection == true) _database.Close();

            IHashTable DT = new IHashTable(hr);
            return DT;
        }



        /// <summary>
        /// get columns data by filter
        /// 得到表中列满足条件的数据
        /// </summary>
        /// <param name="TableName">集合名称 Collection</param>
        /// <param name="IncludeColumnName">包含列 include field</param>
        /// <param name="ExcludeColumnName">排除列 exclude field</param>
        /// <param name="KeyColumnName">筛选字段名 Filter field</param>
        /// <param name="Comparison"></param>
        /// <param name="KeyValue">筛选值 Filter value</param>
        /// <param name="relation">筛选关系 relation</param>
        /// <returns></returns>
        public IHashTable Data(String TableName, List<String> IncludeColumnName,
            List<String> ExcludeColumnName, String[] KeyColumnName, CommandComparison[] Comparison, Object[] KeyValue,
            WhereRelation[] relation)
        {
            if (KeyColumnName.Length != KeyValue.Length && KeyColumnName.Length != Comparison.Length &&
                KeyColumnName.Length != relation.Length)
            {
                throw new Exception("Column number not Equals Value number");
            }

            //数据库连接
            if (_connectionConfig.IsAutoCloseConnection == false)
                if (_database.CheckStatus() == false)
                    throw new Exception("databse connect not open");
            if (_connectionConfig.IsAutoCloseConnection == true) _database.Open();
            List<Hashtable> hr = _database.GetColumnDataSet(_databasename, TableName, IncludeColumnName,
                ExcludeColumnName,
                MongoDBOP.GetWhere(KeyColumnName, KeyValue, Comparison, relation));

            if (_connectionConfig.IsAutoCloseConnection == true) _database.Close();

            IHashTable DT = new IHashTable(hr);
            return DT;
        }

        /// <summary>
        /// Link table
        /// 关联表
        /// </summary>
        /// <param name="TableName">集合名称 Collection</param>
        /// <param name="ColumnName">筛选字段名 Filed</param>
        /// <param name="Comparison">筛选条件 Filter where</param>
        /// <param name="Value">筛选值 Filter value</param>
        /// <param name="relation">筛选关系 relation</param>
        /// <param name="IncludeColumnName">包含列 include field</param>
        /// <param name="ExcludeColumnName">排除列 exclude field</param>
        /// <param name="ForeignTable">外联表名</param>
        /// <param name="LocalField">关联字段</param>
        /// <param name="ForeignField">外联字段</param>
        /// <param name="ForeignName">外联集合名</param>
        /// <returns></returns>
        public IHashTable TableLink(String TableName,
            String[] ColumnName, CommandComparison[] Comparison,
            Object[] Value, WhereRelation[] relation
            , List<String> IncludeColumnName, List<String> ExcludeColumnName, String ForeignTable, String LocalField,
            String ForeignField, String ForeignName)
        {
            if (ColumnName.Length != Value.Length && ColumnName.Length != Comparison.Length &&
                ColumnName.Length != relation.Length)
            {
                throw new Exception("Column number not Equals Value number");
            }

            //数据库连接
            if (_connectionConfig.IsAutoCloseConnection == false)
                if (_database.CheckStatus() == false)
                    throw new Exception("databse connect not open");
            if (_connectionConfig.IsAutoCloseConnection == true) _database.Open();
            List<Hashtable> hr = _database.TableLink(_databasename, TableName,
                MongoDBOP.GetWhere(ColumnName, Value, Comparison, relation), IncludeColumnName, ExcludeColumnName
                , ForeignTable, LocalField, ForeignField, ForeignName);

            if (_connectionConfig.IsAutoCloseConnection == true) _database.Close();

            IHashTable DT = new IHashTable(hr);
            return DT;
        }

    }

}
