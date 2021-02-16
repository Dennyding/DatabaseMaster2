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

        private String _TableName;
        private FilterDefinition<BsonDocument> filterDefinition;

        public MongoGetData(ConnectionConfig config, MongoDBDatabase database, String DatabaseName, String TableName)
        {
            _connectionConfig = config;
            _database = database;
            _databasename = DatabaseName;
            _TableName = TableName;
        }

        /// <summary>
        /// clear filter
        /// 清除过滤条件
        /// </summary>
        /// <returns></returns>
        public MongoGetData Clear()
        {
            filterDefinition = null;

            return this;
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
        /// <returns></returns>
        public IHashTable Data()
        {
            //数据库连接
            if (_connectionConfig.IsAutoCloseConnection == false)
                if (_database.CheckStatus() == false)
                    throw new Exception("databse connect not open");
            if (_connectionConfig.IsAutoCloseConnection == true) _database.Open();


            List<Hashtable> hr;

            if(filterDefinition != null)
                hr = _database.GetDataSet(_databasename, _TableName, filterDefinition);
            else
                hr = _database.GetDataSet(_databasename, _TableName, new BsonDocument());

            if (_connectionConfig.IsAutoCloseConnection == true) _database.Close();


            IHashTable DT = new IHashTable(hr);
            return DT;
        }


        /// <summary>
        /// get all data
        /// 得到表中所有数据
        /// </summary>
        /// <param name="IncludeColumnName">包含列 include field</param>
        /// <param name="ExcludeColumnName">排除列 exclude field</param>
        /// <returns></returns>
        public IHashTable Data(List<String> IncludeColumnName,
            List<String> ExcludeColumnName)
        {
            //数据库连接
            if (_connectionConfig.IsAutoCloseConnection == false)
                if (_database.CheckStatus() == false)
                    throw new Exception("databse connect not open");
            if (_connectionConfig.IsAutoCloseConnection == true) _database.Open();


            List<Hashtable> hr;

            if (filterDefinition != null)
                hr = _database.GetColumnDataSet(_databasename, _TableName, IncludeColumnName,
                ExcludeColumnName, filterDefinition);
            else
                hr = _database.GetColumnDataSet(_databasename, _TableName, IncludeColumnName,
                ExcludeColumnName, new BsonDocument());

            if (_connectionConfig.IsAutoCloseConnection == true) _database.Close();


            IHashTable DT = new IHashTable(hr);
            return DT;
        }

        /// <summary>
        /// check data exist by filter
        ///  检查表中是否存在指定内容
        /// </summary>
        /// <returns></returns>
        public Boolean Exist()
        {
            //数据库连接
            if (_connectionConfig.IsAutoCloseConnection == false)
                if (_database.CheckStatus() == false)
                    throw new Exception("databse connect not open");
            if (_connectionConfig.IsAutoCloseConnection == true) _database.Open();

            long Count;

            if(filterDefinition != null)
                Count = _database.GetDataCount(_databasename, _TableName, filterDefinition);
            else
                Count = _database.GetDataCount(_databasename, _TableName, filterDefinition);

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
        /// <param name="ColumnName"></param>
        /// <returns></returns>
        public Object First( String ColumnName)
        {
            //数据库连接
            if (_connectionConfig.IsAutoCloseConnection == false)
                if (_database.CheckStatus() == false)
                    throw new Exception("databse connect not open");
            if (_connectionConfig.IsAutoCloseConnection == true) _database.Open();

            Object result;

            if (filterDefinition != null)
                result = _database.GetSpeciaRecordValue(_databasename, _TableName,filterDefinition, ColumnName);
            else
                result = _database.GetSpeciaRecordValue(_databasename, _TableName, new BsonDocument(), ColumnName);

            if (_connectionConfig.IsAutoCloseConnection == true) _database.Close();

            return result;
        }



        /// <summary>
        /// get count by filter
        /// 得到指定条件的行计数数据
        /// </summary>
        /// <returns></returns>
        public Object Count()
        {
           
            //数据库连接
            if (_connectionConfig.IsAutoCloseConnection == false)
                if (_database.CheckStatus() == false)
                    throw new Exception("databse connect not open");
            if (_connectionConfig.IsAutoCloseConnection == true) _database.Open();

            Object result;

            if (filterDefinition != null)
                result = _database.GetDataCount(_databasename, _TableName,filterDefinition);
            else
                result = _database.GetDataCount(_databasename, _TableName, new BsonDocument());

            if (_connectionConfig.IsAutoCloseConnection == true) _database.Close();

            return result;
        }

        /// <summary>
        /// get recordnumber data by filter
        /// 得到指定数据的指定内容
        /// </summary>
        /// <param name="RecordNumber">记录数 Record number</param>
        /// <returns></returns>
        public IHashTable TopNumber(Int32 RecordNumber)
        {
            //数据库连接
            if (_connectionConfig.IsAutoCloseConnection == false)
                if (_database.CheckStatus() == false)
                    throw new Exception("databse connect not open");
            if (_connectionConfig.IsAutoCloseConnection == true) _database.Open();

            List<Hashtable> hr;

            if (filterDefinition != null)
                hr = _database.GetTopRecordsData(_databasename, _TableName,
                new BsonDocument(), RecordNumber);
            else
                hr = _database.GetTopRecordsData(_databasename, _TableName,
                    filterDefinition, RecordNumber);

            if (_connectionConfig.IsAutoCloseConnection == true) _database.Close();

            IHashTable DT = new IHashTable(hr);
            return DT;
        }

        /// <summary>
        /// get recordnumber data by filter
        /// 得到指定数据的指定内容
        /// </summary>
        /// <param name="StartNumber">开始行 Start line</param>
        /// <param name="RecordNumber">记录数 Record number</param>
        /// <returns></returns>
        public IHashTable Page(Int32 StartNumber, Int32 RecordNumber)
        {
            //数据库连接
            if (_connectionConfig.IsAutoCloseConnection == false)
                if (_database.CheckStatus() == false)
                    throw new Exception("databse connect not open");
            if (_connectionConfig.IsAutoCloseConnection == true) _database.Open();

            List<Hashtable> hr;

            if (filterDefinition != null)
                hr = _database.GetTopPageRecordsData(_databasename, _TableName,
                new BsonDocument(), StartNumber, RecordNumber);
            else
                hr = _database.GetTopPageRecordsData(_databasename, _TableName,
                    filterDefinition, StartNumber, RecordNumber);

            if (_connectionConfig.IsAutoCloseConnection == true) _database.Close();

            IHashTable DT = new IHashTable(hr);
            return DT;
        }



        /// <summary>
        /// Link table
        /// 关联表
        /// </summary>
     
        /// <param name="IncludeColumnName">包含列 include field</param>
        /// <param name="ExcludeColumnName">排除列 exclude field</param>
        /// <param name="ForeignTable">外联表名</param>
        /// <param name="LocalField">关联字段</param>
        /// <param name="ForeignField">外联字段</param>
        /// <param name="ForeignName">外联集合名</param>
        /// <returns></returns>
        public IHashTable TableLink(List<String> IncludeColumnName, List<String> ExcludeColumnName, String ForeignTable, String LocalField,
            String ForeignField, String ForeignName)
        {
            if (filterDefinition==null)
            {
                throw new Exception("filter cannot null");
            }

            //数据库连接
            if (_connectionConfig.IsAutoCloseConnection == false)
                if (_database.CheckStatus() == false)
                    throw new Exception("databse connect not open");
            if (_connectionConfig.IsAutoCloseConnection == true) _database.Open();
            List<Hashtable> hr = _database.TableLink(_databasename, _TableName,
               filterDefinition, IncludeColumnName, ExcludeColumnName
                , ForeignTable, LocalField, ForeignField, ForeignName);

            if (_connectionConfig.IsAutoCloseConnection == true) _database.Close();

            IHashTable DT = new IHashTable(hr);
            return DT;
        }

        /// <summary>
        /// get data by filter
        /// 得到指定条件的表中所有数据
        /// </summary>
        /// <param name="ColumnName">筛选字段名 Filed</param>
        /// <param name="Value">筛选值 Filter value</param>
        /// <returns></returns>
        public MongoGetData Where(String ColumnName, Object Value)
        {

            filterDefinition = MongoDBOP.GetFilterOP(ColumnName, Value, CommandComparison.Equals);

            return this;
        }



        /// <summary>
        /// get data by filter
        /// 得到指定条件的表中所有数据
        /// </summary>
        /// <param name="ColumnName">筛选字段名 Filed</param>
        /// <param name="Value">筛选值 Filter value</param>
        /// <param name="Comparison">筛选条件 Filter where</param>
        /// <returns></returns>
        public MongoGetData Where(String ColumnName, Object Value,
            CommandComparison Comparison)
        {

            filterDefinition = MongoDBOP.GetFilterOP(ColumnName, Value, Comparison);

            return this;
        }


        /// <summary>
        /// get data by filter
        /// 得到指定条件的表中所有数据
        /// </summary>
        /// <param name="ColumnName">筛选字段名 Filed</param>
        /// <param name="Value">筛选值 Filter value</param>
        /// <returns></returns>
        public MongoGetData Where(String[] ColumnName, Object[] Value)
        {
            if (ColumnName.Length != Value.Length)
            {
                throw new Exception("Column number not Equals Value number");
            }

            filterDefinition = MongoDBOP.GetWhere(ColumnName, Value);

            return this;
        }



        /// <summary>
        /// get data by filter
        /// 得到指定条件的表中所有数据
        /// </summary>
        /// <param name="ColumnName">筛选字段名 Filed</param>
        /// <param name="Comparison">筛选条件 Filter where</param>
        /// <param name="Value">筛选值 Filter value</param>
        /// <param name="relation">筛选关系 relation</param>
        /// <returns></returns>
        public MongoGetData Where(String[] ColumnName, CommandComparison[] Comparison,
            Object[] Value, WhereRelation[] relation)
        {
            if (ColumnName.Length != Value.Length && ColumnName.Length != Comparison.Length &&
                ColumnName.Length != relation.Length)
            {
                throw new Exception("Column number not Equals Value number");
            }


            filterDefinition = MongoDBOP.GetWhere(ColumnName, Value, Comparison, relation);

            return this;
        }



    }

}
