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
        /// use database name
        /// 切换数据库名称
        /// </summary>
        public string SetDatabaseName
        {
            set => _databasename = value;
            get => _databasename;
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
        /// 得到表中所有数据
        /// </summary>
        /// <param name="TableName"></param>
        /// <param name="OrderByName"></param>
        /// <param name="sortMode"></param>
        /// <returns></returns>
        public IDataTable Data(String TableName, String OrderByName = "",
            SortMode sortMode = SortMode.Ascending)
        {

            //数据库连接
            if (_connectionConfig.IsAutoCloseConnection == false)
                if (_database.CheckStatus() == false)
                    throw new Exception("databse connect not open");
            if (_connectionConfig.IsAutoCloseConnection == true) _database.Open();
            DataSet ds = _database.GetDataSet(_databasename, TableName, new BsonDocument(), OrderByName,
                sortMode);

            if (_connectionConfig.IsAutoCloseConnection == true) _database.Close();


            IDataTable DT = new IDataTable {table = ds.Tables[0]};
            return DT;
        }


        /// <summary>
        /// 得到指定条件的表中所有数据
        /// </summary>
        /// <param name="TableName"></param>
        /// <param name="ColumnName"></param>
        /// <param name="Value"></param>
        /// <param name="OrderByName"></param>
        /// <param name="sortMode"></param>
        /// <returns></returns>
        public IDataTable Data(String TableName, String ColumnName, Object Value,
            String OrderByName = "", SortMode sortMode = SortMode.Ascending)
        {



            //数据库连接
            if (_connectionConfig.IsAutoCloseConnection == false)
                if (_database.CheckStatus() == false)
                    throw new Exception("databse connect not open");
            if (_connectionConfig.IsAutoCloseConnection == true) _database.Open();

            DataSet ds = _database.GetDataSet(_databasename, TableName,
                MongoDBOP.GetFilterOP(ColumnName, Value, CommandComparison.Equals), OrderByName, sortMode);

            if (_connectionConfig.IsAutoCloseConnection == true) _database.Close();

            IDataTable DT = new IDataTable {table = ds.Tables[0]};
            return DT;
        }



        /// <summary>
        /// 得到指定条件的表中所有数据
        /// </summary>
        /// <param name="TableName"></param>
        /// <param name="ColumnName"></param>
        /// <param name="Value"></param>
        /// <param name="Comparison"></param>
        /// <param name="OrderByName"></param>
        /// <param name="sortMode"></param>
        /// <returns></returns>
        public IDataTable Data(String TableName, String ColumnName, Object Value,
            CommandComparison Comparison, String OrderByName = "", SortMode sortMode = SortMode.Ascending)
        {

            //数据库连接
            if (_connectionConfig.IsAutoCloseConnection == false)
                if (_database.CheckStatus() == false)
                    throw new Exception("databse connect not open");
            if (_connectionConfig.IsAutoCloseConnection == true) _database.Open();
            DataSet ds = _database.GetDataSet(_databasename, TableName,
                MongoDBOP.GetFilterOP(ColumnName, Value, Comparison), OrderByName, sortMode);

            if (_connectionConfig.IsAutoCloseConnection == true) _database.Close();

            IDataTable DT = new IDataTable { table = ds.Tables[0] };
            return DT;
        }


        /// <summary>
        /// 得到指定条件的表中所有数据
        /// </summary>
        /// <param name="TableName"></param>
        /// <param name="ColumnName"></param>
        /// <param name="Value"></param>
        /// <param name="OrderByName"></param>
        /// <param name="sortMode"></param>
        /// <returns></returns>
        public IDataTable Data(String TableName, String[] ColumnName, Object[] Value,
            String OrderByName = "", SortMode sortMode = SortMode.Ascending)
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
            DataSet ds = _database.GetDataSet(_databasename, TableName,
                MongoDBOP.GetWhere(ColumnName, Value), OrderByName, sortMode);

            if (_connectionConfig.IsAutoCloseConnection == true) _database.Close();

            IDataTable DT = new IDataTable { table = ds.Tables[0] };
            return DT;
        }



        /// <summary>
        /// 得到指定条件的表中所有数据
        /// </summary>
        /// <param name="TableName"></param>
        /// <param name="ColumnName"></param>
        /// <param name="Comparison"></param>
        /// <param name="Value"></param>
        /// <param name="relation"></param>
        /// <param name="OrderByName"></param>
        /// <param name="sortMode"></param>
        /// <returns></returns>
        public IDataTable Data(String TableName, String[] ColumnName, CommandComparison[] Comparison,
            Object[] Value, WhereRelation[] relation, String OrderByName = "", SortMode sortMode = SortMode.Ascending)
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
            DataSet ds = _database.GetDataSet(_databasename, TableName,
                MongoDBOP.GetWhere(ColumnName, Value, Comparison, relation), OrderByName, sortMode);

            if (_connectionConfig.IsAutoCloseConnection == true) _database.Close();

            IDataTable DT = new IDataTable { table = ds.Tables[0] };
            return DT;
        }

        /// <summary>
        /// 得到表中所有数据
        /// </summary>
        /// <param name="TableName"></param>
        /// <param name="OrderByName"></param>
        /// <param name="sortMode"></param>
        /// <returns></returns>
        public List<Hashtable> HashTable(String TableName, String OrderByName = "",
            SortMode sortMode = SortMode.Ascending)
        {

            //数据库连接
            if (_connectionConfig.IsAutoCloseConnection == false)
                if (_database.CheckStatus() == false)
                    throw new Exception("databse connect not open");
            if (_connectionConfig.IsAutoCloseConnection == true) _database.Open();
            List<Hashtable> ds = _database.GetHashtable(_databasename, TableName, new BsonDocument(),
                OrderByName, sortMode);

            if (_connectionConfig.IsAutoCloseConnection == true) _database.Close();


            return ds;
        }


        /// <summary>
        /// 得到指定条件的表中所有数据
        /// </summary>
        /// <param name="TableName"></param>
        /// <param name="ColumnName"></param>
        /// <param name="Value"></param>
        /// <param name="OrderByName"></param>
        /// <param name="sortMode"></param>
        /// <returns></returns>
        public List<Hashtable> HashTable(String TableName, String ColumnName, Object Value,
            String OrderByName = "", SortMode sortMode = SortMode.Ascending)
        {



            //数据库连接
            if (_connectionConfig.IsAutoCloseConnection == false)
                if (_database.CheckStatus() == false)
                    throw new Exception("databse connect not open");
            if (_connectionConfig.IsAutoCloseConnection == true) _database.Open();

            List<Hashtable> ds = _database.GetHashtable(_databasename, TableName,
                MongoDBOP.GetFilterOP(ColumnName, Value, CommandComparison.Equals), OrderByName, sortMode);

            if (_connectionConfig.IsAutoCloseConnection == true) _database.Close();

            return ds;
        }



        /// <summary>
        /// 得到指定条件的表中所有数据
        /// </summary>
        /// <param name="TableName"></param>
        /// <param name="ColumnName"></param>
        /// <param name="Value"></param>
        /// <param name="Comparison"></param>
        /// <param name="OrderByName"></param>
        /// <param name="sortMode"></param>
        /// <returns></returns>
        public List<Hashtable> HashTable(String TableName, String ColumnName, Object Value,
            CommandComparison Comparison, String OrderByName = "", SortMode sortMode = SortMode.Ascending)
        {

            //数据库连接
            if (_connectionConfig.IsAutoCloseConnection == false)
                if (_database.CheckStatus() == false)
                    throw new Exception("databse connect not open");
            if (_connectionConfig.IsAutoCloseConnection == true) _database.Open();
            List<Hashtable> ds = _database.GetHashtable(_databasename, TableName,
                MongoDBOP.GetFilterOP(ColumnName, Value, Comparison), OrderByName, sortMode);

            if (_connectionConfig.IsAutoCloseConnection == true) _database.Close();

            return ds;
        }


        /// <summary>
        /// 得到指定条件的表中所有数据
        /// </summary>
        /// <param name="TableName"></param>
        /// <param name="ColumnName"></param>
        /// <param name="Value"></param>
        /// <param name="OrderByName"></param>
        /// <param name="sortMode"></param>
        /// <returns></returns>
        public List<Hashtable> HashTable(String TableName, String[] ColumnName, Object[] Value,
            String OrderByName = "", SortMode sortMode = SortMode.Ascending)
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
            List<Hashtable> ds = _database.GetHashtable(_databasename, TableName,
                MongoDBOP.GetWhere(ColumnName, Value), OrderByName, sortMode);

            if (_connectionConfig.IsAutoCloseConnection == true) _database.Close();

            return ds;
        }


        /// <summary>
        /// 得到指定条件的表中所有数据
        /// </summary>
        /// <param name="TableName"></param>
        /// <param name="ColumnName"></param>
        /// <param name="Comparison"></param>
        /// <param name="Value"></param>
        /// <param name="relation"></param>
        /// <param name="OrderByName"></param>
        /// <param name="sortMode"></param>
        /// <returns></returns>
        public List<Hashtable> HashTable(String TableName, String[] ColumnName,
            CommandComparison[] Comparison, Object[] Value, WhereRelation[] relation, String OrderByName = "",
            SortMode sortMode = SortMode.Ascending)
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
            List<Hashtable> ds = _database.GetHashtable(_databasename, TableName,
                MongoDBOP.GetWhere(ColumnName, Value, Comparison, relation), OrderByName, sortMode);

            if (_connectionConfig.IsAutoCloseConnection == true) _database.Close();

            return ds;
        }


        /// <summary>
        /// 得到指定条件的表中所有数据
        /// </summary>
        /// <param name="TableName"></param>
        /// <param name="ColumnName"></param>
        /// <param name="Value"></param>
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
        /// 检查表中是否存在指定内容
        /// </summary>
        /// <param name="TableName"></param>
        /// <param name="ColumnName"></param>
        /// <param name="Comparison"></param>
        /// <param name="Value"></param>
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
        /// 检查表中是否存在指定内容
        /// </summary>
        /// <param name="TableName"></param>
        /// <param name="ColumnName"></param>
        /// <param name="Value"></param>
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
        /// 检查表中是否存在指定内容
        /// </summary>
        /// <param name="TableName"></param>
        /// <param name="ColumnName"></param>
        /// <param name="Comparison"></param>
        /// <param name="Value"></param>
        /// <param name="relation"></param>
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
        /// 得到指定条件的一个数据
        /// </summary>
        /// <param name="TableName"></param>
        /// <param name="ColumnName"></param>
        /// <param name="KeyColumnName"></param>
        /// <param name="KeyValue"></param>
        /// <returns></returns>
        public Object First(String TableName, String ColumnName, String KeyColumnName, Object KeyValue,
            String OrderByName = "", SortMode sortMode = SortMode.Ascending)
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
        /// 得到指定条件的一个数据
        /// </summary>
        /// <param name="TableName"></param>
        /// <param name="ColumnName"></param>
        /// <param name="KeyColumnName"></param>
        /// <param name="KeyValue"></param>
        /// <returns></returns>
        public Object First(String TableName, String ColumnName, String[] KeyColumnName,
            Object[] KeyValue, String OrderByName = "", SortMode sortMode = SortMode.Ascending)
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
        /// 得到指定条件的行计数数据
        /// </summary>
        /// <param name="TableName"></param>
        /// <param name="ColumnName"></param>
        /// <param name="KeyColumnName"></param>
        /// <param name="KeyValue"></param>
        /// <param name="CountNumber"></param>
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
        /// 得到指定条件的行计数数据
        /// </summary>
        /// <param name="TableName"></param>
        /// <param name="ColumnName"></param>
        /// <param name="KeyColumnName"></param>
        /// <param name="Comparison"></param>
        /// <param name="KeyValue"></param>
        /// <param name="relation"></param>
        /// <param name="CountNumber"></param>
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
        /// 得到指定条件的行计数数据
        /// </summary>
        /// <param name="TableName"></param>
        /// <param name="ColumnName"></param>
        /// <param name="CountNumber"></param>
        /// <returns></returns>
        public Object First(String TableName, String ColumnName, Boolean CountNumber)
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
        /// 得到指定数据的指定内容
        /// </summary>
        /// <param name="TableName"></param>
        /// <param name="ColumnName"></param>
        /// <param name="Value"></param>
        /// <param name="RecordNumber"></param>
        /// <param name="OrderByName"></param>
        /// <param name="sortMode"></param>
        /// <returns></returns>
        public IDataTable Data(String TableName, Int32 RecordNumber, String OrderByName = "",
            SortMode sortMode = SortMode.Ascending)
        {



            //数据库连接
            if (_connectionConfig.IsAutoCloseConnection == false)
                if (_database.CheckStatus() == false)
                    throw new Exception("databse connect not open");
            if (_connectionConfig.IsAutoCloseConnection == true) _database.Open();

            DataSet ds = _database.GetTopRecordsData(_databasename, TableName,
                new BsonDocument(), RecordNumber, OrderByName, sortMode);

            if (_connectionConfig.IsAutoCloseConnection == true) _database.Close();

            IDataTable DT = new IDataTable {table = ds.Tables[0]};
            return DT;
        }

        /// <summary>
        /// 得到指定数据的指定内容
        /// </summary>
        /// <param name="TableName"></param>
        /// <param name="ColumnName"></param>
        /// <param name="Value"></param>
        /// <param name="RecordNumber"></param>
        /// <param name="OrderByName"></param>
        /// <param name="sortMode"></param>
        /// <returns></returns>
        public IDataTable Data(String TableName, String ColumnName, Object Value, Int32 RecordNumber,
            String OrderByName = "", SortMode sortMode = SortMode.Ascending)
        {



            //数据库连接
            if (_connectionConfig.IsAutoCloseConnection == false)
                if (_database.CheckStatus() == false)
                    throw new Exception("databse connect not open");
            if (_connectionConfig.IsAutoCloseConnection == true) _database.Open();

            DataSet ds = _database.GetTopRecordsData(_databasename, TableName,
                MongoDBOP.GetFilterOP(ColumnName, Value, CommandComparison.Equals), RecordNumber, OrderByName, sortMode);

            if (_connectionConfig.IsAutoCloseConnection == true) _database.Close();

            IDataTable DT = new IDataTable {table = ds.Tables[0]};
            return DT;
        }



        /// <summary>
        /// 得到指定数据的指定内容
        /// </summary>
        /// <param name="TableName"></param>
        /// <param name="ColumnName"></param>
        /// <param name="Comparison"></param>
        /// <param name="Value"></param>
        /// <param name="RecordNumber"></param>
        /// <param name="OrderByName"></param>
        /// <param name="sortMode"></param>
        /// <returns></returns>
        public IDataTable Data(String TableName, String ColumnName, CommandComparison Comparison,
            Object Value, Int32 RecordNumber, String OrderByName = "", SortMode sortMode = SortMode.Ascending)
        {

            //数据库连接
            if (_connectionConfig.IsAutoCloseConnection == false)
                if (_database.CheckStatus() == false)
                    throw new Exception("databse connect not open");
            if (_connectionConfig.IsAutoCloseConnection == true) _database.Open();
            DataSet ds = _database.GetTopRecordsData(_databasename, TableName,
                MongoDBOP.GetFilterOP(ColumnName, Value, Comparison), RecordNumber, OrderByName, sortMode);

            if (_connectionConfig.IsAutoCloseConnection == true) _database.Close();

            IDataTable DT = new IDataTable {table = ds.Tables[0]};
            return DT;
        }


        /// <summary>
        /// 得到指定数据的指定内容
        /// </summary>
        /// <param name="TableName"></param>
        /// <param name="ColumnName"></param>
        /// <param name="Value"></param>
        /// <param name="RecordNumber"></param>
        /// <param name="OrderByName"></param>
        /// <param name="sortMode"></param>
        /// <returns></returns>
        public IDataTable Data(String TableName, String[] ColumnName, Object[] Value,
            Int32 RecordNumber, String OrderByName = "", SortMode sortMode = SortMode.Ascending)
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
            DataSet ds = _database.GetTopRecordsData(_databasename, TableName,
                MongoDBOP.GetWhere(ColumnName, Value), RecordNumber, OrderByName, sortMode);

            if (_connectionConfig.IsAutoCloseConnection == true) _database.Close();

            IDataTable DT = new IDataTable {table = ds.Tables[0]};
            return DT;
        }



        /// <summary>
        /// 得到指定数据的指定内容
        /// </summary>
        /// <param name="TableName"></param>
        /// <param name="ColumnName"></param>
        /// <param name="Comparison"></param>
        /// <param name="Value"></param>
        /// <param name="relation"></param>
        /// <param name="RecordNumber"></param>
        /// <param name="OrderByName"></param>
        /// <param name="sortMode"></param>
        /// <returns></returns>
        public IDataTable Data(String TableName, String[] ColumnName, CommandComparison[] Comparison,
            Object[] Value, WhereRelation[] relation, Int32 RecordNumber, String OrderByName = "",
            SortMode sortMode = SortMode.Ascending)
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
            DataSet ds = _database.GetTopRecordsData(_databasename, TableName,
                MongoDBOP.GetWhere(ColumnName, Value, Comparison, relation), RecordNumber, OrderByName, sortMode);

            if (_connectionConfig.IsAutoCloseConnection == true) _database.Close();

            IDataTable DT = new IDataTable {table = ds.Tables[0]};
            return DT;
        }

        /// <summary>
        /// 得到指定数据的指定内容
        /// </summary>
        /// <param name="TableName"></param>
        /// <param name="ColumnName"></param>
        /// <param name="Value"></param>
        /// <param name="RecordNumber"></param>
        /// <param name="OrderByName"></param>
        /// <param name="sortMode"></param>
        /// <returns></returns>
        public List<Hashtable> HashTable(String TableName, Int32 RecordNumber,
            String OrderByName = "", SortMode sortMode = SortMode.Ascending)
        {



            //数据库连接
            if (_connectionConfig.IsAutoCloseConnection == false)
                if (_database.CheckStatus() == false)
                    throw new Exception("databse connect not open");
            if (_connectionConfig.IsAutoCloseConnection == true) _database.Open();

            List<Hashtable> ds = _database.GetTopRecordsHashtable(_databasename, TableName,
                new BsonDocument(), RecordNumber, OrderByName, sortMode);

            if (_connectionConfig.IsAutoCloseConnection == true) _database.Close();

            return ds;
        }

        /// <summary>
        /// 得到指定数据的指定内容
        /// </summary>
        /// <param name="TableName"></param>
        /// <param name="ColumnName"></param>
        /// <param name="Value"></param>
        /// <param name="RecordNumber"></param>
        /// <param name="OrderByName"></param>
        /// <param name="sortMode"></param>
        /// <returns></returns>
        public List<Hashtable> HashTable(String TableName, String ColumnName, Object Value,
            Int32 RecordNumber, String OrderByName = "", SortMode sortMode = SortMode.Ascending)
        {



            //数据库连接
            if (_connectionConfig.IsAutoCloseConnection == false)
                if (_database.CheckStatus() == false)
                    throw new Exception("databse connect not open");
            if (_connectionConfig.IsAutoCloseConnection == true) _database.Open();

            List<Hashtable> ds = _database.GetTopRecordsHashtable(_databasename, TableName,
                MongoDBOP.GetFilterOP(ColumnName, Value, CommandComparison.Equals), RecordNumber, OrderByName, sortMode);

            if (_connectionConfig.IsAutoCloseConnection == true) _database.Close();

            return ds;
        }


        /// <summary>
        /// 得到指定数据的指定内容
        /// </summary>
        /// <param name="TableName"></param>
        /// <param name="ColumnName"></param>
        /// <param name="Comparison"></param>
        /// <param name="Value"></param>
        /// <param name="RecordNumber"></param>
        /// <param name="OrderByName"></param>
        /// <param name="sortMode"></param>
        /// <returns></returns>
        public List<Hashtable> HashTable(String TableName, String ColumnName,
            CommandComparison Comparison, Object Value, Int32 RecordNumber, String OrderByName = "",
            SortMode sortMode = SortMode.Ascending)
        {

            //数据库连接
            if (_connectionConfig.IsAutoCloseConnection == false)
                if (_database.CheckStatus() == false)
                    throw new Exception("databse connect not open");
            if (_connectionConfig.IsAutoCloseConnection == true) _database.Open();
            List<Hashtable> ds = _database.GetTopRecordsHashtable(_databasename, TableName,
                MongoDBOP.GetFilterOP(ColumnName, Value, Comparison), RecordNumber, OrderByName, sortMode);

            if (_connectionConfig.IsAutoCloseConnection == true) _database.Close();

            return ds;
        }

        /// <summary>
        /// 得到指定数据的指定内容
        /// </summary>
        /// <param name="TableName"></param>
        /// <param name="ColumnName"></param>
        /// <param name="Value"></param>
        /// <param name="RecordNumber"></param>
        /// <param name="OrderByName"></param>
        /// <param name="sortMode"></param>
        /// <returns></returns>
        public List<Hashtable> HashTable(String TableName, String[] ColumnName, Object[] Value,
            Int32 RecordNumber, String OrderByName = "", SortMode sortMode = SortMode.Ascending)
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
            List<Hashtable> ds = _database.GetTopRecordsHashtable(_databasename, TableName,
                MongoDBOP.GetWhere(ColumnName, Value), RecordNumber, OrderByName, sortMode);

            if (_connectionConfig.IsAutoCloseConnection == true) _database.Close();

            return ds;
        }


        /// <summary>
        /// 得到指定数据的指定内容
        /// </summary>
        /// <param name="TableName"></param>
        /// <param name="ColumnName"></param>
        /// <param name="Comparison"></param>
        /// <param name="Value"></param>
        /// <param name="relation"></param>
        /// <param name="RecordNumber"></param>
        /// <param name="OrderByName"></param>
        /// <param name="sortMode"></param>
        /// <returns></returns>
        public List<Hashtable> HashTable(String TableName, String[] ColumnName,
            CommandComparison[] Comparison, Object[] Value, WhereRelation[] relation, Int32 RecordNumber,
            String OrderByName = "", SortMode sortMode = SortMode.Ascending)
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
            List<Hashtable> ds = _database.GetTopRecordsHashtable(_databasename, TableName,
                MongoDBOP.GetWhere(ColumnName, Value, Comparison, relation), RecordNumber, OrderByName, sortMode);

            if (_connectionConfig.IsAutoCloseConnection == true) _database.Close();

            return ds;
        }

        /// <summary>
        /// 得到指定数据的分页指定内容
        /// </summary>
        /// <param name="TableName"></param>
        /// <param name="ColumnName"></param>
        /// <param name="Value"></param>
        /// <param name="StartNumber"></param>
        /// <param name="RecordNumber"></param>
        /// <param name="OrderByName"></param>
        /// <param name="sortMode"></param>
        /// <returns></returns>
        public IDataTable Data(String TableName, Int32 StartNumber, Int32 RecordNumber,
            String OrderByName = "", SortMode sortMode = SortMode.Ascending)
        {



            //数据库连接
            if (_connectionConfig.IsAutoCloseConnection == false)
                if (_database.CheckStatus() == false)
                    throw new Exception("databse connect not open");
            if (_connectionConfig.IsAutoCloseConnection == true) _database.Open();

            DataSet ds = _database.GetTopPageRecordsData(_databasename, TableName,
                new BsonDocument(), StartNumber, RecordNumber, OrderByName, sortMode);

            if (_connectionConfig.IsAutoCloseConnection == true) _database.Close();

            IDataTable DT = new IDataTable {table = ds.Tables[0]};
            return DT;
        }

        /// <summary>
        /// 得到指定数据的分页指定内容
        /// </summary>
        /// <param name="TableName"></param>
        /// <param name="ColumnName"></param>
        /// <param name="Value"></param>
        /// <param name="StartNumber"></param>
        /// <param name="RecordNumber"></param>
        /// <param name="OrderByName"></param>
        /// <param name="sortMode"></param>
        /// <returns></returns>
        public IDataTable Data(String TableName, String ColumnName, Object Value,
            Int32 StartNumber, Int32 RecordNumber, String OrderByName = "", SortMode sortMode = SortMode.Ascending)
        {



            //数据库连接
            if (_connectionConfig.IsAutoCloseConnection == false)
                if (_database.CheckStatus() == false)
                    throw new Exception("databse connect not open");
            if (_connectionConfig.IsAutoCloseConnection == true) _database.Open();

            DataSet ds = _database.GetTopPageRecordsData(_databasename, TableName,
                MongoDBOP.GetFilterOP(ColumnName, Value, CommandComparison.Equals), StartNumber, RecordNumber, OrderByName,
                sortMode);

            if (_connectionConfig.IsAutoCloseConnection == true) _database.Close();

            IDataTable DT = new IDataTable {table = ds.Tables[0]};
            return DT;
        }


        /// <summary>
        /// 得到指定数据的分页指定内容
        /// </summary>
        /// <param name="TableName"></param>
        /// <param name="ColumnName"></param>
        /// <param name="Comparison"></param>
        /// <param name="Value"></param>
        /// <param name="StartNumber"></param>
        /// <param name="RecordNumber"></param>
        /// <param name="OrderByName"></param>
        /// <param name="sortMode"></param>
        /// <returns></returns>
        public IDataTable Data(String TableName, String ColumnName, CommandComparison Comparison,
            Object Value, Int32 StartNumber, Int32 RecordNumber, String OrderByName = "",
            SortMode sortMode = SortMode.Ascending)
        {

            //数据库连接
            if (_connectionConfig.IsAutoCloseConnection == false)
                if (_database.CheckStatus() == false)
                    throw new Exception("databse connect not open");
            if (_connectionConfig.IsAutoCloseConnection == true) _database.Open();
            DataSet ds = _database.GetTopPageRecordsData(_databasename, TableName,
                MongoDBOP.GetFilterOP(ColumnName, Value, Comparison), StartNumber, RecordNumber, OrderByName, sortMode);

            if (_connectionConfig.IsAutoCloseConnection == true) _database.Close();

            IDataTable DT = new IDataTable {table = ds.Tables[0]};
            return DT;
        }


        /// <summary>
        /// 得到指定数据的分页指定内容
        /// </summary>
        /// <param name="TableName"></param>
        /// <param name="ColumnName"></param>
        /// <param name="Value"></param>
        /// <param name="StartNumber"></param>
        /// <param name="RecordNumber"></param>
        /// <param name="OrderByName"></param>
        /// <param name="sortMode"></param>
        /// <returns></returns>
        public IDataTable Data(String TableName, String[] ColumnName, Object[] Value,
            Int32 StartNumber, Int32 RecordNumber, String OrderByName = "", SortMode sortMode = SortMode.Ascending)
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
            DataSet ds = _database.GetTopPageRecordsData(_databasename, TableName,
                MongoDBOP.GetWhere(ColumnName, Value), StartNumber, RecordNumber, OrderByName, sortMode);

            if (_connectionConfig.IsAutoCloseConnection == true) _database.Close();

            IDataTable DT = new IDataTable {table = ds.Tables[0]};
            return DT;
        }



        /// <summary>
        /// 得到指定数据的分页指定内容
        /// </summary>
        /// <param name="TableName"></param>
        /// <param name="ColumnName"></param>
        /// <param name="Comparison"></param>
        /// <param name="Value"></param>
        /// <param name="relation"></param>
        /// <param name="StartNumber"></param>
        /// <param name="RecordNumber"></param>
        /// <param name="OrderByName"></param>
        /// <param name="sortMode"></param>
        /// <returns></returns>
        public IDataTable Data(String TableName, String[] ColumnName,
            CommandComparison[] Comparison, Object[] Value, WhereRelation[] relation, Int32 StartNumber,
            Int32 RecordNumber, String OrderByName = "", SortMode sortMode = SortMode.Ascending)
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
            DataSet ds = _database.GetTopPageRecordsData(_databasename, TableName,
                MongoDBOP.GetWhere(ColumnName, Value, Comparison, relation), StartNumber, RecordNumber, OrderByName, sortMode);

            if (_connectionConfig.IsAutoCloseConnection == true) _database.Close();

            IDataTable DT = new IDataTable {table = ds.Tables[0]};
            return DT;
        }

        /// <summary>
        /// 得到指定数据的分页指定内容
        /// </summary>
        /// <param name="TableName"></param>
        /// <param name="ColumnName"></param>
        /// <param name="Value"></param>
        /// <param name="StartNumber"></param>
        /// <param name="RecordNumber"></param>
        /// <param name="OrderByName"></param>
        /// <param name="sortMode"></param>
        /// <returns></returns>
        public List<Hashtable> HashTable(String TableName, Int32 StartNumber,
            Int32 RecordNumber, String OrderByName = "", SortMode sortMode = SortMode.Ascending)
        {



            //数据库连接
            if (_connectionConfig.IsAutoCloseConnection == false)
                if (_database.CheckStatus() == false)
                    throw new Exception("databse connect not open");
            if (_connectionConfig.IsAutoCloseConnection == true) _database.Open();

            List<Hashtable> ds = _database.GetTopPageRecordsHashtable(_databasename, TableName,
                new BsonDocument(), StartNumber, RecordNumber, OrderByName, sortMode);

            if (_connectionConfig.IsAutoCloseConnection == true) _database.Close();

            return ds;
        }

        /// <summary>
        /// 得到指定数据的分页指定内容
        /// </summary>
        /// <param name="TableName"></param>
        /// <param name="ColumnName"></param>
        /// <param name="Value"></param>
        /// <param name="StartNumber"></param>
        /// <param name="RecordNumber"></param>
        /// <param name="OrderByName"></param>
        /// <param name="sortMode"></param>
        /// <returns></returns>
        public List<Hashtable> HashTable(String TableName, String ColumnName, Object Value,
            Int32 StartNumber, Int32 RecordNumber, String OrderByName = "", SortMode sortMode = SortMode.Ascending)
        {


            //数据库连接
            if (_connectionConfig.IsAutoCloseConnection == false)
                if (_database.CheckStatus() == false)
                    throw new Exception("databse connect not open");
            if (_connectionConfig.IsAutoCloseConnection == true) _database.Open();
            List<Hashtable> ds = _database.GetTopPageRecordsHashtable(_databasename, TableName,
                MongoDBOP.GetFilterOP(ColumnName, Value, CommandComparison.Equals), StartNumber, RecordNumber, OrderByName,
                sortMode);

            if (_connectionConfig.IsAutoCloseConnection == true) _database.Close();

            return ds;
        }



        /// <summary>
        /// 得到指定数据的分页指定内容
        /// </summary>
        /// <param name="TableName"></param>
        /// <param name="ColumnName"></param>
        /// <param name="Comparison"></param>
        /// <param name="Value"></param>
        /// <param name="StartNumber"></param>
        /// <param name="RecordNumber"></param>
        /// <param name="OrderByName"></param>
        /// <param name="sortMode"></param>
        /// <returns></returns>
        public List<Hashtable> HashTable(String TableName, String ColumnName,
            CommandComparison Comparison, Object Value, Int32 StartNumber, Int32 RecordNumber, String OrderByName = "",
            SortMode sortMode = SortMode.Ascending)
        {

            //数据库连接
            if (_connectionConfig.IsAutoCloseConnection == false)
                if (_database.CheckStatus() == false)
                    throw new Exception("databse connect not open");
            if (_connectionConfig.IsAutoCloseConnection == true) _database.Open();
            List<Hashtable> ds = _database.GetTopPageRecordsHashtable(_databasename, TableName,
                MongoDBOP.GetFilterOP(ColumnName, Value, Comparison), StartNumber, RecordNumber, OrderByName, sortMode);

            if (_connectionConfig.IsAutoCloseConnection == true) _database.Close();

            return ds;
        }


        /// <summary>
        /// 得到指定数据的分页指定内容
        /// </summary>
        /// <param name="TableName"></param>
        /// <param name="ColumnName"></param>
        /// <param name="Value"></param>
        /// <param name="StartNumber"></param>
        /// <param name="RecordNumber"></param>
        /// <param name="OrderByName"></param>
        /// <param name="sortMode"></param>
        /// <returns></returns>
        public List<Hashtable> HashTable(String TableName, String[] ColumnName,
            Object[] Value, Int32 StartNumber, Int32 RecordNumber, String OrderByName = "",
            SortMode sortMode = SortMode.Ascending)
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
            List<Hashtable> ds = _database.GetTopPageRecordsHashtable(_databasename, TableName,
                MongoDBOP.GetWhere(ColumnName, Value), StartNumber, RecordNumber, OrderByName, sortMode);

            if (_connectionConfig.IsAutoCloseConnection == true) _database.Close();

            return ds;
        }



        /// <summary>
        /// 得到指定数据的分页指定内容
        /// </summary>
        /// <param name="TableName"></param>
        /// <param name="ColumnName"></param>
        /// <param name="Comparison"></param>
        /// <param name="Value"></param>
        /// <param name="relation"></param>
        /// <param name="StartNumber"></param>
        /// <param name="RecordNumber"></param>
        /// <param name="OrderByName"></param>
        /// <param name="sortMode"></param>
        /// <returns></returns>
        public List<Hashtable> HashTable(String TableName, String[] ColumnName,
            CommandComparison[] Comparison, Object[] Value, WhereRelation[] relation, Int32 StartNumber,
            Int32 RecordNumber, String OrderByName = "", SortMode sortMode = SortMode.Ascending)
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
            List<Hashtable> ds = _database.GetTopPageRecordsHashtable(_databasename, TableName,
                MongoDBOP.GetWhere(ColumnName, Value, Comparison, relation), StartNumber, RecordNumber, OrderByName, sortMode);

            if (_connectionConfig.IsAutoCloseConnection == true) _database.Close();

            return ds;
        }


        /// <summary>
        /// 得到表中一列满足条件的数据
        /// </summary>
        /// <param name="TableName"></param>
        /// <param name="ColumnName"></param>
        /// <param name="OrderByName"></param>
        /// <param name="sortMode"></param>
        /// <returns></returns>
        public IDataTable HashTable(String TableName, List<String> IncludeColumnName,
            List<String> ExcludeColumnName, String OrderByName = "", SortMode sortMode = SortMode.Ascending)
        {

            //数据库连接
            if (_connectionConfig.IsAutoCloseConnection == false)
                if (_database.CheckStatus() == false)
                    throw new Exception("databse connect not open");
            if (_connectionConfig.IsAutoCloseConnection == true) _database.Open();
            DataSet ds = _database.GetColumnDataSet(_databasename, TableName, IncludeColumnName,
                ExcludeColumnName,
                new BsonDocument(), OrderByName, sortMode);


            if (_connectionConfig.IsAutoCloseConnection == true) _database.Close();

            IDataTable DT = new IDataTable {table = ds.Tables[0]};
            return DT;
        }


        /// <summary>
        /// 得到表中一列满足条件的数据
        /// </summary>
        /// <param name="TableName"></param>
        /// <param name="ColumnName"></param>
        /// <param name="MatchColumn"></param>
        /// <param name="MatchValue"></param>
        /// <param name="OrderByName"></param>
        /// <param name="sortMode"></param>
        /// <returns></returns>
        public IDataTable Data(String TableName, List<String> IncludeColumnName,
            List<String> ExcludeColumnName, String MatchColumn, Object MatchValue, String OrderByName = "",
            SortMode sortMode = SortMode.Ascending)
        {


            //数据库连接
            if (_connectionConfig.IsAutoCloseConnection == false)
                if (_database.CheckStatus() == false)
                    throw new Exception("databse connect not open");
            if (_connectionConfig.IsAutoCloseConnection == true) _database.Open();
            DataSet ds = _database.GetColumnDataSet(_databasename, TableName, IncludeColumnName,
                ExcludeColumnName,
                MongoDBOP.GetFilterOP(MatchColumn, MatchValue, CommandComparison.Equals), OrderByName, sortMode);

            if (_connectionConfig.IsAutoCloseConnection == true) _database.Close();

            IDataTable DT = new IDataTable {table = ds.Tables[0]};
            return DT;
        }



        /// <summary>
        /// 得到表中一列满足条件的数据
        /// </summary>
        /// <param name="TableName"></param>
        /// <param name="ColumnName"></param>
        /// <param name="MatchColumn"></param>
        /// <param name="Comparison"></param>
        /// <param name="MatchValue"></param>
        /// <param name="OrderByName"></param>
        /// <param name="sortMode"></param>
        /// <returns></returns>
        public IDataTable Data(String TableName, List<String> IncludeColumnName,
            List<String> ExcludeColumnName, String MatchColumn, CommandComparison Comparison, Object MatchValue,
            String OrderByName = "", SortMode sortMode = SortMode.Ascending)
        {

            //数据库连接
            if (_connectionConfig.IsAutoCloseConnection == false)
                if (_database.CheckStatus() == false)
                    throw new Exception("databse connect not open");
            if (_connectionConfig.IsAutoCloseConnection == true) _database.Open();
            DataSet ds = _database.GetColumnDataSet(_databasename, TableName, IncludeColumnName,
                ExcludeColumnName,
                MongoDBOP.GetFilterOP(MatchColumn, MatchValue, Comparison), OrderByName, sortMode);

            if (_connectionConfig.IsAutoCloseConnection == true) _database.Close();

            IDataTable DT = new IDataTable {table = ds.Tables[0]};
            return DT;
        }


        /// <summary>
        /// 得到表中一列满足条件的数据
        /// </summary>
        /// <param name="TableName"></param>
        /// <param name="ColumnName"></param>
        /// <param name="MatchColumn"></param>
        /// <param name="MatchValue"></param>
        /// <param name="OrderByName"></param>
        /// <param name="sortMode"></param>
        /// <returns></returns>
        public IDataTable Data(String TableName, List<String> IncludeColumnName,
            List<String> ExcludeColumnName, String[] MatchColumn, Object[] MatchValue, String OrderByName = "",
            SortMode sortMode = SortMode.Ascending)
        {


            if (MatchColumn.Length != MatchValue.Length)
            {
                throw new Exception("Column number not Equals Value number");
            }

            //数据库连接
            if (_connectionConfig.IsAutoCloseConnection == false)
                if (_database.CheckStatus() == false)
                    throw new Exception("databse connect not open");
            if (_connectionConfig.IsAutoCloseConnection == true) _database.Open();
            DataSet ds = _database.GetColumnDataSet(_databasename, TableName, IncludeColumnName,
                ExcludeColumnName,
                MongoDBOP.GetWhere(MatchColumn, MatchValue), OrderByName, sortMode);

            if (_connectionConfig.IsAutoCloseConnection == true) _database.Close();

            IDataTable DT = new IDataTable {table = ds.Tables[0]};
            return DT;
        }



        /// <summary>
        /// 得到表中一列满足条件的数据
        /// </summary>
        /// <param name="TableName"></param>
        /// <param name="ColumnName"></param>
        /// <param name="MatchColumn"></param>
        /// <param name="Comparison"></param>
        /// <param name="MatchValue"></param>
        /// <param name="relation"></param>
        /// <param name="OrderByName"></param>
        /// <param name="sortMode"></param>
        /// <returns></returns>
        public IDataTable Data(String TableName, List<String> IncludeColumnName,
            List<String> ExcludeColumnName, String[] MatchColumn, CommandComparison[] Comparison, Object[] MatchValue,
            WhereRelation[] relation, String OrderByName = "", SortMode sortMode = SortMode.Ascending)
        {


            if (MatchColumn.Length != MatchValue.Length && MatchColumn.Length != Comparison.Length &&
                MatchColumn.Length != relation.Length)
            {
                throw new Exception("Column number not Equals Value number");
            }

            //数据库连接
            if (_connectionConfig.IsAutoCloseConnection == false)
                if (_database.CheckStatus() == false)
                    throw new Exception("databse connect not open");
            if (_connectionConfig.IsAutoCloseConnection == true) _database.Open();
            DataSet ds = _database.GetColumnDataSet(_databasename, TableName, IncludeColumnName,
                ExcludeColumnName,
                MongoDBOP.GetWhere(MatchColumn, MatchValue, Comparison, relation), OrderByName, sortMode);

            if (_connectionConfig.IsAutoCloseConnection == true) _database.Close();

            IDataTable DT = new IDataTable {table = ds.Tables[0]};
            return DT;
        }

    }

}
