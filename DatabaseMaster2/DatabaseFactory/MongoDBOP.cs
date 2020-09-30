using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using DatabaseMaster;
using System.Data;
using System.Collections;
using MongoDB.Bson;
using MongoDB.Driver;

namespace DatabaseLayer
{
    public class MongoDBOP
    {
        private static MongoDBDatabase database;
        private static Boolean IsOpened=false;

        /// <summary>
        /// 查询条件
        /// </summary>
        /// <param name="ColumnName"></param>
        /// <param name="Value"></param>
        /// <param name="comparison"></param>
        /// <returns></returns>
        private static FilterDefinition<BsonDocument> GetFilterOP(String ColumnName, Object Value, DatabaseMaster.CommandComparison comparison)
        {
            switch ((int)comparison)
            {
                case 1:
                    return Builders<BsonDocument>.Filter.Eq(ColumnName, GetValueType(Value));
                case 2:
                    return Builders<BsonDocument>.Filter.Ne(ColumnName, GetValueType(Value));
                case 5:
                    return Builders<BsonDocument>.Filter.Gt(ColumnName, GetValueType(Value));
                case 6:
                    return Builders<BsonDocument>.Filter.Gte(ColumnName, GetValueType(Value));
                case 7:
                    return Builders<BsonDocument>.Filter.Lt(ColumnName, GetValueType(Value));
                case 8:
                    return Builders<BsonDocument>.Filter.Lte(ColumnName, GetValueType(Value));
                case 13:
                    return Builders<BsonDocument>.Filter.Eq(ColumnName, new ObjectId(Value.ToString()));
                default:
                    return new BsonDocument();
            }
        }

        /// <summary>
        /// 数据类型匹配
        /// </summary>
        /// <param name="Value"></param>
        /// <returns></returns>
        private static Object GetValueType(Object Value)
        {
            String type = Value.GetType().ToString();

            switch (type)
            {
                case "System.DateTime":
                    return Convert.ToString(Value);
                case "System.String":
                    return Convert.ToString(Value);
                default:
                    return Value;
            }
        }


        /// <summary>
        /// 多种查询条件组合
        /// </summary>
        /// <param name="ColumnName"></param>
        /// <param name="Value"></param>
        /// <returns></returns>
        private static FilterDefinition<BsonDocument> GetWhere(String[] ColumnName, Object[] Value)
        {
            FilterDefinition<BsonDocument> definition = new BsonDocument();

            for (int i = 0; i < ColumnName.Length; i++)
            {

                FilterDefinition<BsonDocument> def = GetFilterOP(ColumnName[i], Value[i], CommandComparison.Equals);

                definition = definition & def;

            }

            return definition;
        }

        /// <summary>
        /// 多种查询条件组合
        /// </summary>
        /// <param name="ColumnName"></param>
        /// <param name="Value"></param>
        /// <param name="comparison"></param>
        /// <param name="relation"></param>
        /// <returns></returns>
        private static FilterDefinition<BsonDocument> GetWhere(String[] ColumnName, Object[] Value, DatabaseMaster.CommandComparison[] comparison,WhereRelation[] relation)
        {
            FilterDefinition<BsonDocument> definition = new BsonDocument();

            for (int i = 0; i < ColumnName.Length; i++)
            {
               
                FilterDefinition<BsonDocument> def = GetFilterOP(ColumnName[i], Value[i],comparison[i]);


                if (relation[i]==WhereRelation.And)
                    definition = definition & def;
                else
                    definition = definition | def;


            }

            return definition;
        }

        /// <summary>
        /// 将HashTable转为Jason
        /// </summary>
        /// <param name="hr"></param>
        /// <returns></returns>
        private static string HashtableToJson(Hashtable hr)
        {
            string json = "{";
            foreach (DictionaryEntry row in hr)
            {
                try
                {
                    string key = "\"" + row.Key + "\":";
                    if (row.Value is Hashtable)
                    {
                        Hashtable t = (Hashtable)row.Value;
                        if (t.Count > 0)
                        {
                            json += key + HashtableToJson(t) + ",";
                        }
                        else { json += key + "{},"; }
                    }
                    else if (row.Value.ToString().StartsWith("[") && row.Value.ToString().EndsWith("]"))
                    {
                        string value = row.Value.ToString() + ",";
                        json += key + value;
                    }
                    else
                    {
                        string value;
                        if (row.Value.GetType().ToString().Equals("System.String"))
                        {
                            if (row.Value.ToString().StartsWith("ISODate(")&& row.Value.ToString().EndsWith(")"))
                                value = "ISODate(\"" + BsonDateTime.Create(row.Value.ToString().Replace("ISODate(","").Replace(")","")) + "\"),";
                            else if (row.Value.ToString().Equals("new Date()"))
                            {
                                value = row.Value.ToString() + ",";
                            }
                            else if (row.Value.ToString().StartsWith("ObjectId(") && row.Value.ToString().EndsWith(")"))
                            {
                                value = row.Value.ToString() + ",";
                            }
                            else
                                value = "\"" + row.Value.ToString() + "\",";
                        }
                        else if (row.Value.GetType().ToString().Equals("System.DateTime"))
                        {
                            value = "\"" + row.Value.ToString() + "\",";
                        }
                        else
                        {
                            value = row.Value.ToString() + ",";
                        }
                        json += key + value;
                    }
                }
                catch { }
            }

            //  json = MyString.ClearEndChar(json);  


            json = json.Remove(json.Length-1) + "}";
            return json;
        }

       
        /// <summary>
        /// 打开数据库连接
        /// </summary>
        public static void Open()
        {
            database = new MongoDBDatabase(DatabaseInit.ConnectName, DatabaseInit.EncryptType);
            database.Open();
            IsOpened = true;
        }

        /// <summary>
        /// 关闭数据库连接
        /// </summary>
        public static void Close()
        {
            database.Close();
            IsOpened = false;
        }

        /// <summary>
        /// Start Transaction
        /// </summary>
        public static void StartTransaction()
        {
            if (IsOpened == false)
            {
                throw new Exception("Database not open,call Open()");
            }

            database.StartTransaction();
        }

        /// <summary>
        /// Commit Transaction
        /// </summary>
        public static void CommitTransaction()
        {
            if (IsOpened == false)
            {
                throw new Exception("Database not open,call Open()");
            }

            database.CommitTransaction();
        }

        /// <summary>
        /// CloseTransaction
        /// </summary>
        public static void CloseTransaction()
        {
            if (IsOpened == false)
            {
                throw new Exception("Database not open,call Open()");
            }

            database.CloseTransaction();
        }

        /// <summary>
        /// 检查数据库连接
        /// </summary>
        /// <returns></returns>
        public static Boolean CheckDatabaseConnect()
        {
            try
            {
                if (IsOpened==false)
                {
                    return false;
                }

                //数据库连接
                Int64 count = Convert.ToInt32(database.CheckConnect());

                if (count == 0)
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
        /// 生成唯一标识号
        /// </summary>
        /// <param name="ObjectIDFormat"></param>
        /// <returns></returns>
        public static String GetNewGUIDString(Boolean ObjectIDFormat = false)
        {
            if (ObjectIDFormat)
                return Guid.NewGuid().ToString("N");
            else
                return Guid.NewGuid().ToString();
        }

        /// <summary>
        /// 生成唯一标识号
        /// </summary>
        /// <param name="ObjectIDFormat"></param>
        /// <returns></returns>
        public static String GetNewObjectID(Boolean ObjectIDFormat=false)
        {
            if (ObjectIDFormat)
                return "ObjectId(\"" + ObjectId.GenerateNewId().ToString()+"\")";
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
        public static DataTable GetAllDataTable(String TableName, String OrderByName="", SortMode sortMode=SortMode.Ascending)
        {
            if (IsOpened == false)
            {
                throw new Exception("Database not open,call Open()");
            }
            DataSet ds = database.GetDataSet(DatabaseInit.DatabaseName, TableName, new BsonDocument(), OrderByName, sortMode);
           

            return ds.Tables[0];
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
        public static DataTable GetAllDataTable(String TableName, String ColumnName, Object Value, String OrderByName = "", SortMode sortMode = SortMode.Ascending)
        {

            if (IsOpened == false)
            {
                throw new Exception("Database not open,call Open()");
            }



            DataSet ds = database.GetDataSet(DatabaseInit.DatabaseName, TableName,
                 GetFilterOP(ColumnName, Value, CommandComparison.Equals), OrderByName, sortMode);

            return ds.Tables[0];
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
        public static DataTable GetAllDataTable(String TableName, String ColumnName, Object Value, DatabaseMaster.CommandComparison Comparison, String OrderByName = "", SortMode sortMode = SortMode.Ascending)
        {
            if (IsOpened == false)
            {
                throw new Exception("Database not open,call Open()");
            }
            DataSet ds = database.GetDataSet(DatabaseInit.DatabaseName, TableName,
                 GetFilterOP(ColumnName, Value, Comparison), OrderByName, sortMode);
            return ds.Tables[0];
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
        public static DataTable GetAllDataTable(String TableName, String[] ColumnName, Object[] Value, String OrderByName = "", SortMode sortMode = SortMode.Ascending)
        {

            if (IsOpened == false)
            {
                throw new Exception("Database not open,call Open()");
            }

            if (ColumnName.Length != Value.Length)
            {
                throw new Exception("Column number not Equals Value number");
            }

            DataSet ds = database.GetDataSet(DatabaseInit.DatabaseName, TableName,
                 GetWhere(ColumnName, Value), OrderByName, sortMode);

            return ds.Tables[0];
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
        public static DataTable GetAllDataTable(String TableName, String[] ColumnName, DatabaseMaster.CommandComparison[] Comparison, Object[] Value, WhereRelation[] relation, String OrderByName = "", SortMode sortMode = SortMode.Ascending)
        {

            if (IsOpened == false)
            {
                throw new Exception("Database not open,call Open()");
            }

            if (ColumnName.Length != Value.Length && ColumnName.Length != Comparison.Length && ColumnName.Length != relation.Length)
            {
                throw new Exception("Column number not Equals Value number");
            }

            DataSet ds = database.GetDataSet(DatabaseInit.DatabaseName, TableName,
                 GetWhere(ColumnName, Value, Comparison, relation), OrderByName, sortMode);

            return ds.Tables[0];
        }

        /// <summary>
        /// 得到表中所有数据
        /// </summary>
        /// <param name="TableName"></param>
        /// <param name="OrderByName"></param>
        /// <param name="sortMode"></param>
        /// <returns></returns>
        public static List<Hashtable> GetAllDataHashTable(String TableName, String OrderByName = "", SortMode sortMode = SortMode.Ascending)
        {
            if (IsOpened == false)
            {
                throw new Exception("Database not open,call Open()");
            }
            List<Hashtable> ds = database.GetHashtable(DatabaseInit.DatabaseName, TableName, new BsonDocument(), OrderByName, sortMode);


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
        public static List<Hashtable> GetAllDataHashTable(String TableName, String ColumnName, Object Value, String OrderByName = "", SortMode sortMode = SortMode.Ascending)
        {

            if (IsOpened == false)
            {
                throw new Exception("Database not open,call Open()");
            }



            List<Hashtable> ds = database.GetHashtable(DatabaseInit.DatabaseName, TableName,
                 GetFilterOP(ColumnName, Value, CommandComparison.Equals), OrderByName, sortMode);

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
        public static List<Hashtable> GetAllDataHashTable(String TableName, String ColumnName,  Object Value, DatabaseMaster.CommandComparison Comparison, String OrderByName = "", SortMode sortMode = SortMode.Ascending)
        {
            if (IsOpened == false)
            {
                throw new Exception("Database not open,call Open()");
            }
            List<Hashtable> ds = database.GetHashtable(DatabaseInit.DatabaseName, TableName,
                 GetFilterOP(ColumnName, Value, Comparison), OrderByName, sortMode);
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
        public static List<Hashtable> GetAllDataHashTable(String TableName, String[] ColumnName, Object[] Value, String OrderByName = "", SortMode sortMode = SortMode.Ascending)
        {

            if (IsOpened == false)
            {
                throw new Exception("Database not open,call Open()");
            }

            if (ColumnName.Length != Value.Length)
            {
                throw new Exception("Column number not Equals Value number");
            }

            List<Hashtable> ds = database.GetHashtable(DatabaseInit.DatabaseName, TableName,
                 GetWhere(ColumnName, Value), OrderByName, sortMode);

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
        public static List<Hashtable> GetAllDataHashTable(String TableName, String[] ColumnName, DatabaseMaster.CommandComparison[] Comparison, Object[] Value, WhereRelation[] relation, String OrderByName = "", SortMode sortMode = SortMode.Ascending)
        {

            if (IsOpened == false)
            {
                throw new Exception("Database not open,call Open()");
            }

            if (ColumnName.Length != Value.Length && ColumnName.Length != Comparison.Length && ColumnName.Length != relation.Length)
            {
                throw new Exception("Column number not Equals Value number");
            }

            List<Hashtable> ds = database.GetHashtable(DatabaseInit.DatabaseName, TableName,
                 GetWhere(ColumnName, Value, Comparison, relation), OrderByName, sortMode);

            return ds;
        }


        /// <summary>
        /// 得到指定条件的表中所有数据
        /// </summary>
        /// <param name="TableName"></param>
        /// <param name="ColumnName"></param>
        /// <param name="Value"></param>
        /// <returns></returns>
        public static Boolean CheckSpecifiedValueExist(String TableName, String ColumnName, Object Value)
        {
            if (IsOpened == false)
            {
                throw new Exception("Database not open,call Open()");
            }


            long Count = database.GetDataCount(DatabaseInit.DatabaseName, TableName,
               GetFilterOP(ColumnName, Value, CommandComparison.Equals));

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
        public static Boolean CheckSpecifiedValueExist(String TableName, String ColumnName, DatabaseMaster.CommandComparison Comparison, Object Value)
        {
            if (IsOpened == false)
            {
                throw new Exception("Database not open,call Open()");
            }


            long Count = database.GetDataCount(DatabaseInit.DatabaseName, TableName,
               GetFilterOP(ColumnName, Value, Comparison));

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
        public static Boolean CheckSpecifiedValueExist(String TableName, String[] ColumnName, Object[] Value)
        {

            if (IsOpened == false)
            {
                throw new Exception("Database not open,call Open()");
            }

            if (ColumnName.Length != Value.Length)
            {
                throw new Exception("Column number not Equals Value number");
            }

            long Count = database.GetDataCount(DatabaseInit.DatabaseName, TableName,
               GetWhere(ColumnName, Value));

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
        public static Boolean CheckSpecifiedValueExist(String TableName, String[] ColumnName, DatabaseMaster.CommandComparison[] Comparison, Object[] Value, DatabaseMaster.WhereRelation[] relation)
        {

            if (IsOpened == false)
            {
                throw new Exception("Database not open,call Open()");
            }

            if (ColumnName.Length != Value.Length && ColumnName.Length != Comparison.Length && ColumnName.Length != relation.Length)
            {
                throw new Exception("Column number not Equals Value number");
            }

            long Count = database.GetDataCount(DatabaseInit.DatabaseName, TableName,
               GetWhere(ColumnName, Value, Comparison, relation));

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
        public static Object GetSpeciaRecord(String TableName, String ColumnName, String KeyColumnName, Object KeyValue, String OrderByName = "", SortMode sortMode = SortMode.Ascending)
        {

            if (IsOpened == false)
            {
                throw new Exception("Database not open,call Open()");
            }
            Object result = database.GetSpeciaRecordValue(DatabaseInit.DatabaseName, TableName,
                GetFilterOP(KeyColumnName, KeyValue,CommandComparison.Equals),ColumnName);

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
        public static Object GetSpeciaRecord(String TableName, String ColumnName, String[] KeyColumnName, Object[] KeyValue, String OrderByName = "", SortMode sortMode = SortMode.Ascending)
        {

            if (IsOpened == false)
            {
                throw new Exception("Database not open,call Open()");
            }

            if (KeyColumnName.Length != KeyValue.Length)
            {
                throw new Exception("Column number not Equals Value number");
            }

            Object result = database.GetSpeciaRecordValue(DatabaseInit.DatabaseName, TableName,
                GetWhere(KeyColumnName, KeyValue), ColumnName);

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
        public static Object GetSpeciaRecord(String TableName, String ColumnName, String[] KeyColumnName, Object[] KeyValue, Boolean CountNumber)
        {

            if (IsOpened == false)
            {
                throw new Exception("Database not open,call Open()");
            }

            if (KeyColumnName.Length != KeyValue.Length)
            {
                throw new Exception("Column number not Equals Value number");
            }

            Object result = database.GetDataCount(DatabaseInit.DatabaseName, TableName,
                GetWhere(KeyColumnName, KeyValue));

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
        public static Object GetSpeciaRecord(String TableName, String ColumnName, String[] KeyColumnName, DatabaseMaster.CommandComparison[] Comparison, Object[] KeyValue,DatabaseMaster.WhereRelation[] relation, Boolean CountNumber)
        {

            if (IsOpened == false)
            {
                throw new Exception("Database not open,call Open()");
            }

            if (KeyColumnName.Length != KeyValue.Length&& KeyColumnName.Length != Comparison.Length&& KeyColumnName.Length !=relation.Length)
            {
                throw new Exception("Column number not Equals Value number");
            }

            Object result = database.GetDataCount(DatabaseInit.DatabaseName, TableName,
                GetWhere(KeyColumnName, KeyValue,Comparison, relation));

            return result;
        }


        /// <summary>
        /// 得到指定条件的行计数数据
        /// </summary>
        /// <param name="TableName"></param>
        /// <param name="ColumnName"></param>
        /// <param name="CountNumber"></param>
        /// <returns></returns>
        public static Object GetSpeciaRecord(String TableName, String ColumnName, Boolean CountNumber)
        {

            if (IsOpened == false)
            {
                throw new Exception("Database not open,call Open()");
            }
            Object result = database.GetDataCount(DatabaseInit.DatabaseName, TableName,
                new BsonDocument());

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
        public static DataTable GetTopRecordsData(String TableName, Int32 RecordNumber, String OrderByName = "", SortMode sortMode = SortMode.Ascending)
        {

            if (IsOpened == false)
            {
                throw new Exception("Database not open,call Open()");
            }



            DataSet ds = database.GetTopRecordsData(DatabaseInit.DatabaseName, TableName,
                 new BsonDocument(), RecordNumber, OrderByName, sortMode);

            return ds.Tables[0];
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
        public static DataTable GetTopRecordsData(String TableName, String ColumnName, Object Value, Int32 RecordNumber, String OrderByName = "", SortMode sortMode = SortMode.Ascending)
        {

            if (IsOpened == false)
            {
                throw new Exception("Database not open,call Open()");
            }



            DataSet ds = database.GetTopRecordsData(DatabaseInit.DatabaseName, TableName,
                 GetFilterOP(ColumnName, Value, CommandComparison.Equals), RecordNumber, OrderByName, sortMode);

            return ds.Tables[0];
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
        public static DataTable GetTopRecordsData(String TableName, String ColumnName, DatabaseMaster.CommandComparison Comparison, Object Value, Int32 RecordNumber, String OrderByName = "", SortMode sortMode = SortMode.Ascending)
        {
            if (IsOpened == false)
            {
                throw new Exception("Database not open,call Open()");
            }
            DataSet ds = database.GetTopRecordsData(DatabaseInit.DatabaseName, TableName,
                 GetFilterOP(ColumnName, Value, Comparison), RecordNumber, OrderByName, sortMode);
            return ds.Tables[0];
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
        public static DataTable GetTopRecordsData(String TableName, String[] ColumnName, Object[] Value, Int32 RecordNumber, String OrderByName = "", SortMode sortMode = SortMode.Ascending)
        {

            if (IsOpened == false)
            {
                throw new Exception("Database not open,call Open()");
            }

            if (ColumnName.Length != Value.Length)
            {
                throw new Exception("Column number not Equals Value number");
            }

            DataSet ds = database.GetTopRecordsData(DatabaseInit.DatabaseName, TableName,
                 GetWhere(ColumnName, Value), RecordNumber, OrderByName, sortMode);

            return ds.Tables[0];
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
        public static DataTable GetTopRecordsData(String TableName, String[] ColumnName, DatabaseMaster.CommandComparison[] Comparison, Object[] Value, WhereRelation[] relation, Int32 RecordNumber, String OrderByName = "", SortMode sortMode = SortMode.Ascending)
        {

            if (IsOpened == false)
            {
                throw new Exception("Database not open,call Open()");
            }

            if (ColumnName.Length != Value.Length && ColumnName.Length != Comparison.Length && ColumnName.Length != relation.Length)
            {
                throw new Exception("Column number not Equals Value number");
            }

            DataSet ds = database.GetTopRecordsData(DatabaseInit.DatabaseName, TableName,
                 GetWhere(ColumnName, Value, Comparison, relation),  RecordNumber, OrderByName, sortMode);

            return ds.Tables[0];
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
        public static List<Hashtable> GetTopHashTableRecordsData(String TableName, Int32 RecordNumber, String OrderByName = "", SortMode sortMode = SortMode.Ascending)
        {

            if (IsOpened == false)
            {
                throw new Exception("Database not open,call Open()");
            }



            List<Hashtable> ds = database.GetTopRecordsHashtable(DatabaseInit.DatabaseName, TableName,
                 new BsonDocument(), RecordNumber, OrderByName, sortMode);

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
        public static List<Hashtable> GetTopHashTableRecordsData(String TableName, String ColumnName, Object Value, Int32 RecordNumber, String OrderByName = "", SortMode sortMode = SortMode.Ascending)
        {

            if (IsOpened == false)
            {
                throw new Exception("Database not open,call Open()");
            }



            List<Hashtable> ds = database.GetTopRecordsHashtable(DatabaseInit.DatabaseName, TableName,
                 GetFilterOP(ColumnName, Value, CommandComparison.Equals), RecordNumber, OrderByName, sortMode);

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
        public static List<Hashtable> GetTopHashTableRecordsData(String TableName, String ColumnName, DatabaseMaster.CommandComparison Comparison, Object Value, Int32 RecordNumber, String OrderByName = "", SortMode sortMode = SortMode.Ascending)
        {
            if (IsOpened == false)
            {
                throw new Exception("Database not open,call Open()");
            }
            List<Hashtable> ds = database.GetTopRecordsHashtable(DatabaseInit.DatabaseName, TableName,
                 GetFilterOP(ColumnName, Value, Comparison), RecordNumber, OrderByName, sortMode);
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
        public static List<Hashtable> GetTopHashTableRecordsData(String TableName, String[] ColumnName, Object[] Value, Int32 RecordNumber, String OrderByName = "", SortMode sortMode = SortMode.Ascending)
        {

            if (IsOpened == false)
            {
                throw new Exception("Database not open,call Open()");
            }

            if (ColumnName.Length != Value.Length)
            {
                throw new Exception("Column number not Equals Value number");
            }

            List<Hashtable> ds = database.GetTopRecordsHashtable(DatabaseInit.DatabaseName, TableName,
                 GetWhere(ColumnName, Value), RecordNumber, OrderByName, sortMode);

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
        public static List<Hashtable> GetTopHashTableRecordsData(String TableName, String[] ColumnName, DatabaseMaster.CommandComparison[] Comparison, Object[] Value, WhereRelation[] relation, Int32 RecordNumber, String OrderByName = "", SortMode sortMode = SortMode.Ascending)
        {

            if (IsOpened == false)
            {
                throw new Exception("Database not open,call Open()");
            }

            if (ColumnName.Length != Value.Length && ColumnName.Length != Comparison.Length && ColumnName.Length != relation.Length)
            {
                throw new Exception("Column number not Equals Value number");
            }

            List<Hashtable> ds = database.GetTopRecordsHashtable(DatabaseInit.DatabaseName, TableName,
                 GetWhere(ColumnName, Value, Comparison, relation), RecordNumber, OrderByName, sortMode);

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
        public static DataTable GetTopPageRecordsData(String TableName,  Int32 StartNumber, Int32 RecordNumber, String OrderByName = "", SortMode sortMode = SortMode.Ascending)
        {

            if (IsOpened == false)
            {
                throw new Exception("Database not open,call Open()");
            }



            DataSet ds = database.GetTopPageRecordsData(DatabaseInit.DatabaseName, TableName,
                 new BsonDocument(), StartNumber, RecordNumber, OrderByName, sortMode);

            return ds.Tables[0];
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
        public static DataTable GetTopPageRecordsData(String TableName, String ColumnName, Object Value, Int32 StartNumber, Int32 RecordNumber, String OrderByName = "", SortMode sortMode = SortMode.Ascending)
        {

            if (IsOpened == false)
            {
                throw new Exception("Database not open,call Open()");
            }



            DataSet ds = database.GetTopPageRecordsData(DatabaseInit.DatabaseName, TableName,
                 GetFilterOP(ColumnName, Value, CommandComparison.Equals), StartNumber, RecordNumber, OrderByName, sortMode);

            return ds.Tables[0];
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
        public static DataTable GetTopPageRecordsData(String TableName, String ColumnName, DatabaseMaster.CommandComparison Comparison, Object Value, Int32 StartNumber, Int32 RecordNumber, String OrderByName = "", SortMode sortMode = SortMode.Ascending)
        {
            if (IsOpened == false)
            {
                throw new Exception("Database not open,call Open()");
            }
            DataSet ds = database.GetTopPageRecordsData(DatabaseInit.DatabaseName, TableName,
                 GetFilterOP(ColumnName, Value, Comparison), StartNumber, RecordNumber, OrderByName, sortMode);
            return ds.Tables[0];
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
        public static DataTable GetTopPageRecordsData(String TableName, String[] ColumnName, Object[] Value, Int32 StartNumber, Int32 RecordNumber, String OrderByName = "", SortMode sortMode = SortMode.Ascending)
        {

            if (IsOpened == false)
            {
                throw new Exception("Database not open,call Open()");
            }

            if (ColumnName.Length != Value.Length)
            {
                throw new Exception("Column number not Equals Value number");
            }

            DataSet ds = database.GetTopPageRecordsData(DatabaseInit.DatabaseName, TableName,
                 GetWhere(ColumnName, Value), StartNumber, RecordNumber, OrderByName, sortMode);

            return ds.Tables[0];
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
        public static DataTable GetTopPageRecordsData(String TableName, String[] ColumnName, DatabaseMaster.CommandComparison[] Comparison, Object[] Value, WhereRelation[] relation, Int32 StartNumber, Int32 RecordNumber, String OrderByName = "", SortMode sortMode = SortMode.Ascending)
        {

            if (IsOpened == false)
            {
                throw new Exception("Database not open,call Open()");
            }

            if (ColumnName.Length != Value.Length && ColumnName.Length != Comparison.Length && ColumnName.Length != relation.Length)
            {
                throw new Exception("Column number not Equals Value number");
            }

            DataSet ds = database.GetTopPageRecordsData(DatabaseInit.DatabaseName, TableName,
                 GetWhere(ColumnName, Value, Comparison, relation),  StartNumber, RecordNumber, OrderByName, sortMode);

            return ds.Tables[0];
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
        public static List<Hashtable> GetTopHashTablePageRecordsData(String TableName, Int32 StartNumber, Int32 RecordNumber, String OrderByName = "", SortMode sortMode = SortMode.Ascending)
        {

            if (IsOpened == false)
            {
                throw new Exception("Database not open,call Open()");
            }



            List<Hashtable> ds = database.GetTopPageRecordsHashtable(DatabaseInit.DatabaseName, TableName,
                new BsonDocument(), StartNumber, RecordNumber, OrderByName, sortMode);

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
        public static List<Hashtable> GetTopHashTablePageRecordsData(String TableName, String ColumnName, Object Value, Int32 StartNumber, Int32 RecordNumber, String OrderByName = "", SortMode sortMode = SortMode.Ascending)
        {

            if (IsOpened == false)
            {
                throw new Exception("Database not open,call Open()");
            }



            List<Hashtable> ds = database.GetTopPageRecordsHashtable(DatabaseInit.DatabaseName, TableName,
                 GetFilterOP(ColumnName, Value, CommandComparison.Equals), StartNumber, RecordNumber, OrderByName, sortMode);

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
        public static List<Hashtable> GetTopHashTablePageRecordsData(String TableName, String ColumnName, DatabaseMaster.CommandComparison Comparison, Object Value, Int32 StartNumber, Int32 RecordNumber, String OrderByName = "", SortMode sortMode = SortMode.Ascending)
        {
            if (IsOpened == false)
            {
                throw new Exception("Database not open,call Open()");
            }
            List<Hashtable> ds = database.GetTopPageRecordsHashtable(DatabaseInit.DatabaseName, TableName,
                 GetFilterOP(ColumnName, Value, Comparison), StartNumber, RecordNumber, OrderByName, sortMode);
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
        public static List<Hashtable> GetTopHashTablePageRecordsData(String TableName, String[] ColumnName, Object[] Value, Int32 StartNumber, Int32 RecordNumber, String OrderByName = "", SortMode sortMode = SortMode.Ascending)
        {

            if (IsOpened == false)
            {
                throw new Exception("Database not open,call Open()");
            }

            if (ColumnName.Length != Value.Length)
            {
                throw new Exception("Column number not Equals Value number");
            }

            List<Hashtable> ds = database.GetTopPageRecordsHashtable(DatabaseInit.DatabaseName, TableName,
                 GetWhere(ColumnName, Value), StartNumber, RecordNumber, OrderByName, sortMode);

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
        public static List<Hashtable> GetTopHashTablePageRecordsData(String TableName, String[] ColumnName, DatabaseMaster.CommandComparison[] Comparison, Object[] Value, WhereRelation[] relation, Int32 StartNumber, Int32 RecordNumber, String OrderByName = "", SortMode sortMode = SortMode.Ascending)
        {

            if (IsOpened == false)
            {
                throw new Exception("Database not open,call Open()");
            }

            if (ColumnName.Length != Value.Length && ColumnName.Length != Comparison.Length && ColumnName.Length != relation.Length)
            {
                throw new Exception("Column number not Equals Value number");
            }

            List<Hashtable> ds = database.GetTopPageRecordsHashtable(DatabaseInit.DatabaseName, TableName,
                 GetWhere(ColumnName, Value, Comparison, relation), StartNumber, RecordNumber, OrderByName, sortMode);

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
        public static DataTable GetColumnDataTable(String TableName, List<String> IncludeColumnName, List<String> ExcludeColumnName, String OrderByName = "", SortMode sortMode = SortMode.Ascending)
        {
            if (IsOpened == false)
            {
                throw new Exception("Database not open,call Open()");
            }
            DataSet ds = database.GetColumnDataSet(DatabaseInit.DatabaseName, TableName, IncludeColumnName, ExcludeColumnName,
                new BsonDocument(), OrderByName, sortMode);


            return ds.Tables[0];
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
        public static DataTable GetColumnDataTable(String TableName, List<String> IncludeColumnName, List<String> ExcludeColumnName, String MatchColumn, Object MatchValue, String OrderByName = "", SortMode sortMode = SortMode.Ascending)
        {

            if (IsOpened == false)
            {
                throw new Exception("Database not open,call Open()");
            }



            DataSet ds = database.GetColumnDataSet(DatabaseInit.DatabaseName, TableName, IncludeColumnName, ExcludeColumnName,
                 GetFilterOP(MatchColumn, MatchValue, CommandComparison.Equals), OrderByName, sortMode);

            return ds.Tables[0];
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
        public static DataTable GetColumnDataTable(String TableName, List<String> IncludeColumnName, List<String> ExcludeColumnName, String MatchColumn, DatabaseMaster.CommandComparison Comparison, Object MatchValue, String OrderByName = "", SortMode sortMode = SortMode.Ascending)
        {
            if (IsOpened == false)
            {
                throw new Exception("Database not open,call Open()");
            }
            DataSet ds = database.GetColumnDataSet(DatabaseInit.DatabaseName, TableName, IncludeColumnName, ExcludeColumnName,
                 GetFilterOP(MatchColumn, MatchValue, Comparison), OrderByName, sortMode);
            return ds.Tables[0];
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
        public static DataTable GetColumnDataTable(String TableName, List<String> IncludeColumnName, List<String> ExcludeColumnName, String[] MatchColumn, Object[] MatchValue, String OrderByName = "", SortMode sortMode = SortMode.Ascending)
        {

            if (IsOpened == false)
            {
                throw new Exception("Database not open,call Open()");
            }

            if (MatchColumn.Length != MatchValue.Length)
            {
                throw new Exception("Column number not Equals Value number");
            }

            DataSet ds = database.GetColumnDataSet(DatabaseInit.DatabaseName, TableName, IncludeColumnName, ExcludeColumnName,
                 GetWhere(MatchColumn, MatchValue), OrderByName, sortMode);

            return ds.Tables[0];
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
        public static DataTable GetColumnDataTable(String TableName, List<String> IncludeColumnName, List<String> ExcludeColumnName, String[] MatchColumn, DatabaseMaster.CommandComparison[] Comparison, Object[] MatchValue, WhereRelation[] relation, String OrderByName = "", SortMode sortMode = SortMode.Ascending)
        {

            if (IsOpened == false)
            {
                throw new Exception("Database not open,call Open()");
            }

            if (MatchColumn.Length != MatchValue.Length && MatchColumn.Length != Comparison.Length && MatchColumn.Length != relation.Length)
            {
                throw new Exception("Column number not Equals Value number");
            }

            DataSet ds = database.GetColumnDataSet(DatabaseInit.DatabaseName, TableName, IncludeColumnName, ExcludeColumnName,
                 GetWhere(MatchColumn, MatchValue, Comparison, relation), OrderByName, sortMode);

            return ds.Tables[0];
        }

        /// <summary>
        /// 插入数据
        /// </summary>
        /// <param name="TableName"></param>
        /// <param name="ColumnName"></param>
        /// <param name="Value"></param>
        public static void InsertNewDataToDatabase(String TableName, String[] ColumnName, Object[] Value)
        {

            if (IsOpened == false)
            {
                throw new Exception("Database not open,call Open()");
            }

            if (ColumnName.Length!=Value.Length)
            {
                throw new Exception("Column number not Equals Value number");
            }

            Hashtable ht = new Hashtable();

            for (int i = 0; i < ColumnName.Length; i++)
            {
                ht.Add(ColumnName[i], Value[i]);
            }

            database.InsertData(DatabaseInit.DatabaseName, TableName, MongoDB.Bson.Serialization.BsonSerializer.Deserialize<BsonDocument>(HashtableToJson(ht)));

            return;
        }


        /// <summary>
        /// 插入数据
        /// </summary>
        /// <param name="TableName"></param>
        /// <param name="ColumnName"></param>
        /// <param name="Value"></param>
        public static void InsertNewDataToDatabase(String TableName, Hashtable ht)
        {

            if (IsOpened == false)
            {
                throw new Exception("Database not open,call Open()");
            }

            if (ht.Count==0)
            {
                throw new Exception("Column number not Equals Value number");
            }

        
            database.InsertData(DatabaseInit.DatabaseName, TableName, MongoDB.Bson.Serialization.BsonSerializer.Deserialize<BsonDocument>(HashtableToJson(ht)));

            return;
        }

        /// <summary>
        /// 插入数据并返回唯一字符串主键
        /// </summary>
        /// <param name="TableName"></param>
        /// <param name="ColumnName"></param>
        /// <param name="Value"></param>
        /// <param name="KeyColumnName"></param>
        /// <param name="GUIDValue"></param>
        /// <returns></returns>
        public static String InsertNewDataToDatabase(String TableName, String[] ColumnName, Object[] Value, String KeyColumnName, String GUIDValue)
        {

            if (IsOpened == false)
            {
                throw new Exception("Database not open,call Open()");
            }

            if (ColumnName.Length != Value.Length)
            {
                throw new Exception("Column number not Equals Value number");
            }

            Hashtable ht = new Hashtable();

            for (int i = 0; i < ColumnName.Length; i++)
            {
                ht.Add(ColumnName[i], Value[i]);
            }

            String s = database.InsertData(DatabaseInit.DatabaseName, TableName, MongoDB.Bson.Serialization.BsonSerializer.Deserialize<BsonDocument>(HashtableToJson(ht))
                , KeyColumnName, GUIDValue);

            return s;
        }


        /// <summary>
        /// 插入数据并返回唯一字符串主键
        /// </summary>
        /// <param name="TableName"></param>
        /// <param name="ht"></param>
        /// <param name="KeyColumnName"></param>
        /// <param name="GUIDValue"></param>
        /// <returns></returns>
        public static String InsertNewDataToDatabase(String TableName, Hashtable ht, String KeyColumnName, String GUIDValue)
        {

            if (IsOpened == false)
            {
                throw new Exception("Database not open,call Open()");
            }

            if (ht.Count == 0)
            {
                throw new Exception("Column number not Equals Value number");
            }


            String s = database.InsertData(DatabaseInit.DatabaseName, TableName, MongoDB.Bson.Serialization.BsonSerializer.Deserialize<BsonDocument>(HashtableToJson(ht))
                 , KeyColumnName, GUIDValue);

            return s;
        }

        /// <summary>
        /// 插入数据
        /// </summary>
        /// <param name="TableName"></param>
        /// <param name="ColumnName"></param>
        /// <param name="Value"></param>
        public static void InsertNewDataToDatabase(String TableName, List<Hashtable> ht)
        {

            if (IsOpened == false)
            {
                throw new Exception("Database not open,call Open()");
            }

            if (ht.Count == 0)
            {
                throw new Exception("Column number not Equals Value number");
            }

            BsonDocument[] document = new BsonDocument[ht.Count];
            for (int i = 0; i < ht.Count; i++)
            {
                document[i] = MongoDB.Bson.Serialization.BsonSerializer.Deserialize<BsonDocument>(HashtableToJson(ht[i]));
            }

            database.InsertData(DatabaseInit.DatabaseName, TableName, document);

            return;
        }

        /// <summary>
        /// 更新数据
        /// </summary>
        /// <param name="TableName"></param>
        /// <param name="ColumnName"></param>
        /// <param name="Value"></param>
        /// <param name="KeyColumnName"></param>
        /// <param name="KeyValue"></param>
        /// <returns></returns>
        public static Int32 UpdateDataToDatabase(String TableName, String[] ColumnName, Object[] Value, String KeyColumnName, Object KeyValue)
        {

            if (IsOpened == false)
            {
                throw new Exception("Database not open,call Open()");
            }

            if (ColumnName.Length != Value.Length)
            {
                throw new Exception("Column number not Equals Value number");
            }

            Hashtable ht = new Hashtable();

            for (int i = 0; i < ColumnName.Length; i++)
            {
                ht.Add(ColumnName[i], Value[i]);
            }

           Int32 count= database.UpdateData(DatabaseInit.DatabaseName, TableName, GetFilterOP(KeyColumnName,KeyValue,DatabaseMaster.CommandComparison.Equals),ht);

            return count;
        }

        /// <summary>
        /// 更新数据
        /// </summary>
        /// <param name="TableName"></param>
        /// <param name="ColumnName"></param>
        /// <param name="Value"></param>
        /// <param name="KeyColumnName"></param>
        /// <param name="KeyValue"></param>
        /// <returns></returns>
        public static Int32 UpdateDataToDatabase(String TableName, String[] ColumnName, Object[] Value, String KeyColumnName, DatabaseMaster.CommandComparison comparison, Object KeyValue)
        {

            if (IsOpened == false)
            {
                throw new Exception("Database not open,call Open()");
            }

            if (ColumnName.Length != Value.Length)
            {
                throw new Exception("Column number not Equals Value number");
            }

            Hashtable ht = new Hashtable();

            for (int i = 0; i < ColumnName.Length; i++)
            {
                ht.Add(ColumnName[i], Value[i]);
            }

            Int32 count = database.UpdateData(DatabaseInit.DatabaseName, TableName, GetFilterOP(KeyColumnName, KeyValue, comparison), ht);

            return count;
        }


        /// <summary>
        /// 更新数据
        /// </summary>
        /// <param name="TableName"></param>
        /// <param name="ColumnName"></param>
        /// <param name="Value"></param>
        /// <param name="KeyColumnName"></param>
        /// <param name="KeyValue"></param>
        /// <returns></returns>
        public static Int32 UpdateDataToDatabase(String TableName, String[] ColumnName, Object[] Value, String[] KeyColumnName, Object[] KeyValue)
        {

            if (IsOpened == false)
            {
                throw new Exception("Database not open,call Open()");
            }

            if (ColumnName.Length != Value.Length)
            {
                throw new Exception("Column number not Equals Value number");
            }

            if (KeyColumnName.Length != KeyValue.Length)
            {
                throw new Exception("Column number not Equals Value number");
            }

            Hashtable ht = new Hashtable();

            for (int i = 0; i < ColumnName.Length; i++)
            {
                ht.Add(ColumnName[i], Value[i]);
            }

            Int32 count = database.UpdateData(DatabaseInit.DatabaseName, TableName, GetWhere(KeyColumnName, KeyValue),ht);

            return count;
        }

        /// <summary>
        /// 更新数据
        /// </summary>
        /// <param name="TableName"></param>
        /// <param name="ColumnName"></param>
        /// <param name="Value"></param>
        /// <param name="KeyColumnName"></param>
        /// <param name="KeyValue"></param>
        /// <returns></returns>
        public static Int32 UpdateDataToDatabase(String TableName, String[] ColumnName, Object[] Value, String[] KeyColumnName, DatabaseMaster.CommandComparison[] comparison, Object[] KeyValue, DatabaseMaster.WhereRelation[] relation)
        {

            if (IsOpened == false)
            {
                throw new Exception("Database not open,call Open()");
            }

            if (ColumnName.Length != Value.Length)
            {
                throw new Exception("Column number not Equals Value number");
            }

            if (KeyColumnName.Length != KeyValue.Length)
            {
                throw new Exception("Column number not Equals Value number");
            }

            Hashtable ht = new Hashtable();

            for (int i = 0; i < ColumnName.Length; i++)
            {
                ht.Add(ColumnName[i], Value[i]);
            }

            Int32 count = database.UpdateData(DatabaseInit.DatabaseName, TableName, GetWhere(KeyColumnName, KeyValue,comparison,relation),
                 ht);

            return count;
        }


        /// <summary>
        /// 删除数据
        /// </summary>
        /// <param name="TableName"></param>
        /// <param name="KeyColumnName"></param>
        /// <param name="KeyValue"></param>
        /// <returns></returns>
        public static Int32 DeleteNowDataToDatabase(String TableName, String KeyColumnName, Object KeyValue)
        {

            if (IsOpened == false)
            {
                throw new Exception("Database not open,call Open()");
            }


            Int32 count = database.DeleteData(DatabaseInit.DatabaseName, TableName,
                GetFilterOP(KeyColumnName, KeyValue, DatabaseMaster.CommandComparison.Equals));

            return count;
        }

        /// <summary>
        /// 删除数据
        /// </summary>
        /// <param name="TableName"></param>
        /// <param name="KeyColumnName"></param>
        /// <param name="KeyValue"></param>
        /// <returns></returns>
        public static Int32 DeleteNowDataToDatabase(String TableName, String KeyColumnName, DatabaseMaster.CommandComparison comparison, Object KeyValue)
        {

            if (IsOpened == false)
            {
                throw new Exception("Database not open,call Open()");
            }


            Int32 count = database.DeleteData(DatabaseInit.DatabaseName, TableName,
                GetFilterOP(KeyColumnName, KeyValue, comparison));

            return count;
        }

        /// <summary>
        /// 删除数据
        /// </summary>
        /// <param name="TableName"></param>
        /// <param name="KeyColumnName"></param>
        /// <param name="KeyValue"></param>
        /// <returns></returns>
        public static Int32 DeleteNowDataToDatabase(String TableName, String[] KeyColumnName, Object[] KeyValue)
        {

            if (IsOpened == false)
            {
                throw new Exception("Database not open,call Open()");
            }

            if (KeyColumnName.Length != KeyValue.Length)
            {
                throw new Exception("Column number not Equals Value number");
            }

            Int32 count = database.DeleteData(DatabaseInit.DatabaseName, TableName,
                GetWhere(KeyColumnName, KeyValue));

            return count;
        }

        /// <summary>
        /// 删除数据
        /// </summary>
        /// <param name="TableName"></param>
        /// <param name="KeyColumnName"></param>
        /// <param name="KeyValue"></param>
        /// <returns></returns>
        public static Int32 DeleteNowDataToDatabase(String TableName, String[] KeyColumnName, DatabaseMaster.CommandComparison[] comparison, Object[] KeyValue, DatabaseMaster.WhereRelation[] relation)
        {

            if (IsOpened == false)
            {
                throw new Exception("Database not open,call Open()");
            }

            if (KeyColumnName.Length != KeyValue.Length)
            {
                throw new Exception("Column number not Equals Value number");
            }

            Int32 count = database.DeleteData(DatabaseInit.DatabaseName, TableName,
                GetWhere(KeyColumnName, KeyValue,comparison,relation));

            return count;
        }

        /// <summary>
        /// 删除数据
        /// </summary>
        /// <param name="TableName"></param>
        /// <param name="KeyColumnName"></param>
        /// <param name="KeyValue"></param>
        /// <returns></returns>
        public static Int32 DeleteNowDataToDatabase(String TableName, Boolean All)
        {

            if (IsOpened == false)
            {
                throw new Exception("Database not open,call Open()");
            }

            if (All==false)
            {
                return 0;
            }

            Int32 count = database.DeleteData(DatabaseInit.DatabaseName, TableName);

            return count;
        }

        /// <summary>
        /// 删除数据
        /// </summary>
        /// <param name="TableName"></param>
        /// <param name="KeyColumnName"></param>
        /// <param name="KeyValue"></param>
        /// <returns></returns>
        public static Int32 DeleteCollection(String TableName)
        {

            if (IsOpened == false)
            {
                throw new Exception("Database not open,call Open()");
            }


            database.DeleteTable(DatabaseInit.DatabaseName, TableName);

            return 0;
        }

        /// <summary>
        /// 删除数据
        /// </summary>
        /// <param name="TableName"></param>
        /// <param name="KeyColumnName"></param>
        /// <param name="KeyValue"></param>
        /// <returns></returns>
        public static String UploadFile(String FilePath)
        {

            if (IsOpened == false)
            {
                throw new Exception("Database not open,call Open()");
            }

         
            String s = database.UploadFile(DatabaseInit.DatabaseName, FilePath);

            return s;
        }

        /// <summary>
        /// 删除数据
        /// </summary>
        /// <param name="TableName"></param>
        /// <param name="KeyColumnName"></param>
        /// <param name="KeyValue"></param>
        /// <returns></returns>
        public static void DownloadFile(String ID,String FilePath)
        {

            if (IsOpened == false)
            {
                throw new Exception("Database not open,call Open()");
            }


            database.DownloadFile(DatabaseInit.DatabaseName, ID, FilePath);

            return ;
        }

        /// <summary>
        /// 删除数据
        /// </summary>
        /// <param name="TableName"></param>
        /// <param name="KeyColumnName"></param>
        /// <param name="KeyValue"></param>
        /// <returns></returns>
        public static void DeleteFile(String ID)
        {

            if (IsOpened == false)
            {
                throw new Exception("Database not open,call Open()");
            }


            database.DeleteFile(DatabaseInit.DatabaseName, ID);

            return;
        }
    }
}
