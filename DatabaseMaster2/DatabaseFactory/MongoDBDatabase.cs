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
using MongoDB.Driver.GridFS;

namespace DatabaseMaster2
{
    public class MongoDBDatabase
    {
        MongoClient conn = null;
        String ConnectString = "";
        IClientSessionHandle session;

        /// <summary>
        /// Json转Hashtable
        /// </summary>
        /// <param name="json"></param>
        /// <returns></returns>
        private Hashtable Json2Hashtable(string json)
        {
            Regex r = new Regex(@"ObjectId\(\S+\)");

            MatchCollection match = r.Matches(json);

            foreach (Match m1 in match)
            {
                json = json.Replace(m1.Value, m1.Value.Replace("ObjectId(", "").Replace(")", ""));
            }


            r = new Regex(@"ISODate\(\S+\)");

            match = r.Matches(json);

            foreach (Match m1 in match)
            {
                String v = m1.Value.Replace("ISODate(\"", "").Replace("\")", "");
                v = DateTime.Parse(v).ToString();
                v = "\"" + v + "\"";
                json = json.Replace(m1.Value, v);


            }


            return JsonConvert.DeserializeObject<Hashtable>(json);
        }

        /// <summary>
        /// 将HashTable 转为DataTable
        /// </summary>
        /// <param name="tableList"></param>
        /// <returns></returns>
        private DataTable HashTableToDataTable(List<Hashtable> tableList)
        {
            DataTable dt = new DataTable();
            if (tableList != null && tableList.Count > 0)
            {
                //把Key 添加到Table中 成为列名称
                foreach (string item in tableList[0].Keys)
                {
                    dt.Columns.Add(item, typeof(string));
                }
                for (int i = 0; i < tableList.Count; i++)
                {
                    Hashtable ht = tableList[i];
                    DataRow dr = dt.NewRow();

                    foreach (DictionaryEntry de in ht)
                    {
                        //把Value 添加到对应的列名下边
                        if (de.Value == null)
                            dr[de.Key.ToString()] = "";
                        else
                            dr[de.Key.ToString()] = de.Value.ToString();
                    }
                    dt.Rows.Add(dr);

                }
            }

            return dt;
        }

        /// <summary>
        /// Json转Hashtable
        /// </summary>
        /// <param name="json"></param>
        /// <returns></returns>
        private String Hashtable2JsonArray(Object json)
        {
            return Newtonsoft.Json.JsonConvert.SerializeObject(json);
        }

        /// <GetConnectionString>
        /// GetConnectionString
        /// </GetConnectionString>
        /// <returns></returns>
        public MongoDBDatabase()
        {
            ConnectString = XmlStream.getXmlValue(AppDomain.CurrentDomain.BaseDirectory + "DatabaseConfig.xml", "MongoDB", "ConnectString");
        }

        /// <GetConnectionString>
        /// GetConnectionString
        /// </GetConnectionString>
        /// <returns></returns>
        public MongoDBDatabase(String ConnString, Boolean ConnStr)
        {
            ConnectString = ConnString;
        }

        /// <GetConnectionString>
        /// GetConnectionString
        /// </GetConnectionString>
        /// <returns></returns>
        public MongoDBDatabase(String ConnectName)
        {
            ConnectString = XmlStream.getXmlValue(AppDomain.CurrentDomain.BaseDirectory + "DatabaseConfig.xml", ConnectName, "ConnectString");
        }

        /// <GetConnectionString>
        /// GetConnectionString
        /// </GetConnectionString>
        /// <returns></returns>
        public MongoDBDatabase(String ConnectName, StringEncrypt.EncryptType type)
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

            conn = new MongoClient(ConnectString);
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
                if (conn.ListDatabaseNames().ToList().Count == 0)
                {
                    return false;
                }
            }
            catch (Exception)
            {
                return false;
            }


            return true;
        }

        /// <summary>
        /// Start Transaction
        /// </summary>
        public void StartTransaction()
        {
            if (conn == null)
            {
                throw new Exception("Connection has not initialize.");
            }

            session = conn.StartSession();
            session.StartTransaction();
        }

        /// <summary>
        /// Commit Transaction
        /// </summary>
        public void CommitTransaction()
        {
            if (conn == null)
            {
                throw new Exception("Connection has not initialize.");
            }

            if (session == null)
            {
                throw new Exception("StartTransaction has not initialize.");
            }

            session.CommitTransaction();
        }

        /// <summary>
        /// CloseTransaction
        /// </summary>
        public void CloseTransaction()
        {
            if (conn == null)
            {
                throw new Exception("Connection has not initialize.");
            }

            if (session == null)
            {
                throw new Exception("StartTransaction has not initialize.");
            }
            session.AbortTransaction();
            session = null;
        }

        /// <summary>
        /// get all database name
        /// </summary>
        /// <returns></returns>
        public List<String> GetDatabaseName()
        {
            if (conn == null)
            {
                throw new Exception("Connection has not initialize.");
            }

            return conn.ListDatabaseNames().ToList();
        }

        /// <summary>
        /// get first row and first colunm value from database
        /// </summary>
        /// <param name="DatabaseName"></param>
        /// <param name="TableName"></param>
        /// <param name="filterDefinition"></param>
        /// <param name="ColunmName"></param>
        /// <returns></returns>
        public object GetSpeciaRecordValue(String DatabaseName, String TableName, FilterDefinition<BsonDocument> filterDefinition, String ColunmName)
        {
            //command:DatabaseName|TableName|[Where JSON]|[Insert,Update,Delete]|[CloumnName][JSONData]
            if (conn == null)
            {
                throw new Exception("Connection has not initialize.");
            }


            if (DatabaseName.Length == 0)
            {
                throw new Exception("Database Name is not exist.");
            }

            IMongoDatabase database = conn.GetDatabase(DatabaseName);

            if (database.ListCollectionNames().ToList().Contains(TableName) == false)
            {
                throw new Exception("Collection Name is not exist in MongoDB Database.");
            }


            IMongoCollection<BsonDocument> collection = database.GetCollection<BsonDocument>(TableName);

            var filter = filterDefinition;

            long Count = collection.Find(filter).CountDocuments();

            if (Count == 0)
                return null;

            var document = collection.Find(filter).First();

            Hashtable ht = Json2Hashtable(document.ToJson());

            if (ht.ContainsKey(ColunmName) == false)
                return null;

            return ht[ColunmName];
        }

        /// <summary>
        /// Check database connect
        /// </summary>
        /// <returns></returns>
        public object CheckConnect()
        {

            if (conn == null)
            {
                throw new Exception("Connection has not initialize.");
            }

            List<String> s = GetDatabaseName();

            if (s.Count == 0)
                return 0;
            else
                return s.Count;
        }

        /// <summary>
        /// return a dataset from database
        /// </summary>
        /// <param name="DatabaseName"></param>
        /// <param name="TableName"></param>
        /// <param name="filterDefinition"></param>
        /// <param name="SortName"></param>
        /// <param name="sortMode"></param>
        /// <returns></returns>
        public DataSet GetDataSet(String DatabaseName, String TableName, FilterDefinition<BsonDocument> filterDefinition, String SortName = "", SortMode sortMode = SortMode.Ascending)
        {
            if (conn == null)
            {
                throw new Exception("Connection has not initialize.");
            }

            if (DatabaseName.Length == 0)
            {
                throw new Exception("Database Name is not exist.");
            }

            IMongoDatabase database = conn.GetDatabase(DatabaseName);

            if (database.ListCollectionNames().ToList().Contains(TableName) == false)
            {
                throw new Exception("Collection Name is not exist in MongoDB Database.");
            }

            IMongoCollection<BsonDocument> collection = database.GetCollection<BsonDocument>(TableName);

            var filter = filterDefinition;


            List<Hashtable> hr = new List<Hashtable>();

            IAsyncCursor<BsonDocument> cursor;

            if (SortName == "")
                cursor = collection.Find(filter).ToCursor();
            else
            {
                SortDefinitionBuilder<BsonDocument> builderSort = Builders<BsonDocument>.Sort;
                SortDefinition<BsonDocument> sort =
                    sortMode == SortMode.Ascending ? builderSort.Ascending(SortName) : builderSort.Descending(SortName);
                cursor = collection.Find(filter).Sort(sort).ToCursor();
            }

            foreach (var document in cursor.ToEnumerable())
            {
                Hashtable ht = Json2Hashtable(document.ToJson());
                hr.Add(ht);
            }

            DataTable dt = HashTableToDataTable(hr);
            DataSet ds = new DataSet();
            ds.Tables.Add(dt);

            return ds;

        }

        /// <summary>
        /// return a count from database
        /// </summary>
        /// <param name="DatabaseName"></param>
        /// <param name="TableName"></param>
        /// <param name="filterDefinition"></param>
        /// <returns></returns>
        public Int64 GetDataCount(String DatabaseName, String TableName, FilterDefinition<BsonDocument> filterDefinition)
        {
            if (conn == null)
            {
                throw new Exception("Connection has not initialize.");
            }

            if (DatabaseName.Length == 0)
            {
                throw new Exception("Database Name is not exist.");
            }

            IMongoDatabase database = conn.GetDatabase(DatabaseName);

            if (database.ListCollectionNames().ToList().Contains(TableName) == false)
            {
                throw new Exception("Collection Name is not exist in MongoDB Database.");
            }

            IMongoCollection<BsonDocument> collection = database.GetCollection<BsonDocument>(TableName);

            var filter = filterDefinition;

            long Number = collection.Find(filter).CountDocuments();


            return Convert.ToInt64(Number);

        }

        /// <summary>
        /// return a dataset from database
        /// </summary>
        /// <param name="DatabaseName"></param>
        /// <param name="TableName"></param>
        /// <param name="filterDefinition"></param>
        /// <returns></returns>
        public DataSet GetColumnDataSet(String DatabaseName, String TableName, List<String> IncludeColumnName, List<String> ExcludeColumnName, FilterDefinition<BsonDocument> filterDefinition, String SortName = "", SortMode sortMode = SortMode.Ascending)
        {
            if (conn == null)
            {
                throw new Exception("Connection has not initialize.");
            }

            if (DatabaseName.Length == 0)
            {
                throw new Exception("Database Name is not exist.");
            }

            IMongoDatabase database = conn.GetDatabase(DatabaseName);

            if (database.ListCollectionNames().ToList().Contains(TableName) == false)
            {
                throw new Exception("Collection Name is not exist in MongoDB Database.");
            }

            IMongoCollection<BsonDocument> collection = database.GetCollection<BsonDocument>(TableName);

            var filter = filterDefinition;


            List<Hashtable> hr = new List<Hashtable>();

            var fieldList = new List<ProjectionDefinition<BsonDocument>>();
            for (int i = 0; i < IncludeColumnName.Count; i++)
            {
                fieldList.Add(Builders<BsonDocument>.Projection.Include(IncludeColumnName[i]));
            }
            for (int i = 0; i < ExcludeColumnName.Count; i++)
            {
                fieldList.Add(Builders<BsonDocument>.Projection.Exclude(ExcludeColumnName[i]));
            }

            var projection = Builders<BsonDocument>.Projection.Combine(fieldList);

            IAsyncCursor<BsonDocument> cursor;

            if (SortName == "")
                cursor = collection.Find(filter).Project(projection).ToCursor();
            else
            {
                SortDefinitionBuilder<BsonDocument> builderSort = Builders<BsonDocument>.Sort;
                SortDefinition<BsonDocument> sort =
                    sortMode == SortMode.Ascending ? builderSort.Ascending(SortName) : builderSort.Descending(SortName);
                cursor = collection.Find(filter).Project(projection).Sort(sort).ToCursor();
            }

            foreach (var document in cursor.ToEnumerable())
            {
                Hashtable ht = Json2Hashtable(document.ToJson());
                hr.Add(ht);
            }

            DataTable dt = HashTableToDataTable(hr);
            DataSet ds = new DataSet();
            ds.Tables.Add(dt);

            return ds;

        }

        /// <summary>
        /// return a dataset from database
        /// </summary>
        /// <param name="DatabaseName"></param>
        /// <param name="TableName"></param>
        /// <param name="filterDefinition"></param>
        /// <param name="LimitNumber"></param>
        /// <param name="SortName"></param>
        /// <param name="sortMode"></param>
        /// <returns></returns>
        public DataSet GetTopRecordsData(String DatabaseName, String TableName, FilterDefinition<BsonDocument> filterDefinition, Int32 LimitNumber, String SortName = "", SortMode sortMode = SortMode.Ascending)
        {
            if (conn == null)
            {
                throw new Exception("Connection has not initialize.");
            }

            if (DatabaseName.Length == 0)
            {
                throw new Exception("Database Name is not exist.");
            }

            IMongoDatabase database = conn.GetDatabase(DatabaseName);

            if (database.ListCollectionNames().ToList().Contains(TableName) == false)
            {
                throw new Exception("Collection Name is not exist in MongoDB Database.");
            }

            IMongoCollection<BsonDocument> collection = database.GetCollection<BsonDocument>(TableName);

            var filter = filterDefinition;


            List<Hashtable> hr = new List<Hashtable>();

            IAsyncCursor<BsonDocument> cursor;

            if (SortName == "")
                cursor = collection.Find(filter).Limit(LimitNumber).ToCursor();
            else
            {
                SortDefinitionBuilder<BsonDocument> builderSort = Builders<BsonDocument>.Sort;
                SortDefinition<BsonDocument> sort =
                    sortMode == SortMode.Ascending ? builderSort.Ascending(SortName) : builderSort.Descending(SortName);
                cursor = collection.Find(filter).Limit(LimitNumber).Sort(sort).ToCursor();
            }


            foreach (var document in cursor.ToEnumerable())
            {
                Hashtable ht = Json2Hashtable(document.ToJson());
                hr.Add(ht);
            }

            DataTable dt = HashTableToDataTable(hr);
            DataSet ds = new DataSet();
            ds.Tables.Add(dt);

            return ds;

        }


        /// <summary>
        /// return a dataset from database
        /// </summary>
        /// <param name="DatabaseName"></param>
        /// <param name="TableName"></param>
        /// <param name="filterDefinition"></param>
        /// <param name="StartNumber"></param>
        /// <param name="LimitNumber"></param>
        /// <param name="SortName"></param>
        /// <param name="sortMode"></param>
        /// <returns></returns>
        public DataSet GetTopPageRecordsData(String DatabaseName, String TableName, FilterDefinition<BsonDocument> filterDefinition, Int32 StartNumber, Int32 LimitNumber, String SortName = "", SortMode sortMode = SortMode.Ascending)
        {
            if (conn == null)
            {
                throw new Exception("Connection has not initialize.");
            }

            if (DatabaseName.Length == 0)
            {
                throw new Exception("Database Name is not exist.");
            }

            IMongoDatabase database = conn.GetDatabase(DatabaseName);

            if (database.ListCollectionNames().ToList().Contains(TableName) == false)
            {
                throw new Exception("Collection Name is not exist in MongoDB Database.");
            }

            IMongoCollection<BsonDocument> collection = database.GetCollection<BsonDocument>(TableName);

            var filter = filterDefinition;


            List<Hashtable> hr = new List<Hashtable>();

            IAsyncCursor<BsonDocument> cursor;

            if (SortName == "")
                cursor = collection.Find(filter).Skip(StartNumber - 1).Limit(LimitNumber).ToCursor();
            else
            {
                SortDefinitionBuilder<BsonDocument> builderSort = Builders<BsonDocument>.Sort;
                SortDefinition<BsonDocument> sort =
                    sortMode == SortMode.Ascending ? builderSort.Ascending(SortName) : builderSort.Descending(SortName);
                cursor = collection.Find(filter).Skip(StartNumber - 1).Limit(LimitNumber).Sort(sort).ToCursor();
            }

            foreach (var document in cursor.ToEnumerable())
            {
                Hashtable ht = Json2Hashtable(document.ToJson());
                hr.Add(ht);
            }

            DataTable dt = HashTableToDataTable(hr);
            DataSet ds = new DataSet();
            ds.Tables.Add(dt);

            return ds;

        }


        /// <summary>
        /// Insert new data
        /// </summary>
        /// <param name="DatabaseName"></param>
        /// <param name="TableName"></param>
        /// <param name="document"></param>
        public void InsertData(String DatabaseName, String TableName, BsonDocument document)
        {
            if (conn == null)
            {
                throw new Exception("Connection has not initialize.");
            }

            if (DatabaseName.Length == 0)
            {
                throw new Exception("Database Name is not exist.");
            }

            IMongoDatabase database = conn.GetDatabase(DatabaseName);
            IMongoCollection<BsonDocument> collection = database.GetCollection<BsonDocument>(TableName);
            collection.InsertOne(document);

        }

        /// <summary>
        /// Insert new data
        /// </summary>
        /// <param name="DatabaseName"></param>
        /// <param name="TableName"></param>
        /// <param name="document"></param>
        /// <param name="ColumnName"></param>
        /// <param name="GUIDValue"></param>
        /// <returns></returns>
        public String InsertData(String DatabaseName, String TableName, BsonDocument document, String ColumnName, String GUIDValue)
        {
            if (conn == null)
            {
                throw new Exception("Connection has not initialize.");
            }

            if (DatabaseName.Length == 0)
            {
                throw new Exception("Database Name is not exist.");
            }

            IMongoDatabase database = conn.GetDatabase(DatabaseName);
            IMongoCollection<BsonDocument> collection = database.GetCollection<BsonDocument>(TableName);
            collection.InsertOne(document);

            FilterDefinition<BsonDocument> filter = Builders<BsonDocument>.Filter.Eq(ColumnName, GUIDValue);

            if (collection.Find(filter).CountDocuments() == 0)
                return "";
            else
            {
                var doc = collection.Find(filter).First();
                return doc[ColumnName].AsString;
            }

        }

        /// <summary>
        /// Insert new data
        /// </summary>
        /// <param name="DatabaseName"></param>
        /// <param name="TableName"></param>
        /// <param name="document"></param>
        public void InsertData(String DatabaseName, String TableName, BsonDocument[] document)
        {
            if (conn == null)
            {
                throw new Exception("Connection has not initialize.");
            }

            if (DatabaseName.Length == 0)
            {
                throw new Exception("Database Name is not exist.");
            }

            IMongoDatabase database = conn.GetDatabase(DatabaseName);
            IMongoCollection<BsonDocument> collection = database.GetCollection<BsonDocument>(TableName);
            collection.InsertMany(document);

        }

        /// <summary>
        /// Update new data
        /// </summary>
        /// <param name="DatabaseName"></param>
        /// <param name="TableName"></param>
        /// <param name="filterDefinition"></param>
        /// <param name="ht"></param>
        /// <returns></returns>
        public Int32 UpdateData(String DatabaseName, String TableName, FilterDefinition<BsonDocument> filterDefinition, Hashtable ht)
        {
            if (conn == null)
            {
                throw new Exception("Connection has not initialize.");
            }

            if (DatabaseName.Length == 0)
            {
                throw new Exception("Database Name is not exist.");
            }

            IMongoDatabase database = conn.GetDatabase(DatabaseName);
            IMongoCollection<BsonDocument> collection = database.GetCollection<BsonDocument>(TableName);


            List<UpdateDefinition<BsonDocument>> s = new List<UpdateDefinition<BsonDocument>>();
            foreach (DictionaryEntry de in ht)
            {
                if (de.Value.ToString().StartsWith("ISODate(") && de.Value.ToString().EndsWith(")"))
                    s.Add(Builders<BsonDocument>.Update.Set(de.Key.ToString(), BsonDateTime.Create(Convert.ToDateTime(de.Value.ToString().Replace("ISODate(", "").Replace(")", "")))));
                else
                    s.Add(Builders<BsonDocument>.Update.Set(de.Key.ToString(), de.Value));
            }

            var result1 = collection.UpdateMany(filterDefinition, Builders<BsonDocument>.Update.Combine(s.ToArray()));

            return Convert.ToInt32(result1.ModifiedCount);
        }

        /// <summary>
        /// delete data
        /// </summary>
        /// <param name="DatabaseName"></param>
        /// <param name="TableName"></param>
        /// <param name="filterDefinition"></param>
        /// <returns></returns>
        public Int32 DeleteData(String DatabaseName, String TableName, FilterDefinition<BsonDocument> filterDefinition)
        {
            if (conn == null)
            {
                throw new Exception("Connection has not initialize.");
            }

            if (DatabaseName.Length == 0)
            {
                throw new Exception("Database Name is not exist.");
            }

            IMongoDatabase database = conn.GetDatabase(DatabaseName);
            IMongoCollection<BsonDocument> collection = database.GetCollection<BsonDocument>(TableName);
            var result2 = collection.DeleteMany(filterDefinition);

            return Convert.ToInt32(result2.DeletedCount);

        }

        /// <summary>
        /// delete data
        /// </summary>
        /// <param name="DatabaseName"></param>
        /// <param name="TableName"></param>
        /// <returns></returns>
        public Int32 DeleteData(String DatabaseName, String TableName)
        {
            if (conn == null)
            {
                throw new Exception("Connection has not initialize.");
            }

            if (DatabaseName.Length == 0)
            {
                throw new Exception("Database Name is not exist.");
            }

            IMongoDatabase database = conn.GetDatabase(DatabaseName);
            IMongoCollection<BsonDocument> collection = database.GetCollection<BsonDocument>(TableName);

            FilterDefinition<BsonDocument> filterDefinition = new BsonDocument();
            var result2 = collection.DeleteMany(filterDefinition);

            return Convert.ToInt32(result2.DeletedCount);

        }

        /// <summary>
        /// delete data
        /// </summary>
        /// <param name="DatabaseName"></param>
        /// <param name="TableName"></param>
        /// <returns></returns>
        public void DeleteTable(String DatabaseName, String TableName)
        {
            if (conn == null)
            {
                throw new Exception("Connection has not initialize.");
            }

            if (DatabaseName.Length == 0)
            {
                throw new Exception("Database Name is not exist.");
            }

            IMongoDatabase database = conn.GetDatabase(DatabaseName);
            database.DropCollection(TableName);
            return;

        }

        /// <summary>
        /// 将文件写入数据库
        /// </summary>
        /// <param name="DatabaseName"></param>
        /// <param name="FilePath"></param>
        /// <param name="GridFSName"></param>
        /// <returns></returns>
        public String UploadFile(String DatabaseName, String FilePath, String GridFSName="")
        {
            if (conn == null)
            {
                throw new Exception("Connection has not initialize.");
            }

            if (DatabaseName.Length == 0)
            {
                throw new Exception("Database Name is not exist.");
            }

            if (File.Exists(FilePath) == false)
            {
                throw new Exception("File path is not exist.");
            }

            IMongoDatabase database = conn.GetDatabase(DatabaseName);

            MongoDB.Driver.GridFS.GridFSBucket bucket;
            if (String.IsNullOrEmpty(GridFSName))
                bucket = new MongoDB.Driver.GridFS.GridFSBucket(database);
            else
                bucket = new MongoDB.Driver.GridFS.GridFSBucket(database, new GridFSBucketOptions{ BucketName = GridFSName });
            ObjectId id = bucket.UploadFromBytes(Path.GetFileName(FilePath), File.ReadAllBytes(FilePath));

            return id.ToString();

        }


        /// <summary>
        /// 将数据库数据还原为文件
        /// </summary>
        /// <param name="DatabaseName"></param>
        /// <param name="id"></param>
        /// <param name="FilePath"></param>
        /// <param name="GridFSName"></param>
        public void DownloadFile(String DatabaseName, String id, String FilePath, String GridFSName = "")
        {
            if (conn == null)
            {
                throw new Exception("Connection has not initialize.");
            }

            if (DatabaseName.Length == 0)
            {
                throw new Exception("Database Name is not exist.");
            }

            IMongoDatabase database = conn.GetDatabase(DatabaseName);
            MongoDB.Driver.GridFS.GridFSBucket bucket;
            if (String.IsNullOrEmpty(GridFSName))
                bucket = new MongoDB.Driver.GridFS.GridFSBucket(database);
            else
                bucket = new MongoDB.Driver.GridFS.GridFSBucket(database, new GridFSBucketOptions { BucketName = GridFSName });
            Byte[] s = bucket.DownloadAsBytes(new ObjectId(id));

            File.WriteAllBytes(FilePath, s);

            return;

        }

        /// <summary>
        /// 删除文件
        /// </summary>
        /// <param name="DatabaseName"></param>
        /// <param name="id"></param>
        /// <param name="GridFSName"></param>
        public void DeleteFile(String DatabaseName, String id, String GridFSName = "")
        {
            if (conn == null)
            {
                throw new Exception("Connection has not initialize.");
            }

            if (DatabaseName.Length == 0)
            {
                throw new Exception("Database Name is not exist.");
            }

            IMongoDatabase database = conn.GetDatabase(DatabaseName);
            MongoDB.Driver.GridFS.GridFSBucket bucket;
            if (String.IsNullOrEmpty(GridFSName))
                bucket = new MongoDB.Driver.GridFS.GridFSBucket(database);
            else
                bucket = new MongoDB.Driver.GridFS.GridFSBucket(database, new GridFSBucketOptions { BucketName = GridFSName });
            bucket.Delete(new ObjectId(id));

            return;

        }


        /// <summary>
        ///  改名文件
        /// </summary>
        /// <param name="DatabaseName"></param>
        /// <param name="id"></param>
        /// <param name="Filename"></param>
        /// <param name="GridFSName"></param>
        public void ReNameFile(String DatabaseName, String id, String Filename, String GridFSName = "")
        {
            if (conn == null)
            {
                throw new Exception("Connection has not initialize.");
            }

            if (DatabaseName.Length == 0)
            {
                throw new Exception("Database Name is not exist.");
            }

            IMongoDatabase database = conn.GetDatabase(DatabaseName);
            MongoDB.Driver.GridFS.GridFSBucket bucket;
            if (String.IsNullOrEmpty(GridFSName))
                bucket = new MongoDB.Driver.GridFS.GridFSBucket(database);
            else
                bucket = new MongoDB.Driver.GridFS.GridFSBucket(database, new GridFSBucketOptions { BucketName = GridFSName });
            bucket.Rename(new ObjectId(id), Filename);

            return;

        }

        /// <summary>
        /// 删除文件库
        /// </summary>
        /// <param name="DatabaseName"></param>
        /// <param name="GridFSName"></param>
        public void DeleteGridFS(String DatabaseName, String GridFSName = "")
        {
            if (conn == null)
            {
                throw new Exception("Connection has not initialize.");
            }

            if (DatabaseName.Length == 0)
            {
                throw new Exception("Database Name is not exist.");
            }

            IMongoDatabase database = conn.GetDatabase(DatabaseName);
            MongoDB.Driver.GridFS.GridFSBucket bucket;
            if (String.IsNullOrEmpty(GridFSName))
                bucket = new MongoDB.Driver.GridFS.GridFSBucket(database);
            else
                bucket = new MongoDB.Driver.GridFS.GridFSBucket(database, new GridFSBucketOptions { BucketName = GridFSName });
            bucket.Drop();

            return;

        }

    }


}
