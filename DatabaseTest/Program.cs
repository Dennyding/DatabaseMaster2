using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using  DatabaseMaster2;

namespace DatabaseTest
{
    class Program
    {
        static void Main(string[] args)
        {
            try
            {
                //xml string
                DatabaseConfigXml.SetStringEncrypt("abcdefgh");
                StringEncrypt.DataEncrypt(StringEncrypt.EncryptType.MD5,
                    "server=135.251.245.8;Data Source=Test;uid=HR;pwd=hr;");

                DatabaseConfigXml.SetStringEncrypt();
                DatabaseMaster data = new DatabaseMaster(new ConnectionConfig()
                {
                    //ConnectionString = "server=135.251.245.8;Data Source=Test;uid=HR;pwd=hr;";
                    ConnectionString = DatabaseConfigXml.GetDbConfigXml("DatabaseConfig.xml", "MSSQL", "ConnectString",
                        StringEncrypt.EncryptType.None),
                    DBType = DatabaseType.MSSQL,
                    IsAutoCloseConnection = false
                });

                //data.Connect();
                //DataTable dt = data.Queryable().Data("T1111").ToDataTable();
                //ParameterClass[] input = new ParameterClass[2];
                //input[0].ParameterName = "@UserName";
                //input[0].Value = "";
                //input[1].ParameterName = "@Password";
                //input[1].Value = "";
                //ParameterOutClass[] output = new ParameterOutClass[1];
                //output[0].ParameterName = "@IsVaild";
                //output[0].SqlDbType = SqlDbType.Int;
                //data.Queryable().Procedure("CheckUserLoginVaild", input, output);
                //data.Insertable().Data("Table", new string[] { "C1" }, new object[] { 1 });
                //data.Updateable().Data("Table", new string[] { "C1" }, new object[] { 1 }, "id", 1);
                //data.Deleteable().Data("Table", "id", 1);
                //data.Close();

                DatabaseMasterMongoDB mongodb = new DatabaseMasterMongoDB(new ConnectionConfig()
                {
                    ConnectionString = DatabaseConfigXml.GetDbConfigXml("DatabaseConfig.xml", "MongoDB",
                        "ConnectString", StringEncrypt.EncryptType.None),
                    IsAutoCloseConnection = true

                });

                //List<Hashtable> hr = mongodb.Queryable("DurabilityTest").Data("Summary").ToListHashTable();

                //mongodb.Connect();
                //DataTable dt1 = mongodb.Queryable("DurabilityTest").Data("Data2").ToDataTable();
                //DataTable dt2 = mongodb.Queryable("DurabilityTest").Data("Data1", "VOLT电压", 38, CommandComparison.GreaterOrEquals).ToDataTable();
                //mongodb.Close();
                //mongodb.Queryable("DurabilityTest").Data("Summary", new string[] { "开始时间/TestStartTime" }, new CommandComparison[]{CommandComparison.GreaterOrEquals
                //}, new object[] { Convert.ToDateTime("2020-10-25 10:06:00") }, new WhereRelation[] { WhereRelation.None }).ToDataTable();

                //mongodb.Queryable("DurabilityTest").Data("Table").ToDataTable();
                //mongodb.Insertable("DurabilityTest").Data("Summary", new string[] { "开始时间/TestStartTime" }, new object[] { Convert.ToDateTime("2020-10-25 10:05:32") });
                //mongodb.Updateable("DurabilityTest").Data("Summary", new string[] { "开始时间/TestStartTime" }, new object[] { Convert.ToDateTime("2020-10-25 10:06:00") },"_id",CommandComparison.Is,"5f94f387ed02f4ba4007e0c5");
                //mongodb.Deleteable("DurabilityTest").Data("Table", "id", 1);

                //String id = mongodb.FileManage("DurabilityTest").UploadFile("D:\\Downloads\\011002000411-31504817.pdf", "EXCEL");
                //mongodb.FileManage("DurabilityTest").ReNameFile(id, "dingchen.pdf", "EXCEL");
                //mongodb.FileManage("DurabilityTest").DownloadFile(id, "D:\\Downloads\\dingchen.pdf", "EXCEL");
                //mongodb.FileManage("DurabilityTest").DeleteFile(id, "EXCEL");
                //mongodb.FileManage("DurabilityTest").DeleteGridFS("EXCEL");

                DatabaseMasterRedis redis = new DatabaseMasterRedis(new ConnectionConfig()
                {
                    ConnectionString = DatabaseConfigXml.GetDbConfigXml("DatabaseConfig.xml", "Redis",
        "ConnectString", StringEncrypt.EncryptType.None),
                    IsAutoCloseConnection = true

                });

                //redis.Writeable().Data("Test", "12345");
                //redis.Readable().Data<Int32>("Test");
                //redis.Writeable().Delete(new string[]{"Test"});
            }
            catch (Exception e)
            {
                Console.WriteLine(e);
            }
        }
    }
}
