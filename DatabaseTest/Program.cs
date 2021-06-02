using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.IO;
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
                //DatabaseConfigXml.SetStringEncrypt("abcdefgh");
                //StringEncrypt.DataEncrypt(StringEncrypt.EncryptType.MD5,
                //    "server=135.251.245.8;Data Source=Test;uid=HR;pwd=hr;");

                DatabaseConfigXml.SetStringEncrypt();
                DatabaseMaster data = new DatabaseMaster(
                    new ConnectionConfig()
                    {
                        ConnectionString = "Data Source=127.0.0.1;database=LabelMaster2;uid=Test;pwd=ioedu#2345",
                        //ConnectionString =
                        //    DatabaseConfigXml.GetDbConfigXml(
                        //            "DatabaseConfig.xml",
                        //            "SQLite",
                        //            "ConnectString",
                        //            StringEncrypt.EncryptType.None),
                        DBType = DatabaseType.MSSQL.ToString(),
                        IsAutoCloseConnection = true
                    });

                //FileInfo fi = new FileInfo("D:\\Downloads\\MU400_SGWM_E50_V4.0.dbc");
                //byte[] buff = new byte[fi.Length];

                //FileStream fs = fi.OpenRead();
                //fs.Read(buff, 0, Convert.ToInt32(fs.Length));
                //fs.Close();

                //ParameterINClass[] para = new ParameterINClass[1];
                //para[0].SqlDbType = SqlDbType.Binary;
                //para[0].ParameterName = "@File";
                //para[0].Value = buff;
                //data.Insertable("Label_Table").Data(new String[] { "Label_Name", "Lable_TypeID", "Label_Desp", "Label_CreateTime", "Label_Creater", "Label_File" , "Label_Enable" }
                //, new Object[] {"AA",1,"a",DateTime.Now,"A","@File",1 }).ExecuteParameterCommand(para);
                //data.Updateable("Label_Table").Data(new String[] { "Label_File" }, new Object[] {"@File" }).Where("Label_ID", 1).ExecuteCommand(para);

                //DataTable dt = data.Queryable("Label_Table").Columns().Data().ToDataTable();

                //byte[] buff = (Byte[])dt.Rows[0]["Label_File"];

                //FileStream fs = new FileStream("D:\\Downloads\\aa.dbc", FileMode.CreateNew);
                //BinaryWriter bw = new BinaryWriter(fs);
                //bw.Write(buff, 0, buff.Length);
                //bw.Close();
                //fs.Close();

                //data.Queryable("TestCase_List_Table")
                //    .Columns()
                //   .Where("Product_ID", 1)
                //   .Where("TestCase_List_SequenceNo", 4)
                //   .Data().ToDataTable();
                //data.Updateable("TestCase_List_Table")
                //    .Where("Product_ID", 1)
                //    .Where("TestCase_List_SequenceNo", 4)
                //    .Data(new String[] { "TestCase_List_PassFail" }, new Object[] { 0 })
                //    .ExecuteCommand();



                DatabaseMasterMongoDB mongodb = new DatabaseMasterMongoDB(new ConnectionConfig()
                {
                    ConnectionString = DatabaseConfigXml.GetDbConfigXml("DatabaseConfig.xml", "MongoDBName",
                        "ConnectString", StringEncrypt.EncryptType.MD5),
                    IsAutoCloseConnection = true

                });

                //List<Hashtable> hr = mongodb.Queryable("DurabilityTest").Data("Summary").ToListHashTable();


                //mongodb.Queryable("DurabilityTest").TableLink("T1111", new string[] {"_id"},
                //    new CommandComparison[] {CommandComparison.Is}, new object[] {"5f929ac3023a36f332afd822"},
                //    new WhereRelation[] {WhereRelation.None}, new List<string> {"Serial样机编号", "Time时间"},
                //    new List<string> { }, "Summary", "Serial样机编号", "Serial样机编号", "Summary").ToDataTable();
                //mongodb.Connect();
                //DataTable dt1 = mongodb.Queryable("DurabilityTest", "T111111").Data().ToDataTable();
                //DataTable dt2 = mongodb.Queryable("DurabilityTest", "T111111").Where( "VOLT电压", 38, CommandComparison.GreaterOrEquals).Data().ToDataTable();
                //mongodb.Close();
              


                //String id = mongodb.FileManage("DurabilityTest").UploadFile("D:\\Downloads\\011002000411-31504817.pdf", "EXCEL");
                //mongodb.FileManage("DurabilityTest").ReNameFile(id, "dingchen.pdf", "EXCEL");
                //mongodb.FileManage("DurabilityTest").DownloadFile(id, "D:\\Downloads\\dingchen.pdf", "EXCEL");
                //mongodb.FileManage("DurabilityTest").DeleteFile(id, "EXCEL");
                //mongodb.FileManage("DurabilityTest").DeleteGridFS("EXCEL");

                //        DatabaseMasterRedis redis = new DatabaseMasterRedis(new ConnectionConfig()
                //        {
                //            ConnectionString = DatabaseConfigXml.GetDbConfigXml("DatabaseConfig.xml", "Redis",
                //"ConnectString", StringEncrypt.EncryptType.None),
                //            IsAutoCloseConnection = true

                //        });

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
