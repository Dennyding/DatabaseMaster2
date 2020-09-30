using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using  DatabaseMaster2;
using DbType = DatabaseMaster2.DbType;

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
                        StringEncrypt.EncryptType.MD5),
                    DBType = DbType.MSSQL,
                    IsAutoCloseConnection = true
                });

                data.Queryable.Data("Table").ToDataTable();
                data.Insertable.Data("Table", new string[] {"C1" }, new object[] {1 });
                data.Updateable.Data("Table", new string[] { "C1" }, new object[] { 1 },"id",1);
                data.Deleteable.Data("Table", "id",1);

                DatabaseMasterNOSQL mongodb = new DatabaseMasterNOSQL(new ConnectionConfig()
                {
                    ConnectionString = DatabaseConfigXml.GetDbConfigXml("DatabaseConfig.xml", "MongoDB",
                        "ConnectString", StringEncrypt.EncryptType.MD5),
                    IsAutoCloseConnection = true,
                    DatabaseName = "DurabilityTest"
                });

                mongodb.Queryable.Data("Table").ToDataTable();
                mongodb.Insertable.Data("Table", new string[] { "C1" }, new object[] { 1 });
                mongodb.Updateable.Data("Table", new string[] { "C1" }, new object[] { 1 }, "id", 1);
                mongodb.Deleteable.Data("Table", "id", 1);

            }
            catch (Exception e)
            {
                Console.WriteLine(e);
            }
        }
    }
}
