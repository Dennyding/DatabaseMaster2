# DatabaseMaster2
.NET ORM
A simple universal database access interface. Support sql server, mysql, Oracle, sqlite, PostgreSQL. It also supoort NoSQL:MongoDB and Redis.

This is the second version for DatabaseMaster. If your want to join, please contact with my email:denny.ding @ hotmail.com.

                //xml string
                DatabaseConfigXml.SetStringEncrypt("abcdefgh");
                StringEncrypt.DataEncrypt(StringEncrypt.EncryptType.MD5,
                    "server=135.251.245.8;Data Source=Test;uid=HR;pwd=hr;");

                DatabaseConfigXml.SetStringEncrypt();
                DatabaseMaster data = new DatabaseMaster(new ConnectionConfig()
                {
                    ConnectionString = "server=135.251.245.8;Data Source=Test;uid=HR;pwd=hr;";
                    //ConnectionString = DatabaseConfigXml.GetDbConfigXml("DatabaseConfig.xml", "MSSQL", "ConnectString",
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
