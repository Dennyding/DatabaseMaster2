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

                data.Connect();
                DataTable dt1 = data.Queryable().Data("T1111").ToDataTable();
                ParameterClass[] input=new ParameterClass[2];
                input[0].ParameterName = "@UserName";
                input[0].Value = "";
                input[1].ParameterName = "@Password";
                input[1].Value = "";
                ParameterOutClass[] output = new ParameterOutClass[1];
                output[0].ParameterName = "@IsVaild";
                output[0].SqlDbType = SqlDbType.Int;
                data.Queryable().Procedure("CheckUserLoginVaild", input,output);
                data.Insertable().Data("Table", new string[] { "C1" }, new object[] { 1 });
                data.Updateable().Data("Table", new string[] { "C1" }, new object[] { 1 }, "id", 1);
                data.Deleteable().Data("Table", "id", 1);
                data.Close();

                DatabaseMasterNOSQL mongodb = new DatabaseMasterNOSQL(new ConnectionConfig()
                {
                    ConnectionString = DatabaseConfigXml.GetDbConfigXml("DatabaseConfig.xml", "MongoDB",
                        "ConnectString", StringEncrypt.EncryptType.MD5),
                    IsAutoCloseConnection = true,
                    DatabaseName = "DurabilityTest"
                });

                mongodb.Connect();
                DataTable dt1 = mongodb.Queryable("DurabilityTest").Data("Data2").ToDataTable();
                DataTable dt2 = mongodb.Queryable("DurabilityTest").Data("Data1", "VOLT电压", 38,CommandComparison.GreaterOrEquals).ToDataTable();
                mongodb.Close();
                mongodb.Queryable("DurabilityTest").Data("Table").ToDataTable();
                mongodb.Insertable("DurabilityTest").Data("Table", new string[] { "C1" }, new object[] { 1 });
                mongodb.Updateable("DurabilityTest").Data("Table", new string[] { "C1" }, new object[] { 1 }, "id", 1);
                mongodb.Deleteable("DurabilityTest").Data("Table", "id", 1);
