using System;
using System.Collections.Generic;
using System.Text;

namespace DatabaseMaster2
{

    public class DBCommandConvert
    {
        //get nowdate function
        public static String GetSQLDate(String Type)
        {
            if (Type.Contains("MSSQL"))
                return "getdate()";
            else if (Type.Contains("Oracle"))
                return "sysdate ";
            else if (Type.Contains("MySQL"))
                return "curdate() ";
            else if (Type.Contains("Access"))
                return "now()";
            else
                return "";
        }

        public static String GetTopRecords(String Command, String TopRecord, DBCommandFactory type)
        {
            switch (type)
            {
                case DBCommandFactory.SQLServer:
                    Command = Command.Replace("select", "select Top " + TopRecord);
                    return Command;
                case DBCommandFactory.Oracle:
                    if (Command.Contains("where"))
                        Command += "and rownum <=" + TopRecord;
                    else
                        Command += "rownum <=" + TopRecord;
                    return Command;
                case DBCommandFactory.MySQL:
                    Command += "limit " + TopRecord;
                    return Command;
                case DBCommandFactory.Access:
                    Command = Command.Replace("select", "Select Top " + TopRecord);
                    return Command;
                default:
                    return "";
            }
        }

    }

}
