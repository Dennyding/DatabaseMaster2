using System;
using System.Collections.Generic;
using System.Text;

namespace DatabaseMaster2
{

    public class DBCommandConvert
    {
        //get nowdate function
        public static String GetSQLDate(DatabaseType dbType)
        {
            if (dbType==DatabaseType.MSSQL)
                return "getdate()";
            else if (dbType == DatabaseType.Oracle)
                return "sysdate";
            else if (dbType == DatabaseType.MYSQL)
                return "curdate()";
            else if (dbType == DatabaseType.OleDB)
                return "now()";
            else
                return "";
        }

        public static String GetTopRecords(String Command, String TopRecord, DatabaseType type)
        {
            switch (type)
            {
                case DatabaseType.MSSQL:
                    Command = Command.Replace("select", "select Top " + TopRecord);
                    return Command;
                case DatabaseType.Oracle:
                    if (Command.Contains("where"))
                        Command += "and rownum <=" + TopRecord;
                    else
                        Command += "rownum <=" + TopRecord;
                    return Command;
                case DatabaseType.MYSQL:
                    Command += "limit " + TopRecord;
                    return Command;
                case DatabaseType.Access:
                    Command = Command.Replace("select", "Select Top " + TopRecord);
                    return Command;
                default:
                    return "";
            }
        }

    }

}
