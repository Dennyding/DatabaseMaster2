using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace DatabaseMaster2
{
   public enum DatabaseType
    {
        MSSQL=1,
        MYSQL=2,
        Oracle=3,
        OleDB=4,
        SQLite=5,
        PostgreSQL=6,
        Access = 7,
        PinusDB=8
    }

    public class DBFactory
    {
        public static DatabaseInterface CreateDatabase(DatabaseType dbType,String ConnString)
        {
          
            switch (dbType)
            {
                case DatabaseType.MSSQL:
                    return new SQLServerDatabase(ConnString, true);
                case DatabaseType.MYSQL:
                    return new MYSQLDatabase(ConnString, true);
                case DatabaseType.Oracle:
                    return new OracleDatabase(ConnString, true);
                case DatabaseType.OleDB:
                    return new OleDBDatabase(ConnString, true);
                case DatabaseType.SQLite:
                    return new SQLiteDatabase(ConnString, true);
                case DatabaseType.PostgreSQL:
                    return new PostgreSQL(ConnString, true);
                case DatabaseType.Access:
                    return new OleDBDatabase(ConnString, true);
                case DatabaseType.PinusDB:
                    return new PinusDatabase(ConnString, true);
                default:
                    return new SQLServerDatabase(ConnString, true);
            }
        }
    }
}
