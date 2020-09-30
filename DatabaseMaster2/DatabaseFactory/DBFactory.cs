using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace DatabaseMaster2
{
   public enum DbType
    {
        MSSQL=1,
        MYSQL=2,
        Oracle=3,
        OleDB=4,
        SQLite=5,
        PostgreSQL=6
    }

    public class DBFactory
    {
        public static DatabaseInterface CreateDatabase(DbType dbType,String ConnString)
        {
          
            switch (dbType)
            {
                case DbType.MSSQL:
                    return new SQLServerDatabase(ConnString, true);
                case DbType.MYSQL:
                    return new MYSQLDatabase(ConnString, true);
                case DbType.Oracle:
                    return new OracleDatabase(ConnString, true);
                case DbType.OleDB:
                    return new OleDBDatabase(ConnString, true);
                case DbType.SQLite:
                    return new SQLiteDatabase(ConnString, true);
                case DbType.PostgreSQL:
                    return new PostgreSQL(ConnString, true);
                default:
                    return new SQLServerDatabase(ConnString, true);
            }
        }
    }
}
