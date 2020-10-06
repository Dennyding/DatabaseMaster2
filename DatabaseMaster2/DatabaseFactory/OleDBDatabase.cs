using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data;
using System.Data.OleDb;

namespace DatabaseMaster2
{
    public class OleDBDatabase:DatabaseInterface
    {

        OleDbConnection conn = null;
        String ConnectString = "";

        /// <GetConnectionString>
        /// GetConnectionString
        /// </GetConnectionString>
        /// <returns></returns>
        public OleDBDatabase()
        {
            ConnectString= XmlStream.getXmlValue(AppDomain.CurrentDomain.BaseDirectory+"DatabaseConfig.xml", "OleDB", "ConnectString");
        }

        /// <GetConnectionString>
        /// GetConnectionString
        /// </GetConnectionString>
        /// <returns></returns>
        public OleDBDatabase(String ConnString, Boolean ConnStr)
        {
            ConnectString = ConnString;
        }

        /// <GetConnectionString>
        /// GetConnectionString
        /// </GetConnectionString>
        /// <returns></returns>
        public OleDBDatabase(String ConnectName)
        {
            ConnectString = XmlStream.getXmlValue(AppDomain.CurrentDomain.BaseDirectory+"DatabaseConfig.xml", ConnectName, "ConnectString");
        }

        /// <GetConnectionString>
        /// GetConnectionString
        /// </GetConnectionString>
        /// <returns></returns>
        public OleDBDatabase(String ConnectName, StringEncrypt.EncryptType type)
        {
            String EncryptString = XmlStream.getXmlValue(AppDomain.CurrentDomain.BaseDirectory+"DatabaseConfig.xml", ConnectName, "ConnectString");

            if (type != StringEncrypt.EncryptType.None)
                ConnectString = StringEncrypt.DataDecrypt(type, EncryptString);
            else
                ConnectString = EncryptString;
        }

        /// <summary>
        /// Open database with connectstring
        /// </summary>
        public  void Open()
        {
            if (conn != null && conn.State == ConnectionState.Open)
            {
                ;
            }

            conn = new OleDbConnection(ConnectString);
            conn.Open();
        }

        /// <summary>
        /// close database with connectstring
        /// </summary>
        public  void Close()
        {
            if (conn == null)
            {
                throw new Exception("Connection has not initialize.");
            }

            if (conn.State == ConnectionState.Closed)
            {
                throw new Exception("Connection is already closed.");
            }

            conn.Close();
            conn.Dispose();
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

            if (conn.State == ConnectionState.Closed)
            {
                return false;
            }

            return true;
        }

        /// <summary>
        /// get first row and first colunm value from database
        /// </summary>
        /// <param name="Command">SQL command String</param>
        /// <param name="Timeout">[option] timeout of database connect(seconds)</param>
        /// <returns>
        /// object value
        /// </returns>
        public  object GetSpeciaRecordValue( String Command, Int32 Timeout=30)
        {
            object Value;

            if (conn == null)
            {
                throw new Exception("Connection has not initialize.");
            }

            if (conn.State != ConnectionState.Open)
            {
                throw new Exception("Connection has not opened.");
            }

            OleDbCommand cmd = new OleDbCommand(Command,conn);

            cmd.CommandTimeout = Timeout;
            Value=cmd.ExecuteScalar();
            cmd.Dispose();

            return Value;
        }

        /// <summary>
        /// get first row and first colunm value from database with Parameter
        /// </summary>
        /// <param name="Command">SQL command String</param>
        /// <param name="Parameter">Parameter</param>
        /// <param name="Timeout">[option] timeout of database connect(seconds)</param>
        /// <returns>
        /// object value
        /// </returns>
        public object GetSpeciaRecordValue( String Command, ParameterClass[] Parameter, Int32 Timeout = 30)
        {
            object Value;

            if (conn == null)
            {
                throw new Exception("Connection has not initialize.");
            }

            if (conn.State != ConnectionState.Open)
            {
                throw new Exception("Connection has not opened.");
            }

            OleDbCommand cmd = new OleDbCommand(Command, conn);
            
            //add parameter
            cmd.CommandTimeout = Timeout;
            for (int number = 0; number < Parameter.Length; number++)
            {
                cmd.Parameters.AddWithValue(Parameter[number].ParameterName,Parameter[number].Value);
            }
            
            Value = cmd.ExecuteScalar();
            cmd.Dispose();

            return Value;
        }

        /// <summary>
        /// return a datareader from database
        /// </summary>
        /// <param name="Command">SQL command String</param>
        /// <param name="Timeout">[option] timeout of database connect(seconds)</param>
        /// <returns>
        /// DataReader value
        /// </returns>
        public IDataReader GetDataReaderList( String Command, Int32 Timeout = 30)
        {
            IDataReader Value;

            if (conn == null)
            {
                throw new Exception("Connection has not initialize.");
            }

            if (conn.State != ConnectionState.Open)
            {
                throw new Exception("Connection has not opened.");
            }

            OleDbCommand cmd = new OleDbCommand(Command, conn);

            //add parameter
            cmd.CommandTimeout = Timeout;

            Value = cmd.ExecuteReader();
            cmd.Dispose();

            return Value;
        }

        /// <summary>
        /// return a datareader from database
        /// </summary>
        /// <param name="Command">SQL command String</param>
        /// <param name="Parameter">Parameter</param>
        /// <param name="Timeout">[option] timeout of database connect(seconds)</param>
        /// <returns>
        /// DataReader value
        /// </returns>
        public IDataReader GetDataReaderList(String Command, ParameterClass[] Parameter, Int32 Timeout = 30)
        {
            IDataReader Value;

            if (conn == null)
            {
                throw new Exception("Connection has not initialize.");
            }

            if (conn.State != ConnectionState.Open)
            {
                throw new Exception("Connection has not opened.");
            }

            OleDbCommand cmd = new OleDbCommand(Command, conn);

            //add parameter
            cmd.CommandTimeout = Timeout;
            //add parameter
            for (int number = 0; number < Parameter.Length; number++)
            {
                cmd.Parameters.AddWithValue(Parameter[number].ParameterName, Parameter[number].Value);
            }

            Value = cmd.ExecuteReader();
            cmd.Dispose();

            return Value;
        }

        /// <summary>
        /// return a dataset from database
        /// </summary>
        /// <param name="Command">SQL command String</param>
        /// <param name="Timeout">[option] timeout of database connect(seconds)</param>
        /// <returns>
        /// dataset value
        /// </returns>
        public DataSet GetDataSet( String Command, Int32 Timeout = 30)
        {
            if (conn == null)
            {
                throw new Exception("Connection has not initialize.");
            }

            if (conn.State != ConnectionState.Open)
            {
                throw new Exception("Connection has not opened.");
            }

            OleDbDataAdapter da = new OleDbDataAdapter(Command, conn);

            da.SelectCommand.CommandTimeout = Timeout;
            DataSet ds = new DataSet();
            da.Fill(ds);
            da.Dispose();

            return ds;

        }

        /// <summary>
        /// return a dataset from database with Parameter
        /// </summary>
        /// <param name="Command">SQL command String</param>
        /// <param name="Parameter">Parameter</param>
        /// <param name="Timeout">[option] timeout of database connect(seconds)</param>
        /// <returns>
        /// dataset value
        /// </returns>
        public DataSet GetDataSet( String Command, ParameterClass[] Parameter, Int32 Timeout = 30)
        {
            if (conn == null)
            {
                throw new Exception("Connection has not initialize.");
            }

            if (conn.State != ConnectionState.Open)
            {
                throw new Exception("Connection has not opened.");
            }

            OleDbDataAdapter da = new OleDbDataAdapter(Command, conn);
            da.SelectCommand.CommandTimeout = Timeout;

            //add parameter
            for (int number = 0; number < Parameter.Length; number++)
            {
                da.SelectCommand.Parameters.AddWithValue(Parameter[number].ParameterName, Parameter[number].Value);
            }

            DataSet ds = new DataSet();
            da.Fill(ds);
            da.Dispose();

            return ds;
        }

        /// <summary>
        /// Execue sql Command
        /// </summary>
        /// <param name="Command">SQL command String</param>
        /// <param name="Timeout">[option] timeout of database connect(seconds)</param>
        /// <returns>
        /// The number of rows affected 
        /// </returns>
        public Int32 ExecueCommand( String Command, Int32 Timeout = 30)
        {
            Int32 Value;

            if (conn == null)
            {
                throw new Exception("Connection has not initialize.");
            }

            if (conn.State != ConnectionState.Open)
            {
                throw new Exception("Connection has not opened.");
            }

            OleDbCommand cmd = new OleDbCommand(Command, conn);

            //add parameter
            cmd.CommandTimeout = Timeout;

            Value = cmd.ExecuteNonQuery();
            cmd.Dispose();

            return Value;
        }

        /// <summary>
        /// Execue sql Command
        /// </summary>
        /// <param name="Command">SQL command String</param>
        /// <param name="Timeout">[option] timeout of database connect(seconds)</param>
        /// <returns>
        /// The number of rows affected 
        /// </returns>
        public Int32 ExecueTransactionCommand(String[] Command, Int32 Timeout = 30)
        {
            Int32 Value = 0;

            if (conn == null)
            {
                throw new Exception("Connection has not initialize.");
            }

            if (conn.State != ConnectionState.Open)
            {
                throw new Exception("Connection has not opened.");
            }



            for (int i = 0; i < Command.Length; i++)
            {
                OleDbCommand cmd = new OleDbCommand(Command[i], conn);

                //add parameter
                cmd.CommandTimeout = Timeout;

                Value = cmd.ExecuteNonQuery();

                cmd.Dispose();
            }

            return Value;
        }


        /// <summary>
        /// Execue sql Command With Parameter
        /// </summary>
        /// <param name="Command">SQL command String</param>
        /// <param name="Parameter">Parameter</param>
        /// <param name="Timeout">[option] timeout of database connect(seconds)</param>
        /// <returns>
        /// The number of rows affected 
        /// </returns>
        public Int32 ExecueCommand( String Command, ParameterClass[] Parameter, Int32 Timeout = 30)
        {
            Int32 Value;

            if (conn == null)
            {
                throw new Exception("Connection has not initialize.");
            }

            if (conn.State != ConnectionState.Open)
            {
                throw new Exception("Connection has not opened.");
            }

            OleDbCommand cmd = new OleDbCommand(Command, conn);

            //add parameter
            cmd.CommandTimeout = Timeout;
            for (int number = 0; number < Parameter.Length; number++)
            {
                cmd.Parameters.AddWithValue(Parameter[number].ParameterName, Parameter[number].Value);
            }

            Value = cmd.ExecuteNonQuery();
            cmd.Dispose();

            return Value;
        }

        /// <summary>
        /// Execue sql Command With Parameter
        /// </summary>
        /// <param name="StoreProCommand">SQL command String</param>
        /// <param name="ParameterIn">Parameter of Input</param>
        ///  <param name="ParameterOut">Parameter of Output</param>
        /// <param name="Timeout">[option] timeout of database connect(seconds)</param>
        /// <returns>
        /// The parameter of output
        /// </returns>
        public  String[] ExecueStoreProCommand( String StoreProCommand,
            ParameterClass[] ParameterIn, ParameterOutClass[] ParameterOut, Int32 Timeout = 30)
        {
            if (conn == null)
            {
                throw new Exception("Connection has not initialize.");
            }

            if (conn.State != ConnectionState.Open)
            {
                throw new Exception("Connection has not opened.");
            }

            OleDbCommand cmd = new OleDbCommand(StoreProCommand, conn);

            //add parameter
            cmd.CommandTimeout = Timeout;
            cmd.CommandType = CommandType.StoredProcedure;

            //add parameter
            String[] values = new String[ParameterOut.Length];


            //add parameter
            for (int number = 0; number < ParameterIn.Length; number++)
            {
                cmd.Parameters.AddWithValue(ParameterIn[number].ParameterName, ParameterIn[number].Value);
            }

            for (int number = 0; number < ParameterOut.Length; number++)
            {
                OleDbParameter parameter = new OleDbParameter
                {
                    ParameterName = ParameterOut[number].ParameterName,
                    DbType = ParameterOut[number].SqliteDbType,
                    Direction = ParameterDirection.Output
                };
                cmd.Parameters.Add(parameter);
            }

            cmd.ExecuteNonQuery();

            //get output
            for (int number = 0; number < ParameterOut.Length; number++)
            {
                values[number] = Convert.ToString(cmd.Parameters[ParameterOut[number].ParameterName].Value);
            }

            cmd.Dispose();

            return values;
        }

        /// <summary>
        /// Execue sql Command With Parameter
        /// </summary>
        /// <param name="StoreProCommand">SQL command String</param>
        /// <param name="ParameterIn">Parameter of Input</param>
        ///  <param name="ParameterOut">Parameter of Output</param>
        /// <param name="Timeout">[option] timeout of database connect(seconds)</param>
        /// <returns>
        /// The parameter of output
        /// </returns>
        public String[] ExecueStoreProCommand(String StoreProCommand,
            ParameterClass[] ParameterIn, ParameterReturnClass[] ParameterOut, Int32 Timeout = 30)
        {
            if (conn == null)
            {
                throw new Exception("Connection has not initialize.");
            }

            if (conn.State != ConnectionState.Open)
            {
                throw new Exception("Connection has not opened.");
            }

            OleDbCommand cmd = new OleDbCommand(StoreProCommand, conn);

            //add parameter
            cmd.CommandTimeout = Timeout;
            cmd.CommandType = CommandType.StoredProcedure;

            //add parameter
            String[] values = new String[ParameterOut.Length];


            //add parameter
            for (int number = 0; number < ParameterIn.Length; number++)
            {
                cmd.Parameters.AddWithValue(ParameterIn[number].ParameterName, ParameterIn[number].Value);
            }

            for (int number = 0; number < ParameterOut.Length; number++)
            {
                OleDbParameter parameter = new OleDbParameter
                {
                    ParameterName = ParameterOut[number].ParameterName,
                    Direction = ParameterDirection.ReturnValue
                };
                cmd.Parameters.Add(parameter);
            }

            cmd.ExecuteNonQuery();

            //get output
            for (int number = 0; number < ParameterOut.Length; number++)
            {
                values[number] = Convert.ToString(cmd.Parameters[ParameterOut[number].ParameterName].Value);
            }


            cmd.Dispose();

            return values;
        }

        /// <summary>
        /// Execue sql Command With Parameter
        /// </summary>
        /// <param name="StoreProCommand">SQL command String</param>
        /// <param name="ParameterIn">Parameter of Input</param>
        /// <param name="Timeout">[option] timeout of database connect(seconds)</param>
        /// <returns>
        /// dataset value
        /// </returns>
        public  DataSet ExecueStoreProCommand( String StoreProCommand,
            ParameterClass[] ParameterIn, Int32 Timeout = 30)
        {
            if (conn == null)
            {
                throw new Exception("Connection has not initialize.");
            }

            if (conn.State != ConnectionState.Open)
            {
                throw new Exception("Connection has not opened.");
            }

            OleDbDataAdapter da = new OleDbDataAdapter(StoreProCommand, conn);
            da.SelectCommand.CommandTimeout = Timeout;
            da.SelectCommand.CommandType = CommandType.StoredProcedure;

            //add parameter
            for (int number = 0; number < ParameterIn.Length; number++)
            {
                da.SelectCommand.Parameters.AddWithValue(ParameterIn[number].ParameterName, ParameterIn[number].Value);
            }

            DataSet ds = new DataSet();
            da.Fill(ds);
            da.Dispose();

            return ds;
        }
    }
}
