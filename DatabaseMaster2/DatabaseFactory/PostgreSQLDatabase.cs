using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data;
using System.Data.SqlClient;
using System.Data.SqlTypes;
using Npgsql;
using NpgsqlTypes;

namespace DatabaseMaster2
{
    public class PostgreSQL : DatabaseInterface
    {

        NpgsqlConnection conn = null;
        String ConnectString = "";

        /// <GetConnectionString>
        /// GetConnectionString
        /// </GetConnectionString>
        /// <returns></returns>
        public PostgreSQL()
        {
            ConnectString= XmlStream.getXmlValue(AppDomain.CurrentDomain.BaseDirectory+"DatabaseConfig.xml", "PostgreSQL", "ConnectString");
        }

        /// <GetConnectionString>
        /// GetConnectionString
        /// </GetConnectionString>
        /// <returns></returns>
        public PostgreSQL(String ConnString, Boolean ConnStr)
        {
            ConnectString = ConnString;
        }

        /// <GetConnectionString>
        /// GetConnectionString
        /// </GetConnectionString>
        /// <returns></returns>
        public PostgreSQL(String ConnectName)
        {
            ConnectString = XmlStream.getXmlValue(AppDomain.CurrentDomain.BaseDirectory+"DatabaseConfig.xml", ConnectName, "ConnectString");
        }

        /// <GetConnectionString>
        /// GetConnectionString
        /// </GetConnectionString>
        /// <returns></returns>
        public PostgreSQL(String ConnectName, StringEncrypt.EncryptType type)
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

            conn = new NpgsqlConnection(ConnectString);
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

            NpgsqlCommand cmd = new NpgsqlCommand(Command,conn);

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

            NpgsqlCommand cmd = new NpgsqlCommand(Command, conn);
            
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

            NpgsqlCommand cmd = new NpgsqlCommand(Command, conn);

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

            NpgsqlCommand cmd = new NpgsqlCommand(Command, conn);

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

            NpgsqlDataAdapter da = new NpgsqlDataAdapter(Command, conn);

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

            NpgsqlDataAdapter da = new NpgsqlDataAdapter(Command, conn);
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

            NpgsqlCommand cmd = new NpgsqlCommand(Command, conn);

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
            Int32 Value=0;

            if (conn == null)
            {
                throw new Exception("Connection has not initialize.");
            }

            if (conn.State != ConnectionState.Open)
            {
                throw new Exception("Connection has not opened.");
            }

            NpgsqlTransaction tran = conn.BeginTransaction();

            if (Command.Length == 0)
                return 0;

            try
            {
                NpgsqlCommand cmd = new NpgsqlCommand();
                for (int i = 0; i < Command.Length; i++)
                {
                    cmd.CommandText = Command[i];
                    cmd.Connection = conn;

                    //add parameter
                    cmd.CommandTimeout = Timeout;

                    cmd.Transaction = tran;
                    Value = cmd.ExecuteNonQuery();
                }

                tran.Commit();

                cmd.Dispose();
            }
            catch (Exception)
            {
                tran.Rollback();
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

            NpgsqlCommand cmd = new NpgsqlCommand(Command, conn);

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

            NpgsqlCommand cmd = new NpgsqlCommand(StoreProCommand, conn);

            //add parameter
            cmd.CommandTimeout = Timeout;
            cmd.CommandType = CommandType.StoredProcedure;

            //add parameter
            String[] values = new String[ParameterOut.Length];

            for (int number = 0; number < ParameterOut.Length; number++)
            {
                NpgsqlParameter parameter = new NpgsqlParameter
                {
                    ParameterName = ParameterOut[number].ParameterName,
                    NpgsqlDbType = (NpgsqlDbType)ParameterOut[number].NpgsqlDbType,
                    Direction = ParameterDirection.Output
                };
                cmd.Parameters.Add(parameter);
            }
            //add parameter
            for (int number = 0; number < ParameterIn.Length; number++)
            {
                cmd.Parameters.AddWithValue(ParameterIn[number].ParameterName, ParameterIn[number].Value);
            }

            cmd.ExecuteNonQuery();
            
            //get output
            for (int number = 0; number < ParameterOut.Length; number++)
            {
                values[number]=Convert.ToString(cmd.Parameters[ParameterOut[number].ParameterName].Value);
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

            NpgsqlCommand cmd = new NpgsqlCommand(StoreProCommand, conn);

            //add parameter
            cmd.CommandTimeout = Timeout;
            cmd.CommandType = CommandType.StoredProcedure;

            //add parameter
            String[] values = new String[ParameterOut.Length];

            //add parameter
            for (int number = 0; number < ParameterIn.Length; number++)
            {
                cmd.Parameters.AddWithValue(ParameterIn[number].ParameterName,ParameterIn[number].Value);
            }

            for (int number = 0; number < ParameterOut.Length; number++)
            {
                NpgsqlParameter parameter = new NpgsqlParameter
                {
                    ParameterName = ParameterOut[number].ParameterName,
                    NpgsqlDbType = (NpgsqlDbType)ParameterOut[number].NpgsqlDbType,
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

            NpgsqlDataAdapter da = new NpgsqlDataAdapter(StoreProCommand, conn);
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
