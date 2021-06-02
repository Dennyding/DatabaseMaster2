using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using System.Text;
using System.Data;
using PDB.DotNetSDK;


namespace DatabaseMaster2
{
    public class PinusDatabase : DatabaseInterface
    {

        PDBConnection conn = null;
        String ConnectString = "";

        /// <GetConnectionString>
        /// GetConnectionString
        /// </GetConnectionString>
        /// <returns></returns>
        public PinusDatabase()
        {
            ConnectString = XmlStream.getXmlValue(AppDomain.CurrentDomain.BaseDirectory + "DatabaseConfig.xml", "MSSQL",
                "ConnectString");
        }

        /// <GetConnectionString>
        /// GetConnectionString
        /// </GetConnectionString>
        /// <returns></returns>
        public PinusDatabase(String ConnString, Boolean ConnStr)
        {
            ConnectString = ConnString;
        }

        /// <GetConnectionString>
        /// GetConnectionString
        /// </GetConnectionString>
        /// <returns></returns>
        public PinusDatabase(String ConnectName)
        {
            ConnectString = XmlStream.getXmlValue(AppDomain.CurrentDomain.BaseDirectory + "DatabaseConfig.xml",
                ConnectName, "ConnectString");
        }

        /// <GetConnectionString>
        /// GetConnectionString
        /// </GetConnectionString>
        /// <returns></returns>
        public PinusDatabase(String ConnectName, StringEncrypt.EncryptType type)
        {
            String EncryptString = XmlStream.getXmlValue(AppDomain.CurrentDomain.BaseDirectory + "DatabaseConfig.xml",
                ConnectName, "ConnectString");

            if (type != StringEncrypt.EncryptType.None)
                ConnectString = StringEncrypt.DataDecrypt(type, EncryptString);
            else
                ConnectString = EncryptString;
        }



        /// <summary>
        /// Open database with connectstring
        /// </summary>
        public void Open()
        {
            if (conn != null && conn.IsValid() == true)
            {
                ;
            }

            conn = new PDBConnection(ConnectString);
            conn.Open();
        }


        /// <summary>
        /// close database with connectstring
        /// </summary>
        public void Close()
        {
            if (conn == null)
            {
                throw new Exception("Connection has not initialize.");
            }

            if (conn.IsValid() == false)
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

            if (conn.IsValid() == false)
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
        public object GetSpeciaRecordValue(String Command, Int32 Timeout = 30)
        {
            object Value;

            if (conn == null)
            {
                throw new Exception("Connection has not initialize.");
            }

            if (conn.IsValid() != true)
            {
                throw new Exception("Connection has not opened.");
            }

            PDBCommand cmd = conn.CreateCommand();

            DataTable dt = cmd.ExecuteQuery(Command);

            if (dt.Rows.Count > 0)
                Value = dt.Rows[0][0];
            else
                Value = null;

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
        public object GetSpeciaRecordValue(String Command, ParameterClass[] Parameter, Int32 Timeout = 30)
        {
            object Value;

            if (conn == null)
            {
                throw new Exception("Connection has not initialize.");
            }

            if (conn.IsValid() != true)
            {
                throw new Exception("Connection has not opened.");
            }

            PDBCommand cmd = conn.CreateCommand();

            //add parameter
            PDBParameter[] parameter = new PDBParameter[Parameter.Length];
            for (int number = 0; number < Parameter.Length; number++)
            {
                parameter[number] = new PDBParameter(Parameter[number].ParameterName,
                    (PDBType) Enum.Parse(typeof(PDBType), Parameter[number].Value.GetType().ToString()));
                parameter[number].Value = Parameter[number].Value;
            }

            DataTable dt = cmd.ExecuteQuery(Command, parameter);

            if (dt.Rows.Count > 0)
                Value = dt.Rows[0][0];
            else
                Value = null;

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
        public IDataReader GetDataReaderList(String Command, Int32 Timeout = 30)
        {
            IDataReader Value;

            if (conn == null)
            {
                throw new Exception("Connection has not initialize.");
            }

            if (conn.IsValid() != true)
            {
                throw new Exception("Connection has not opened.");
            }

            PDBCommand cmd = conn.CreateCommand();

            //add parameter

            Value = cmd.ExecuteQuery(Command).CreateDataReader();


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

            if (conn.IsValid() != true)
            {
                throw new Exception("Connection has not opened.");
            }

            PDBCommand cmd = conn.CreateCommand();

            //add parameter
            PDBParameter[] parameter = new PDBParameter[Parameter.Length];
            for (int number = 0; number < Parameter.Length; number++)
            {
                parameter[number] = new PDBParameter(Parameter[number].ParameterName,
                    (PDBType) Enum.Parse(typeof(PDBType), Parameter[number].Value.GetType().ToString()));
                parameter[number].Value = Parameter[number].Value;
            }

            Value = cmd.ExecuteQuery(Command, parameter).CreateDataReader();

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
        public DataSet GetDataSet(String Command, Int32 Timeout = 30)
        {
            if (conn == null)
            {
                throw new Exception("Connection has not initialize.");
            }

            if (conn.IsValid() != true)
            {
                throw new Exception("Connection has not opened.");
            }

            PDBCommand cmd = conn.CreateCommand();

            DataTable dt = cmd.ExecuteQuery(Command);

            DataSet ds = new DataSet();
            ds.Tables.Add(dt);

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
        public DataSet GetDataSet(String Command, ParameterClass[] Parameter, Int32 Timeout = 30)
        {
            if (conn == null)
            {
                throw new Exception("Connection has not initialize.");
            }

            if (conn.IsValid() != true)
            {
                throw new Exception("Connection has not opened.");
            }

            PDBCommand cmd = conn.CreateCommand();

            //add parameter
            PDBParameter[] parameter = new PDBParameter[Parameter.Length];
            for (int number = 0; number < Parameter.Length; number++)
            {
                parameter[number] = new PDBParameter(Parameter[number].ParameterName,
                    (PDBType) Enum.Parse(typeof(PDBType), Parameter[number].Value.GetType().ToString()));
                parameter[number].Value = Parameter[number].Value;
            }

            DataTable dt = cmd.ExecuteQuery(Command, parameter);

            DataSet ds = new DataSet();
            ds.Tables.Add(dt);

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
        public Int32 ExecueCommand(String Command, Int32 Timeout = 30)
        {
            Int32 Value=0;

            if (conn == null)
            {
                throw new Exception("Connection has not initialize.");
            }

            if (conn.IsValid() != true)
            {
                throw new Exception("Connection has not opened.");
            }

            PDBCommand cmd = conn.CreateCommand();

            cmd.ExecuteInsert(Command);

            return cmd.SuccessCount;
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

            if (conn.IsValid() != true)
            {
                throw new Exception("Connection has not opened.");
            }

            PDBCommand cmd = conn.CreateCommand();

            for (int i = 0; i < Command.Length; i++)
            {
                cmd.ExecuteNonQuery(Command[i]);
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
        public Int32 ExecueCommand(String Command, ParameterClass[] Parameter, Int32 Timeout = 30)
        {
            Int32 Value=0;

            if (conn == null)
            {
                throw new Exception("Connection has not initialize.");
            }

            if (conn.IsValid() != true)
            {
                throw new Exception("Connection has not opened.");
            }

            PDBCommand cmd = conn.CreateCommand();

            PDBParameter[] parameter = new PDBParameter[Parameter.Length];
            for (int number = 0; number < Parameter.Length; number++)
            {
                parameter[number] = new PDBParameter(Parameter[number].ParameterName,
                    (PDBType)Enum.Parse(typeof(PDBType), Parameter[number].Value.GetType().ToString()));
                parameter[number].Value = Parameter[number].Value;
            }

            cmd.ExecuteNonQuery(Command, parameter);

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
        public Int32 ExecueCommand(String Command, ParameterINClass[] Parameter, Int32 Timeout = 30)
        {
            Int32 Value=0;

            if (conn == null)
            {
                throw new Exception("Connection has not initialize.");
            }

            if (conn.IsValid() != true)
            {
                throw new Exception("Connection has not opened.");
            }

            PDBCommand cmd = conn.CreateCommand();

            PDBParameter[] parameter = new PDBParameter[Parameter.Length];
            for (int number = 0; number < Parameter.Length; number++)
            {
                parameter[number] = new PDBParameter(Parameter[number].ParameterName,
                    (PDBType)Enum.Parse(typeof(PDBType), Parameter[number].Value.GetType().ToString()));
                parameter[number].Value = Parameter[number].Value;
            }

            cmd.ExecuteNonQuery(Command, parameter);

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
        public String[] ExecueStoreProCommand(String StoreProCommand,
            ParameterClass[] ParameterIn, ParameterOutClass[] ParameterOut, Int32 Timeout = 30)
        {
            return new string[] {""};
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
            return new string[] {""};
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
        public DataSet ExecueStoreProCommand(String StoreProCommand,
            ParameterClass[] ParameterIn, Int32 Timeout = 30)
        {
            return new DataSet();
        }
    }
}
