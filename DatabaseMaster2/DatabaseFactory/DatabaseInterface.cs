using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data;
using MySql.Data.MySqlClient;
using NpgsqlTypes;
using Oracle.ManagedDataAccess.Client;

namespace DatabaseMaster2
{

    public struct ParameterClass
    {
        public String ParameterName;
        public object Value;
    }

    public struct ParameterReturnClass
    {
        public String ParameterName;
        public object Value;
    }

    /// <DatabaseInterface>
    /// base class of database
    /// </DatabaseInterface>
    public interface DatabaseInterface
    {

        /// <summary>
        /// Open database with connectstring
        /// </summary>
         void Open();

        /// <summary>
        /// close database with connectstring
        /// </summary>
        void Close();

        /// <summary>
        /// check conn is open
        /// </summary>
        Boolean CheckStatus();

        /// <summary>
        /// get first row and first colunm value from database
        /// </summary>
        /// <param name="Command">SQL command String</param>
        /// <param name="Timeout">[option] timeout of database connect(seconds)</param>
        /// <returns>
        /// object value
        /// </returns>
        object GetSpeciaRecordValue( String Command, Int32 Timeout = 30);

        /// <summary>
        /// get first row and first colunm value from database with Parameter
        /// </summary>
        /// <param name="Command">SQL command String</param>
        /// <param name="Parameter">Parameter</param>
        /// <param name="Timeout">[option] timeout of database connect(seconds)</param>
         /// <returns>
         /// object value
         /// </returns>
         object GetSpeciaRecordValue( String Command, ParameterClass[] Parameter, Int32 Timeout = 30);

         /// <summary>
         /// return a datareader from database
         /// </summary>
         /// <param name="Command">SQL command String</param>
         /// <param name="Timeout">[option] timeout of database connect(seconds)</param>
         /// <returns>
         /// DataReader value
         /// </returns>
         IDataReader GetDataReaderList( String Command, Int32 Timeout = 30);

         /// <summary>
         /// return a datareader from database
         /// </summary>
         /// <param name="Command">SQL command String</param>
         /// <param name="Parameter">Parameter</param>
         /// <param name="Timeout">[option] timeout of database connect(seconds)</param>
         /// <returns>
         /// DataReader value
         /// </returns>
         IDataReader GetDataReaderList(String Command, ParameterClass[] Parameter, Int32 Timeout = 30);

         /// <summary>
         /// return a dataset from database
         /// </summary>
         /// <param name="Command">SQL command String</param>
         /// <param name="Timeout">[option] timeout of database connect(seconds)</param>
         /// <returns>
         /// dataset value
         /// </returns>
         DataSet GetDataSet( String Command, Int32 Timeout = 30);

         /// <summary>
         /// return a dataset from database with Parameter
         /// </summary>
         /// <param name="Command">SQL command String</param>
         /// <param name="Parameter">Parameter</param>
         /// <param name="Timeout">[option] timeout of database connect(seconds)</param>
         /// <returns>
         /// dataset value
         /// </returns>
         DataSet GetDataSet( String Command, ParameterClass[] Parameter, Int32 Timeout = 30);

         /// <summary>
         /// Execue sql Command
         /// </summary>
         /// <param name="Command">SQL command String</param>
         /// <param name="Timeout">[option] timeout of database connect(seconds)</param>
         /// <returns>
         /// The number of rows affected 
         /// </returns>
         Int32 ExecueCommand( String Command, Int32 Timeout = 30);

        /// <summary>
        /// Execue Transaction sql Command
        /// </summary>
        /// <param name="Command">SQL command String</param>
        /// <param name="Timeout">[option] timeout of database connect(seconds)</param>
        /// <returns>
        /// The number of rows affected 
        /// </returns>
        Int32 ExecueTransactionCommand(String[] Command, Int32 Timeout = 30);


        /// <summary>
        /// Execue sql Command With Parameter
        /// </summary>
        /// <param name="Command">SQL command String</param>
        /// <param name="Parameter">Parameter</param>
        /// <param name="Timeout">[option] timeout of database connect(seconds)</param>
        /// <returns>
        /// The number of rows affected 
        /// </returns>
        Int32 ExecueCommand( String Command, ParameterClass[] Parameter, Int32 Timeout = 30);

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
         String[] ExecueStoreProCommand( String StoreProCommand,
            ParameterClass[] ParameterIn, ParameterClass[] ParameterOut, Int32 Timeout = 30);

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
         String[] ExecueStoreProCommand(String StoreProCommand,
             ParameterClass[] ParameterIn, ParameterReturnClass[] ParameterOut, Int32 Timeout = 30);


        /// <summary>
        /// Execue sql Command With Parameter
        /// </summary>
        /// <param name="StoreProCommand">SQL command String</param>
        /// <param name="ParameterIn">Parameter of Input</param>
        /// <param name="Timeout">[option] timeout of database connect(seconds)</param>
        /// <returns>
        /// dataset value
        /// </returns>
        DataSet ExecueStoreProCommand( String StoreProCommand,
            ParameterClass[] ParameterIn, Int32 Timeout = 30);


    }
}
