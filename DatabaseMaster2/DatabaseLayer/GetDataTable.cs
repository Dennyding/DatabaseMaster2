using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data;

namespace DatabaseMaster2
{
    public class DatabaseGetData
    {
        private ConnectionConfig _connectionConfig;
        private DatabaseInterface _database;

        public DatabaseGetData(ConnectionConfig config, DatabaseInterface database)
        {
            _connectionConfig = config;
            _database = database;
        }


        /// <summary>
        /// check connect status
        /// 检查连接状态
        /// </summary>
        /// <returns></returns>
        public Boolean Status()
        {
            try
            {
                //数据库连接
                if (_connectionConfig.IsAutoCloseConnection == false)
                    if (_database.CheckStatus() == false)
                        throw new Exception("databse connect not open");
                if (_connectionConfig.IsAutoCloseConnection == true) _database.Open();
                var count = Convert.ToInt64(_database.GetSpeciaRecordValue("select 1", _connectionConfig.WaitTimeout));
                if (_connectionConfig.IsAutoCloseConnection == true) _database.Close();

                if (count == 0)
                    return false;
                else
                    return true;
            }
            catch (Exception)
            {
                return false;
            }
        }


        /// <summary>
        /// get all data
        /// 得到表中所有数据
        /// </summary>
        /// <param name="TableName"></param>
        /// <param name="OrderByName"></param>
        /// <returns></returns>
        public IDataTable Data(String TableName, String OrderByName="", SortMode sortMode=SortMode.Ascending)
        {
            //sql生成
            var sql = new SelectDBCommandBuilder();
            sql.AddSelectTable(TableName);
            sql.AddSelectALLColumn();
            if (String.IsNullOrEmpty(OrderByName)==false)
                sql.AddOrderBy(OrderByName, sortMode);


            //数据库连接
            if (_connectionConfig.IsAutoCloseConnection == false)
                if (_database.CheckStatus() == false)
                    throw new Exception("databse connect not open");
            if (_connectionConfig.IsAutoCloseConnection == true) _database.Open();
            var ds = _database.GetDataSet(sql.BuildCommand(), _connectionConfig.WaitTimeout);
            if (_connectionConfig.IsAutoCloseConnection == true) _database.Close();

            IDataTable DT = new IDataTable {table = ds.Tables[0]};
            return DT;
        }


        /// <summary>
        /// get data by filter
        /// 得到指定条件的表中所有数据
        /// </summary>
        /// <param name="TableName"></param>
        /// <param name="ColumnName"></param>
        /// <param name="Value"></param>
        /// <param name="OrderByName"></param>
        /// <returns></returns>
        public IDataTable Data(string TableName, string ColumnName, object Value, String OrderByName = "", SortMode sortMode = SortMode.Ascending)
        {
            //sql生成
            var sql = new SelectDBCommandBuilder();
            sql.AddSelectTable(TableName);
            sql.AddSelectALLColumn();
            sql.AddWhere(WhereRelation.None, ColumnName, CommandComparison.Equals, Value);
            if (String.IsNullOrEmpty(OrderByName)==false)
                sql.AddOrderBy(OrderByName, sortMode);

            //数据库连接
            if (_connectionConfig.IsAutoCloseConnection == false)
                if (_database.CheckStatus() == false)
                    throw new Exception("databse connect not open");
            if (_connectionConfig.IsAutoCloseConnection == true) _database.Open();
            var ds = _database.GetDataSet(sql.BuildCommand(), _connectionConfig.WaitTimeout);
            if (_connectionConfig.IsAutoCloseConnection == true) _database.Close();

            IDataTable DT = new IDataTable {table = ds.Tables[0]};
            return DT;
        }

        /// <summary>
        /// get data by filter
        /// 得到指定条件的表中所有数据
        /// </summary>
        /// <param name="TableName"></param>
        /// <param name="ColumnName"></param>
        /// <param name="Comparison"></param>
        /// <param name="Value"></param>
        /// <returns></returns>
        public IDataTable Data(string TableName, string ColumnName, CommandComparison Comparison,
            object Value, String OrderByName = "", SortMode sortMode = SortMode.Ascending)
        {
            //sql生成
            var sql = new SelectDBCommandBuilder();
            sql.AddSelectTable(TableName);
            sql.AddSelectALLColumn();
            sql.AddWhere(WhereRelation.None, ColumnName, (CommandComparison)Comparison, Value);
            if (String.IsNullOrEmpty(OrderByName)==false)
                sql.AddOrderBy(OrderByName, sortMode);

            //数据库连接
            if (_connectionConfig.IsAutoCloseConnection == false)
                if (_database.CheckStatus() == false)
                    throw new Exception("databse connect not open");
            if (_connectionConfig.IsAutoCloseConnection == true) _database.Open();
            var ds = _database.GetDataSet(sql.BuildCommand(), _connectionConfig.WaitTimeout);
            if (_connectionConfig.IsAutoCloseConnection == true) _database.Close();

            IDataTable DT = new IDataTable {table = ds.Tables[0]};
            return DT;
        }

        /// <summary>
        /// get data by filter
        /// 得到得到多个指定条件的表中所有数据
        /// </summary>
        /// <param name="TableName"></param>
        /// <param name="ColumnName"></param>
        /// <param name="Value"></param>
        /// <returns></returns>
        public IDataTable Data(string TableName, string[] ColumnName, object[] Value, String OrderByName = "", SortMode sortMode = SortMode.Ascending)
        {
            //sql生成
            var sql = new SelectDBCommandBuilder();
            sql.AddSelectTable(TableName);
            sql.AddSelectALLColumn();

            for (var i = 0; i < ColumnName.Length; i++)
                sql.AddWhere(WhereRelation.And, ColumnName[i], CommandComparison.Equals, Value[i]);
            if (String.IsNullOrEmpty(OrderByName)==false)
                sql.AddOrderBy(OrderByName, sortMode);

            //数据库连接
            if (_connectionConfig.IsAutoCloseConnection == false)
                if (_database.CheckStatus() == false)
                    throw new Exception("databse connect not open");
            if (_connectionConfig.IsAutoCloseConnection == true) _database.Open();
            var ds = _database.GetDataSet(sql.BuildCommand(), _connectionConfig.WaitTimeout);
            if (_connectionConfig.IsAutoCloseConnection == true) _database.Close();

            IDataTable DT = new IDataTable {table = ds.Tables[0]};
            return DT;
        }


        /// <summary>
        /// get data by filter
        /// 得到得到多个指定条件的表中所有数据
        /// </summary>
        /// <param name="TableName"></param>
        /// <param name="ColumnName"></param>
        /// <param name="Comparison"></param>
        /// <param name="Value"></param>
        /// <returns></returns>
        public IDataTable Data(string TableName, string[] ColumnName, CommandComparison[] Comparison,
            object[] Value, String OrderByName = "", SortMode sortMode = SortMode.Ascending)
        {
            //sql生成
            var sql = new SelectDBCommandBuilder();
            sql.AddSelectTable(TableName);
            sql.AddSelectALLColumn();

            for (var i = 0; i < ColumnName.Length; i++)
                sql.AddWhere(WhereRelation.And, ColumnName[i], (CommandComparison)Comparison[i], Value[i]);
            if (String.IsNullOrEmpty(OrderByName)==false)
                sql.AddOrderBy(OrderByName, sortMode);

            //数据库连接
            if (_connectionConfig.IsAutoCloseConnection == false)
                if (_database.CheckStatus() == false)
                    throw new Exception("databse connect not open");
            if (_connectionConfig.IsAutoCloseConnection == true) _database.Open();
            var ds = _database.GetDataSet(sql.BuildCommand(), _connectionConfig.WaitTimeout);
            if (_connectionConfig.IsAutoCloseConnection == true) _database.Close();

            IDataTable DT = new IDataTable {table = ds.Tables[0]};
            return DT;
        }

        /// <summary>
        /// get data by filter
        /// 得到得到多个指定条件的表中所有数据
        /// </summary>
        /// <param name="TableName"></param>
        /// <param name="GroupColumnName"></param>
        /// <param name="ColumnName"></param>
        /// <param name="Comparison"></param>
        /// <param name="Value"></param>
        /// <param name="GroupByName"></param>
        /// <returns></returns>
        public IDataTable Data(string TableName, List<string> GroupColumnName, string[] ColumnName,
            CommandComparison[] Comparison, object[] Value, string GroupByName)
        {
            //sql生成
            var sql = new SelectDBCommandBuilder();
            sql.AddSelectTable(TableName);
            sql.AddSelectColumn(GroupColumnName);

            sql.AddGroupBy(GroupByName);

            for (var i = 0; i < ColumnName.Length; i++)
                sql.AddWhere(WhereRelation.And, ColumnName[i], (CommandComparison)Comparison[i], Value[i]);


            //数据库连接
            if (_connectionConfig.IsAutoCloseConnection == false)
                if (_database.CheckStatus() == false)
                    throw new Exception("databse connect not open");
            if (_connectionConfig.IsAutoCloseConnection == true) _database.Open();
            var ds = _database.GetDataSet(sql.BuildCommand(), _connectionConfig.WaitTimeout);
            if (_connectionConfig.IsAutoCloseConnection == true) _database.Close();

            IDataTable DT = new IDataTable {table = ds.Tables[0]};
            return DT;
        }

        /// <summary>
        /// get data by filter
        /// 得到得到多个指定条件的表中所有数据
        /// </summary>
        /// <param name="TableName"></param>
        /// <param name="ColumnName"></param>
        /// <param name="Comparison"></param>
        /// <param name="Value"></param>
        /// <param name="Table1ColumnName"></param>
        /// <param name="Table2ColumnName"></param>
        /// <returns></returns>
        public IDataTable Data(List<string> TableName, string[] ColumnName, CommandComparison[] Comparison,
            object[] Value,
            string[] Table1ColumnName, string[] Table2ColumnName)
        {
            //sql生成
            var sql = new SelectDBCommandBuilder();
            sql.AddSelectTable(TableName);
            sql.AddSelectALLColumn();

            for (var i = 0; i < ColumnName.Length; i++)
                sql.AddWhere(WhereRelation.And, ColumnName[i], (CommandComparison)Comparison[i], Value[i]);

            for (var i = 0; i < Table1ColumnName.Length; i++)
                sql.AddWhereByRelationShip(WhereRelation.And, Table1ColumnName[i], CommandComparison.Equals,
                    Table2ColumnName[i]);


            //数据库连接
            if (_connectionConfig.IsAutoCloseConnection == false)
                if (_database.CheckStatus() == false)
                    throw new Exception("databse connect not open");
            if (_connectionConfig.IsAutoCloseConnection == true) _database.Open();
            var ds = _database.GetDataSet(sql.BuildCommand(), _connectionConfig.WaitTimeout);
            if (_connectionConfig.IsAutoCloseConnection == true) _database.Close();

            IDataTable DT = new IDataTable {table = ds.Tables[0]};
            return DT;
        }

        /// <summary>
        /// get data by filter
        /// 得到得到多个指定条件的表中所有数据
        /// </summary>
        /// <param name="TableName"></param>
        /// <param name="Table1ColumnName"></param>
        /// <param name="Table2ColumnName"></param>
        /// <returns></returns>
        public IDataTable Data(List<string> TableName, string[] Table1ColumnName, string[] Table2ColumnName)
        {
            //sql生成
            var sql = new SelectDBCommandBuilder();
            sql.AddSelectTable(TableName);
            sql.AddSelectALLColumn();


            for (var i = 0; i < Table1ColumnName.Length; i++)
                sql.AddWhereByRelationShip(WhereRelation.And, Table1ColumnName[i], CommandComparison.Equals,
                    Table2ColumnName[i]);


            //数据库连接
            if (_connectionConfig.IsAutoCloseConnection == false)
                if (_database.CheckStatus() == false)
                    throw new Exception("databse connect not open");
            if (_connectionConfig.IsAutoCloseConnection == true) _database.Open();
            var ds = _database.GetDataSet(sql.BuildCommand(), _connectionConfig.WaitTimeout);
            if (_connectionConfig.IsAutoCloseConnection == true) _database.Close();

            IDataTable DT = new IDataTable {table = ds.Tables[0]};
            return DT;
        }


        /// <summary>
        /// get data by filter
        /// 得到得到多个指定条件的表中所有数据
        /// </summary>
        /// <param name="TableName"></param>
        /// <param name="ColumnName"></param>
        /// <param name="KeyColumnName"></param>
        /// <param name="KeyValue"></param>
        /// <returns></returns>
        public IDataTable Data(string TableName, List<string> ColumnName, string KeyColumnName,
            object KeyValue)
        {
            //sql生成
            var sql = new SelectDBCommandBuilder();
            sql.AddSelectTable(TableName);
            sql.AddSelectColumn(ColumnName);
            sql.AddWhere(WhereRelation.None, KeyColumnName, CommandComparison.Equals, KeyValue);


            //数据库连接
            if (_connectionConfig.IsAutoCloseConnection == false)
                if (_database.CheckStatus() == false)
                    throw new Exception("databse connect not open");
            if (_connectionConfig.IsAutoCloseConnection == true) _database.Open();
            var ds = _database.GetDataSet(sql.BuildCommand(), _connectionConfig.WaitTimeout);
            if (_connectionConfig.IsAutoCloseConnection == true) _database.Close();

            IDataTable DT = new IDataTable {table = ds.Tables[0]};
            return DT;
        }

        /// <summary>
        /// check data exist
        /// 检查表中是否存在指定内容
        /// </summary>
        /// <param name="TableName"></param>
        /// <param name="ColumnName"></param>
        /// <param name="Value"></param>
        /// <returns></returns>
        public bool Exist(string TableName, string ColumnName, object Value)
        {
            //sql生成
            var sql = new SelectDBCommandBuilder();
            sql.AddSelectTable(TableName);
            sql.AddSelectALLColumn();
            sql.AddWhere(WhereRelation.None, ColumnName, CommandComparison.Equals, Value);
            sql.SelectCount = true;

            //数据库连接
            if (_connectionConfig.IsAutoCloseConnection == false)
                if (_database.CheckStatus() == false)
                    throw new Exception("databse connect not open");
            if (_connectionConfig.IsAutoCloseConnection == true) _database.Open();
            var count = Convert.ToInt64(_database.GetSpeciaRecordValue(sql.BuildCommand(),
                _connectionConfig.WaitTimeout));
            if (_connectionConfig.IsAutoCloseConnection == true) _database.Close();

            if (count == 0)
                return false;
            else
                return true;
        }

        /// <summary>
        /// check data exist
        /// 检查表中是否存在指定内容
        /// </summary>
        /// <param name="TableName"></param>
        /// <param name="ColumnName"></param>
        /// <param name="Value"></param>
        /// <returns></returns>
        public bool Exist(string TableName, string[] ColumnName, object[] Value)
        {
            //sql生成
            var sql = new SelectDBCommandBuilder();
            sql.AddSelectTable(TableName);
            sql.AddSelectALLColumn();

            for (var i = 0; i < ColumnName.Length; i++)
                sql.AddWhere(WhereRelation.And, ColumnName[i], CommandComparison.Equals, Value[i]);


            sql.SelectCount = true;

            //数据库连接
            if (_connectionConfig.IsAutoCloseConnection == false)
                if (_database.CheckStatus() == false)
                    throw new Exception("databse connect not open");
            if (_connectionConfig.IsAutoCloseConnection == true) _database.Open();
            var count = Convert.ToInt64(_database.GetSpeciaRecordValue(sql.BuildCommand(),
                _connectionConfig.WaitTimeout));
            if (_connectionConfig.IsAutoCloseConnection == true) _database.Close();

            if (count == 0)
                return false;
            else
                return true;
        }

        /// <summary>
        /// check data exist
        /// 检查表中是否存在指定内容
        /// </summary>
        /// <param name="TableName"></param>
        /// <param name="ColumnName"></param>
        /// <param name="Comparison"></param>
        /// <param name="Value"></param>
        /// <returns></returns>
        public bool Exist(string TableName, string[] ColumnName, CommandComparison[] Comparison,
            object[] Value)
        {
            //sql生成
            var sql = new SelectDBCommandBuilder();
            sql.AddSelectTable(TableName);
            sql.AddSelectALLColumn();

            for (var i = 0; i < ColumnName.Length; i++)
                sql.AddWhere(WhereRelation.And, ColumnName[i], (CommandComparison)Comparison[i], Value[i]);


            sql.SelectCount = true;

            //数据库连接
            if (_connectionConfig.IsAutoCloseConnection == false)
                if (_database.CheckStatus() == false)
                    throw new Exception("databse connect not open");
            if (_connectionConfig.IsAutoCloseConnection == true) _database.Open();
            var count = Convert.ToInt64(_database.GetSpeciaRecordValue(sql.BuildCommand(),
                _connectionConfig.WaitTimeout));
            if (_connectionConfig.IsAutoCloseConnection == true) _database.Close();

            if (count == 0)
                return false;
            else
                return true;
        }


        /// <summary>
        /// execute procedure data exist
        /// 检查存储过程中是否存在指定内容
        /// </summary>
        /// <param name="ProcedureCommand"></param>
        /// <param name="parameterin"></param>
        /// <returns></returns>
        public bool Exist(string ProcedureCommand, ParameterClass[] parameterin)
        {
            //数据库连接
            if (_connectionConfig.IsAutoCloseConnection == false)
                if (_database.CheckStatus() == false)
                    throw new Exception("databse connect not open");
            if (_connectionConfig.IsAutoCloseConnection == true) _database.Open();
            var count = Convert.ToInt32(_database.GetSpeciaRecordValue(ProcedureCommand, parameterin,
                _connectionConfig.WaitTimeout));
            if (_connectionConfig.IsAutoCloseConnection == true) _database.Close();

            if (count == 0)
                return false;
            else
                return true;
        }

        /// <summary>
        /// execute procedure data
        /// 执行存储过程中返回结果
        /// </summary>
        /// <param name="ProcedureCommand"></param>
        /// <param name="parameterin"></param>
        /// <param name="parameterout"></param>
        /// <returns></returns>
        public string[] Procedure(string ProcedureCommand, ParameterClass[] parameterin,
            ParameterOutClass[] parameterout)
        {
            string[] value;

            //数据库连接
            if (_connectionConfig.IsAutoCloseConnection == false)
                if (_database.CheckStatus() == false)
                    throw new Exception("databse connect not open");
            if (_connectionConfig.IsAutoCloseConnection == true) _database.Open();
            value = _database.ExecueStoreProCommand(ProcedureCommand, parameterin, parameterout,
                _connectionConfig.WaitTimeout);
            if (_connectionConfig.IsAutoCloseConnection == true) _database.Close();

            return value;
        }

        /// <summary>
        /// execute procedure data
        /// 执行存储过程中返回结果
        /// </summary>
        /// <param name="ProcedureCommand"></param>
        /// <param name="parameterin"></param>
        /// <param name="parameterout"></param>
        /// <returns></returns>
        public string[] Procedure(string ProcedureCommand, ParameterClass[] parameterin,
            ParameterReturnClass[] parameterout)
        {
            string[] value;

            //数据库连接
            if (_connectionConfig.IsAutoCloseConnection == false)
                if (_database.CheckStatus() == false)
                    throw new Exception("databse connect not open");
            if (_connectionConfig.IsAutoCloseConnection == true) _database.Open();
            value = _database.ExecueStoreProCommand(ProcedureCommand, parameterin, parameterout,
                _connectionConfig.WaitTimeout);
            if (_connectionConfig.IsAutoCloseConnection == true) _database.Close();

            return value;
        }

        /// <summary>
        /// Execue Command
        /// 执行SQL语句
        /// </summary>
        /// <param name="ProcedureCommand"></param>
        /// <returns></returns>
        public void ExecueCommand(string ProcedureCommand)
        {
            //数据库连接
            if (_connectionConfig.IsAutoCloseConnection == false)
                if (_database.CheckStatus() == false)
                    throw new Exception("databse connect not open");
            if (_connectionConfig.IsAutoCloseConnection == true) _database.Open();
            _database.ExecueCommand(ProcedureCommand, _connectionConfig.WaitTimeout);
            if (_connectionConfig.IsAutoCloseConnection == true) _database.Close();
        }

        /// <summary>
        /// execute procedure data
        /// 执行存储过程中返回结果
        /// </summary>
        /// <param name="ProcedureCommand"></param>
        /// <returns></returns>
        public IDataTable[] Procedure(string SQLCommand)
        {
            //数据库连接
            if (_connectionConfig.IsAutoCloseConnection == false)
                if (_database.CheckStatus() == false)
                    throw new Exception("databse connect not open");
            if (_connectionConfig.IsAutoCloseConnection == true) _database.Open();
            var ds = _database.GetDataSet(SQLCommand, _connectionConfig.WaitTimeout);
            if (_connectionConfig.IsAutoCloseConnection == true) _database.Close();

            IDataTable[] dt = new IDataTable[ds.Tables.Count];

            for (int i = 0; i < ds.Tables.Count; i++)
            {
                dt[i].table = ds.Tables[i];
            }
            return dt;
        }

        /// <summary>
        /// execute procedure data
        /// 执行存储过程
        /// </summary>
        /// <param name="ProcedureCommand"></param>
        /// <param name="parameterin"></param>
        public void Procedure(string ProcedureCommand, ParameterClass[] parameterin)
        {
            //数据库连接
            if (_connectionConfig.IsAutoCloseConnection == false)
                if (_database.CheckStatus() == false)
                    throw new Exception("databse connect not open");
            if (_connectionConfig.IsAutoCloseConnection == true) _database.Open();
            _database.ExecueStoreProCommand(ProcedureCommand, parameterin, _connectionConfig.WaitTimeout);
            if (_connectionConfig.IsAutoCloseConnection == true) _database.Close();
        }


        /// <summary>
        /// execute procedure data
        /// 执行存储过程中返回结果
        /// </summary>
        /// <param name="ProcedureCommand"></param>
        /// <param name="parameterin"></param>
        /// <param name="ReturnDataset"></param>
        /// <returns></returns>
        public IDataTable[] Procedure(string ProcedureCommand, ParameterClass[] parameterin, bool ReturnDataset)
        {
            var ds = new DataSet();

            //数据库连接
            if (_connectionConfig.IsAutoCloseConnection == false)
                if (_database.CheckStatus() == false)
                    throw new Exception("databse connect not open");
            if (_connectionConfig.IsAutoCloseConnection == true) _database.Open();
            ds = _database.ExecueStoreProCommand(ProcedureCommand, parameterin, _connectionConfig.WaitTimeout);
            if (_connectionConfig.IsAutoCloseConnection == true) _database.Close();

            IDataTable[] dt=new IDataTable[ds.Tables.Count];

            for (int i = 0; i < ds.Tables.Count; i++)
            {
                dt[i].table = ds.Tables[i];
            }
            return dt;
        }

        /// <summary>'
        /// get first data by filter
        /// 得到指定条件的一个数据
        /// </summary>
        /// <param name="TableName"></param>
        /// <param name="ColumnName"></param>
        /// <param name="KeyColumnName"></param>
        /// <param name="KeyValue"></param>
        /// <returns></returns>
        public object First(string TableName, string ColumnName, string KeyColumnName, object KeyValue)
        {
            //sql生成
            var sql = new SelectDBCommandBuilder();
            sql.AddSelectTable(TableName);
            sql.AddSelectColumn(ColumnName);
            sql.AddWhere(WhereRelation.None, KeyColumnName, CommandComparison.Equals, KeyValue);

            //数据库连接
            if (_connectionConfig.IsAutoCloseConnection == false)
                if (_database.CheckStatus() == false)
                    throw new Exception("databse connect not open");
            if (_connectionConfig.IsAutoCloseConnection == true) _database.Open();
            var result = _database.GetSpeciaRecordValue(sql.BuildCommand(), _connectionConfig.WaitTimeout);
            if (_connectionConfig.IsAutoCloseConnection == true) _database.Close();

            return result;
        }


        /// <summary>
        /// get first data by filter
        /// 得到指定条件的一个数据
        /// </summary>
        /// <param name="TableName"></param>
        /// <param name="ColumnName"></param>
        /// <param name="KeyColumnName"></param>
        /// <param name="KeyValue"></param>
        /// <returns></returns>
        public object First(string TableName, string ColumnName, string[] KeyColumnName, object[] KeyValue)
        {
            //sql生成
            var sql = new SelectDBCommandBuilder();
            sql.AddSelectTable(TableName);
            sql.AddSelectColumn(ColumnName);

            for (var i = 0; i < KeyColumnName.Length; i++)
                sql.AddWhere(WhereRelation.And, KeyColumnName[i], CommandComparison.Equals, KeyValue[i]);

            //数据库连接
            if (_connectionConfig.IsAutoCloseConnection == false)
                if (_database.CheckStatus() == false)
                    throw new Exception("databse connect not open");
            if (_connectionConfig.IsAutoCloseConnection == true) _database.Open();
            var result = _database.GetSpeciaRecordValue(sql.BuildCommand(), _connectionConfig.WaitTimeout);
            if (_connectionConfig.IsAutoCloseConnection == true) _database.Close();

            return result;
        }

        /// <summary>
        /// get count data by filter
        ///  得到指定条件的行计数数据
        /// </summary>
        /// <param name="TableName"></param>
        /// <param name="ColumnName"></param>
        /// <param name="KeyColumnName"></param>
        /// <param name="KeyValue"></param>
        /// <param name="CountNumber"></param>
        /// <returns></returns>
        public object First(string TableName, string ColumnName, string[] KeyColumnName, object[] KeyValue,
            bool CountNumber)
        {
            //sql生成
            var sql = new SelectDBCommandBuilder();
            sql.AddSelectTable(TableName);
            sql.AddSelectColumn(ColumnName);
            sql.SelectCount = true;

            for (var i = 0; i < KeyColumnName.Length; i++)
                sql.AddWhere(WhereRelation.And, KeyColumnName[i], CommandComparison.Equals, KeyValue[i]);

            //数据库连接
            if (_connectionConfig.IsAutoCloseConnection == false)
                if (_database.CheckStatus() == false)
                    throw new Exception("databse connect not open");
            if (_connectionConfig.IsAutoCloseConnection == true) _database.Open();
            var result = _database.GetSpeciaRecordValue(sql.BuildCommand(), _connectionConfig.WaitTimeout);
            if (_connectionConfig.IsAutoCloseConnection == true) _database.Close();

            return result;
        }

        /// <summary>
        /// get count data by filter
        ///  得到指定条件的行计数数据
        /// </summary>
        /// <param name="TableName"></param>
        /// <param name="ColumnName"></param>
        /// <param name="KeyColumnName"></param>
        /// <param name="KeyValue"></param>
        /// <param name="CountNumber"></param>
        /// <returns></returns>
        public object First(string TableName, string ColumnName, string[] KeyColumnName,
            CommandComparison[] Comparison, object[] KeyValue, bool CountNumber)
        {
            //sql生成
            var sql = new SelectDBCommandBuilder();
            sql.AddSelectTable(TableName);
            sql.AddSelectColumn(ColumnName);
            sql.SelectCount = true;

            for (var i = 0; i < KeyColumnName.Length; i++)
                sql.AddWhere(WhereRelation.And, KeyColumnName[i], CommandComparison.Equals, KeyValue[i]);

            //数据库连接
            if (_connectionConfig.IsAutoCloseConnection == false)
                if (_database.CheckStatus() == false)
                    throw new Exception("databse connect not open");
            if (_connectionConfig.IsAutoCloseConnection == true) _database.Open();
            var result = _database.GetSpeciaRecordValue(sql.BuildCommand(), _connectionConfig.WaitTimeout);
            if (_connectionConfig.IsAutoCloseConnection == true) _database.Close();

            return result;
        }

        /// <summary>
        /// get count data by filter
        ///  得到指定条件的行计数数据
        /// </summary>
        /// <param name="TableName"></param>
        /// <param name="ColumnName"></param>
        /// <param name="CountNumber"></param>
        /// <returns></returns>
        public object First(string TableName, string ColumnName, bool CountNumber)
        {
            //sql生成
            var sql = new SelectDBCommandBuilder();
            sql.AddSelectTable(TableName);
            sql.AddSelectColumn(ColumnName);
            sql.SelectCount = true;

            //数据库连接
            if (_connectionConfig.IsAutoCloseConnection == false)
                if (_database.CheckStatus() == false)
                    throw new Exception("databse connect not open");
            if (_connectionConfig.IsAutoCloseConnection == true) _database.Open();
            var result = _database.GetSpeciaRecordValue(sql.BuildCommand(), _connectionConfig.WaitTimeout);
            if (_connectionConfig.IsAutoCloseConnection == true) _database.Close();

            return result;
        }


        /// <summary>
        /// get recordnumber data by filter
        /// 得到指定数据的指定内容
        /// </summary>
        /// <param name="TableName"></param>
        /// <param name="ColumnName"></param>
        /// <param name="KeyColumnName"></param>
        /// <param name="KeyValue"></param>
        /// <param name="RecordNumber"></param>
        /// <returns></returns>
        public IDataTable Data(string TableName, string ColumnName, string KeyColumnName, object KeyValue,
            int RecordNumber)
        {
            //sql生成
            var sql = new SelectDBCommandBuilder();
            sql.AddSelectTable(TableName);
            sql.AddSelectColumn(ColumnName);
            sql.AddWhere(WhereRelation.None, KeyColumnName, CommandComparison.Equals, KeyValue);
            sql.TopRecords = RecordNumber;

            //数据库连接
            if (_connectionConfig.IsAutoCloseConnection == false)
                if (_database.CheckStatus() == false)
                    throw new Exception("databse connect not open");
            if (_connectionConfig.IsAutoCloseConnection == true) _database.Open();
            var dt = _database.GetDataSet(sql.BuildCommand(), _connectionConfig.WaitTimeout).Tables[0];
            if (_connectionConfig.IsAutoCloseConnection == true) _database.Close();

            IDataTable DT = new IDataTable();
            DT.table = dt;
            return DT;
        }

        /// <summary>
        /// get recordnumber data by filter
        /// 得到指定数据的指定内容
        /// </summary>
        /// <param name="TableName"></param>
        /// <param name="ColumnName"></param>
        /// <param name="OrderByName"></param>
        /// <param name="RecordNumber"></param>
        /// <returns></returns>
        public IDataTable Data(string TableName, int RecordNumber, string ColumnName,String OrderByName= "", SortMode sortMode = SortMode.Ascending)
        {
            //sql生成
            var sql = new SelectDBCommandBuilder();
            sql.AddSelectTable(TableName);
            sql.AddSelectColumn(ColumnName);
            if (String.IsNullOrEmpty(OrderByName)==false)
                sql.AddOrderBy(OrderByName, sortMode);
            sql.TopRecords = RecordNumber;

            //数据库连接
            if (_connectionConfig.IsAutoCloseConnection == false)
                if (_database.CheckStatus() == false)
                    throw new Exception("databse connect not open");
            if (_connectionConfig.IsAutoCloseConnection == true) _database.Open();
            var dt = _database.GetDataSet(sql.BuildCommand(), _connectionConfig.WaitTimeout).Tables[0];
            if (_connectionConfig.IsAutoCloseConnection == true) _database.Close();

            IDataTable DT = new IDataTable();
            DT.table = dt;
            return DT;
        }

        /// <summary>
        /// get recordnumber data by filter
        /// 得到指定数据的指定内容
        /// </summary>
        /// <param name="TableName"></param>
        /// <param name="ColumnName"></param>
        /// <param name="KeyColumnName"></param>
        /// <param name="KeyValue"></param>
        /// <param name="OrderByName"></param>
        /// <param name="RecordNumber"></param>
        /// <returns></returns>
        public IDataTable Data(string TableName, string ColumnName, string[] KeyColumnName,
            object[] KeyValue, int RecordNumber, String OrderByName= "", SortMode sortMode = SortMode.Ascending)
        {
            //sql生成
            var sql = new SelectDBCommandBuilder();
            sql.AddSelectTable(TableName);
            sql.AddSelectColumn(ColumnName);
            if (String.IsNullOrEmpty(OrderByName)==false)
                sql.AddOrderBy(OrderByName, sortMode);
            sql.TopRecords = RecordNumber;

            for (var i = 0; i < KeyColumnName.Length; i++)
                sql.AddWhere(WhereRelation.And, KeyColumnName[i], CommandComparison.Equals, KeyValue[i]);

            //数据库连接
            if (_connectionConfig.IsAutoCloseConnection == false)
                if (_database.CheckStatus() == false)
                    throw new Exception("databse connect not open");
            if (_connectionConfig.IsAutoCloseConnection == true) _database.Open();
            var dt = _database.GetDataSet(sql.BuildCommand(), _connectionConfig.WaitTimeout).Tables[0];
            if (_connectionConfig.IsAutoCloseConnection == true) _database.Close();

            IDataTable DT = new IDataTable();
            DT.table = dt;
            return DT;
        }

        /// <summary>
        /// get recordnumber data by filter
        /// 得到指定数据的指定内容
        /// </summary>
        /// <param name="TableName"></param>
        /// <param name="ColumnName"></param>
        /// <param name="KeyColumnName"></param>
        /// <param name="Comparison"></param>
        /// <param name="KeyValue"></param>
        /// <param name="OrderByName"></param>
        /// <param name="RecordNumber"></param>
        /// <returns></returns>
        public IDataTable Data(string TableName, string ColumnName, string[] KeyColumnName,
            CommandComparison[] Comparison, object[] KeyValue,  int RecordNumber, String OrderByName = "", SortMode sortMode = SortMode.Ascending)
        {
            //sql生成
            var sql = new SelectDBCommandBuilder();
            sql.AddSelectTable(TableName);
            sql.AddSelectColumn(ColumnName);
            if (String.IsNullOrEmpty(OrderByName)==false)
                sql.AddOrderBy(OrderByName, sortMode);
            sql.TopRecords = RecordNumber;

            for (var i = 0; i < KeyColumnName.Length; i++)
                sql.AddWhere(WhereRelation.And, KeyColumnName[i], (CommandComparison)Comparison[i], KeyValue[i]);

            //数据库连接
            if (_connectionConfig.IsAutoCloseConnection == false)
                if (_database.CheckStatus() == false)
                    throw new Exception("databse connect not open");
            if (_connectionConfig.IsAutoCloseConnection == true) _database.Open();
            var dt = _database.GetDataSet(sql.BuildCommand(), _connectionConfig.WaitTimeout).Tables[0];
            if (_connectionConfig.IsAutoCloseConnection == true) _database.Close();

            IDataTable DT = new IDataTable();
            DT.table = dt;
            return DT;
        }


        /// <summary>
        /// get recordnumber data by filter
        /// 得到指定数据的指定内容
        /// </summary>
        /// <param name="TableName"></param>
        /// <param name="KeyColumnName"></param>
        /// <param name="KeyValue"></param>
        /// <param name="RecordNumber"></param>
        /// <returns></returns>
        public IDataTable Data(string TableName, string[] KeyColumnName, object[] KeyValue,
            int RecordNumber)
        {
            //sql生成
            var sql = new SelectDBCommandBuilder();
            sql.AddSelectTable(TableName);
            sql.AddSelectALLColumn();

            sql.TopRecords = RecordNumber;

            for (var i = 0; i < KeyColumnName.Length; i++)
                sql.AddWhere(WhereRelation.And, KeyColumnName[i], CommandComparison.Equals, KeyValue[i]);

            //数据库连接
            if (_connectionConfig.IsAutoCloseConnection == false)
                if (_database.CheckStatus() == false)
                    throw new Exception("databse connect not open");
            if (_connectionConfig.IsAutoCloseConnection == true) _database.Open();
            var dt = _database.GetDataSet(sql.BuildCommand(), _connectionConfig.WaitTimeout).Tables[0];
            if (_connectionConfig.IsAutoCloseConnection == true) _database.Close();

            IDataTable DT = new IDataTable();
            DT.table = dt;
            return DT;
        }

        /// <summary>
        /// get recordnumber data by filter
        /// 得到指定数据的指定内容
        /// </summary>
        /// <param name="TableName"></param>
        /// <param name="KeyColumnName"></param>
        /// <param name="Comparison"></param>
        /// <param name="KeyValue"></param>
        /// <param name="RecordNumber"></param>
        /// <returns></returns>
        public IDataTable Data(string TableName, string[] KeyColumnName, CommandComparison[] Comparison,
            object[] KeyValue, int RecordNumber)
        {
            //sql生成
            var sql = new SelectDBCommandBuilder();
            sql.AddSelectTable(TableName);
            sql.AddSelectALLColumn();

            sql.TopRecords = RecordNumber;

            for (var i = 0; i < KeyColumnName.Length; i++)
                sql.AddWhere(WhereRelation.And, KeyColumnName[i], (CommandComparison)Comparison[i], KeyValue[i]);

            //数据库连接
            if (_connectionConfig.IsAutoCloseConnection == false)
                if (_database.CheckStatus() == false)
                    throw new Exception("databse connect not open");
            if (_connectionConfig.IsAutoCloseConnection == true) _database.Open();
            var dt = _database.GetDataSet(sql.BuildCommand(), _connectionConfig.WaitTimeout).Tables[0];
            if (_connectionConfig.IsAutoCloseConnection == true) _database.Close();

            IDataTable DT = new IDataTable();
            DT.table = dt;
            return DT;
        }


        /// <summary>
        /// get columns data by filter
        /// 得到指定数据的指定内容
        /// </summary>
        /// <param name="TableName"></param>
        /// <param name="ColumnName"></param>
        /// <param name="KeyColumnName"></param>
        /// <param name="KeyValue"></param>
        /// <returns></returns>
        public IDataTable Data(string TableName, string ColumnName, string KeyColumnName, object KeyValue)
        {
            //sql生成
            var sql = new SelectDBCommandBuilder();
            sql.AddSelectTable(TableName);
            sql.AddSelectColumn("max(" + ColumnName + ")");
            sql.AddWhere(WhereRelation.None, KeyColumnName, CommandComparison.Equals, KeyValue);


            //数据库连接
            if (_connectionConfig.IsAutoCloseConnection == false)
                if (_database.CheckStatus() == false)
                    throw new Exception("databse connect not open");
            if (_connectionConfig.IsAutoCloseConnection == true) _database.Open();
            var dt = _database.GetDataSet(sql.BuildCommand(), _connectionConfig.WaitTimeout).Tables[0];
            if (_connectionConfig.IsAutoCloseConnection == true) _database.Close();

            IDataTable DT = new IDataTable();
            DT.table = dt;
            return DT;
        }

        /// <summary>
        /// get columns data by filter
        /// 得到指定数据的指定内容
        /// </summary>
        /// <param name="TableName"></param>
        /// <param name="ColumnName"></param>
        /// <param name="KeyColumnName"></param>
        /// <param name="KeyValue"></param>
        /// <returns></returns>
        public long Max(string TableName, string ColumnName, string KeyColumnName, object KeyValue)
        {
            long Value;

            //sql生成
            var sql = new SelectDBCommandBuilder();
            sql.AddSelectTable(TableName);
            sql.AddSelectColumn("max(" + ColumnName + ")");
            sql.AddWhere(WhereRelation.None, KeyColumnName, CommandComparison.Equals, KeyValue);


            //数据库连接
            if (_connectionConfig.IsAutoCloseConnection == false)
                if (_database.CheckStatus() == false)
                    throw new Exception("databse connect not open");
            if (_connectionConfig.IsAutoCloseConnection == true) _database.Open();
            Value = Convert.ToInt64(_database.GetSpeciaRecordValue(sql.BuildCommand(), _connectionConfig.WaitTimeout));
            if (_connectionConfig.IsAutoCloseConnection == true) _database.Close();

            return Value;
        }

        /// <summary>
        /// get columns data by filter
        /// 得到指定数据的指定内容
        /// </summary>
        /// <param name="TableName"></param>
        /// <param name="ColumnName"></param>
        /// <returns></returns>
        public IDataTable ColumnData(string TableName, List<string> ColumnName, String OrderByName= "", SortMode sortMode = SortMode.Ascending)
        {
            //sql生成
            var sql = new SelectDBCommandBuilder();
            sql.AddSelectTable(TableName);
            sql.AddSelectColumn(ColumnName);
            if (String.IsNullOrEmpty(OrderByName)==false)
                sql.AddOrderBy(OrderByName, sortMode);

            //数据库连接
            if (_connectionConfig.IsAutoCloseConnection == false)
                if (_database.CheckStatus() == false)
                    throw new Exception("databse connect not open");
            if (_connectionConfig.IsAutoCloseConnection == true) _database.Open();
            var ds = _database.GetDataSet(sql.BuildCommand(), _connectionConfig.WaitTimeout);
            if (_connectionConfig.IsAutoCloseConnection == true) _database.Close();

            IDataTable DT = new IDataTable {table = ds.Tables[0]};
            return DT;
        }


        /// <summary>
        /// get columns data by filter
        /// 得到指定数据的指定内容
        /// </summary>
        /// <param name="TableName"></param>
        /// <param name="ColumnName"></param>
        /// <param name="MatchColumn"></param>
        /// <param name="MatchValue"></param>
        /// <returns></returns>
        public IDataTable ColumnData(string TableName, List<string> ColumnName, string MatchColumn,
            object MatchValue)
        {
            //sql生成
            var sql = new SelectDBCommandBuilder();
            sql.AddSelectTable(TableName);
            sql.AddSelectColumn(ColumnName);
            sql.AddWhere(WhereRelation.None, MatchColumn, CommandComparison.Equals, MatchValue);


            //数据库连接
            if (_connectionConfig.IsAutoCloseConnection == false)
                if (_database.CheckStatus() == false)
                    throw new Exception("databse connect not open");
            if (_connectionConfig.IsAutoCloseConnection == true) _database.Open();
            var ds = _database.GetDataSet(sql.BuildCommand(), _connectionConfig.WaitTimeout);
            if (_connectionConfig.IsAutoCloseConnection == true) _database.Close();

            IDataTable DT = new IDataTable {table = ds.Tables[0]};
            return DT;
        }

        /// <summary>
        /// get columns data by filter
        /// 得到指定数据的指定内容
        /// </summary>
        /// <param name="TableName"></param>
        /// <param name="ColumnName"></param>
        /// <param name="MatchColumn"></param>
        /// <param name="Comparison"></param>
        /// <param name="MatchValue"></param>
        /// <returns></returns>
        public IDataTable ColumnData(string TableName, List<string> ColumnName, string MatchColumn,
            CommandComparison Comparison, object MatchValue)
        {
            //sql生成
            var sql = new SelectDBCommandBuilder();
            sql.AddSelectTable(TableName);
            sql.AddSelectColumn(ColumnName);
            sql.AddWhere(WhereRelation.None, MatchColumn, (CommandComparison)Comparison, MatchValue);


            //数据库连接
            if (_connectionConfig.IsAutoCloseConnection == false)
                if (_database.CheckStatus() == false)
                    throw new Exception("databse connect not open");
            if (_connectionConfig.IsAutoCloseConnection == true) _database.Open();
            var ds = _database.GetDataSet(sql.BuildCommand(), _connectionConfig.WaitTimeout);
            if (_connectionConfig.IsAutoCloseConnection == true) _database.Close();

            IDataTable DT = new IDataTable {table = ds.Tables[0]};
            return DT;
        }

        /// <summary>
        /// get columns data by filter
        /// 得到指定数据的指定内容
        /// </summary>
        /// <param name="TableName"></param>
        /// <param name="ColumnName"></param>
        /// <param name="MatchColumn"></param>
        /// <param name="Comparison"></param>
        /// <param name="MatchValue"></param>
        /// <returns></returns>
        public IDataTable ColumnData(string TableName, List<string> ColumnName, string MatchColumn,
            CommandComparison Comparison, object[] MatchValue)
        {
            //sql生成
            var sql = new SelectDBCommandBuilder();
            sql.AddSelectTable(TableName);
            sql.AddSelectColumn(ColumnName);
            sql.AddWhere(WhereRelation.None, MatchColumn, (CommandComparison)Comparison, MatchValue);


            //数据库连接
            if (_connectionConfig.IsAutoCloseConnection == false)
                if (_database.CheckStatus() == false)
                    throw new Exception("databse connect not open");
            if (_connectionConfig.IsAutoCloseConnection == true) _database.Open();
            var ds = _database.GetDataSet(sql.BuildCommand(), _connectionConfig.WaitTimeout);
            if (_connectionConfig.IsAutoCloseConnection == true) _database.Close();

            IDataTable DT = new IDataTable {table = ds.Tables[0]};
            return DT;
        }

        /// <summary>
        /// get columns data by filter
        /// 得到指定数据的指定内容
        /// </summary>
        /// <param name="TableName"></param>
        /// <param name="ColumnName"></param>
        /// <param name="MatchColumn"></param>
        /// <param name="Comparison"></param>
        /// <param name="MatchValue"></param>
        /// <returns></returns>
        public IDataTable ColumnData(string TableName, List<string> ColumnName, string[] MatchColumn,
            CommandComparison[] Comparison, object[] MatchValue)
        {
            //sql生成
            var sql = new SelectDBCommandBuilder();
            sql.AddSelectTable(TableName);
            sql.AddSelectColumn(ColumnName);

            for (var i = 0; i < MatchColumn.Length; i++)
                sql.AddWhere(WhereRelation.And, MatchColumn[i], (CommandComparison)Comparison[i], MatchValue[i]);


            //数据库连接
            if (_connectionConfig.IsAutoCloseConnection == false)
                if (_database.CheckStatus() == false)
                    throw new Exception("databse connect not open");
            if (_connectionConfig.IsAutoCloseConnection == true) _database.Open();
            var ds = _database.GetDataSet(sql.BuildCommand(), _connectionConfig.WaitTimeout);
            if (_connectionConfig.IsAutoCloseConnection == true) _database.Close();

            IDataTable DT = new IDataTable {table = ds.Tables[0]};
            return DT;
        }

        /// <summary>
        /// get columns data by filter
        /// 得到指定数据的指定内容
        /// </summary>
        /// <param name="TableName"></param>
        /// <param name="ColumnName"></param>
        /// <param name="MatchColumn"></param>
        /// <param name="Comparison"></param>
        /// <param name="MatchValue"></param>
        /// <param name="GroupByName"></param>
        /// <returns></returns>
        public IDataTable ColumnData(string TableName, List<string> ColumnName, string[] MatchColumn,
            CommandComparison[] Comparison, object[] MatchValue, string GroupByName)
        {
            //sql生成
            var sql = new SelectDBCommandBuilder();
            sql.AddSelectTable(TableName);
            sql.AddSelectColumn(ColumnName);

            for (var i = 0; i < MatchColumn.Length; i++)
                sql.AddWhere(WhereRelation.And, MatchColumn[i], (CommandComparison)Comparison[i], MatchValue[i]);

            sql.AddGroupBy(GroupByName);

            //数据库连接
            if (_connectionConfig.IsAutoCloseConnection == false)
                if (_database.CheckStatus() == false)
                    throw new Exception("databse connect not open");
            if (_connectionConfig.IsAutoCloseConnection == true) _database.Open();
            var ds = _database.GetDataSet(sql.BuildCommand(), _connectionConfig.WaitTimeout);
            if (_connectionConfig.IsAutoCloseConnection == true) _database.Close();

            IDataTable DT = new IDataTable {table = ds.Tables[0]};
            return DT;
        }
    }
}