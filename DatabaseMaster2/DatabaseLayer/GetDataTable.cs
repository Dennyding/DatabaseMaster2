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
        private SelectDBCommandBuilder sql = new SelectDBCommandBuilder();

        public DatabaseGetData(ConnectionConfig config, DatabaseInterface database, String
            TableName)
        {
            _connectionConfig = config;
            _database = database;
            if(!String.IsNullOrEmpty(TableName))
                sql.AddSelectTable(TableName);
        }

        public DatabaseGetData(ConnectionConfig config, DatabaseInterface database, List<string> TableName)
        {
            _connectionConfig = config;
            _database = database;
            if (TableName.Count>0)
                sql.AddSelectTable(TableName);
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
        /// clear filter
        /// 清除过滤条件
        /// </summary>
        /// <returns></returns>
        public DatabaseGetData Clear()
        {
            sql.ClearCommand();
           
            return this;
        }

        /// <summary>
        /// get all data
        /// 得到表中所有数据
        /// </summary>
        /// <returns></returns>
        public IDataTable Data()
        {
            //数据库连接
            if (_connectionConfig.IsAutoCloseConnection == false)
                if (_database.CheckStatus() == false)
                    throw new Exception("databse connect not open");
            if (_connectionConfig.IsAutoCloseConnection == true) _database.Open();
            var ds = _database.GetDataSet(sql.BuildCommand(), _connectionConfig.WaitTimeout);
            if (_connectionConfig.IsAutoCloseConnection == true) _database.Close();

            IDataTable DT = new IDataTable { table = ds.Tables[0] };
            return DT;
        }

        /// <summary>
        /// check data exist
        /// 检查表中是否存在指定内容
        /// </summary>
        /// <returns></returns>
        public bool Exist()
        {
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
        /// get first data by filter
        /// 得到指定条件的一个数据
        /// </summary>
        /// <returns></returns>
        public object First()
        {
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
        /// get all data
        /// 得到表中计数
        /// </summary>
        /// <param name="OrderByName"></param>
        /// <param name="sortMode"></param>
        /// <returns></returns>
        public object Count()
        {
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
        /// get all data
        /// 得到表中指定行数数据
        /// </summary>
        /// <param name="OrderByName"></param>
        /// <param name="sortMode"></param>
        /// <returns></returns>
        public DatabaseGetData TopNumber(Int32 RowsNumber)
        {
            sql.TopRecords= RowsNumber;

            return this;
        }

        /// <summary>
        /// get all columns
        /// 得到表中所有列
        /// </summary>
        /// <param name="OrderByName"></param>
        /// <param name="sortMode"></param>
        /// <returns></returns>
        public DatabaseGetData Columns()
        {
            sql.AddSelectALLColumn();
         
            return this;
        }

        /// <summary>
        /// get columns list
        /// 得到表中指定列
        /// </summary>
        /// <returns></returns>
        public DatabaseGetData Columns(String ColumnName)
        {
            sql.AddSelectColumn(ColumnName);

            return this;
        }

        /// <summary>
        /// get columns list
        /// 得到表中指定列
        /// </summary>
        /// <param name="OrderByName"></param>
        /// <param name="sortMode"></param>
        /// <returns></returns>
        public DatabaseGetData Columns(List<string> ColumnName)
        {
            sql.AddSelectColumn(ColumnName);
           
            return this;
        }

        /// <summary>
        /// set sort
        /// 指定排序条件
        /// </summary>
        /// <param name="OrderByName"></param>
        /// <param name="sortMode"></param>
        /// <returns></returns>
        public DatabaseGetData OrderBy(String OrderByName, SortMode sortMode = SortMode.Ascending)
        {
            if (String.IsNullOrEmpty(OrderByName) == false)
                sql.AddOrderBy(OrderByName, sortMode);

            return this;
        }

        /// <summary>
        /// set group name
        /// 指定分组字段
        /// </summary>
        /// <param name="GroupByName"></param>
        /// <returns></returns>
        public DatabaseGetData GroupBy(string GroupByName)
        {
            sql.AddGroupBy(GroupByName);

            return this;
        }


        /// <summary>
        /// get data by filter
        /// 设定表中指定条件
        /// </summary>
        /// <param name="ColumnName"></param>
        /// <param name="Value"></param>
        /// <param name="OrderByName"></param>
        /// <param name="sortMode"></param>
        /// <returns></returns>
        public DatabaseGetData Where(string ColumnName, object Value)
        {
            sql.AddWhere(WhereRelation.And, ColumnName, CommandComparison.Equals, Value);

            return this;
        }

        /// <summary>
        /// get data by filter
        /// 设定表中指定条件
        /// </summary>
        /// <param name="ColumnName"></param>
        /// <param name="Comparison"></param>
        /// <param name="Value"></param>
        /// <returns></returns>
        public DatabaseGetData Where(string ColumnName, CommandComparison Comparison,object Value)
        {
            sql.AddWhere(WhereRelation.And, ColumnName, (CommandComparison)Comparison, Value);
       
            return this;
        }

        /// <summary>
        /// get data by filter
        /// 设定表中指定条件
        /// </summary>
        /// <param name="ColumnName"></param>
        /// <param name="Value"></param>
        /// <returns></returns>
        public DatabaseGetData Where(string[] ColumnName, object[] Value)
        {
            for (var i = 0; i < ColumnName.Length; i++)
                sql.AddWhere(WhereRelation.And, ColumnName[i], CommandComparison.Equals, Value[i]);

            return this;
        }


        /// <summary>
        /// get data by filter
        /// 设定表中指定条件
        /// </summary>
        /// <param name="ColumnName"></param>
        /// <param name="Comparison"></param>
        /// <param name="Value"></param>
        /// <returns></returns>
        public DatabaseGetData Where(string[] ColumnName, CommandComparison[] Comparison,
            object[] Value)
        {
            for(var i = 0; i < ColumnName.Length; i++)
                sql.AddWhere(WhereRelation.And, ColumnName[i], Comparison[i], Value[i]);

            return this;
        }

        /// <summary>
        /// get data by filter
        /// 设定表中指定条件
        /// </summary>
        /// <param name="ColumnName"></param>
        /// <param name="Comparison"></param>
        /// <param name="Value"></param>
        /// <param name="raRelation"></param>
        /// <returns></returns>
        public DatabaseGetData Where(string[] ColumnName, CommandComparison[] Comparison,
            object[] Value,WhereRelation[] raRelation)
        {
            for (var i = 0; i < ColumnName.Length; i++)
                sql.AddWhere(raRelation[i], ColumnName[i], Comparison[i], Value[i]);
          
            return this;
        }


        /// <summary>
        /// get data by filter
        /// 设定表中指定条件
        /// </summary>
        /// <param name="ColumnName"></param>
        /// <param name="Comparison"></param>
        /// <param name="Value"></param>
        /// <param name="Table1ColumnName"></param>
        /// <param name="Table2ColumnName"></param>
        /// <returns></returns>
        public DatabaseGetData Where(string[] ColumnName, CommandComparison[] Comparison,
            object[] Value,
            string[] Table1ColumnName, string[] Table2ColumnName)
        {
            for (var i = 0; i < ColumnName.Length; i++)
                sql.AddWhere(WhereRelation.And, ColumnName[i], (CommandComparison)Comparison[i], Value[i]);

            for (var i = 0; i < Table1ColumnName.Length; i++)
                sql.AddWhereByRelationShip(WhereRelation.And, Table1ColumnName[i], CommandComparison.Equals,
                    Table2ColumnName[i]);

            return this;
        }

        /// <summary>
        /// get data by filter
        /// 设定表中指定条件
        /// </summary>
        /// <param name="Table1ColumnName"></param>
        /// <param name="Table2ColumnName"></param>
        /// <returns></returns>
        public DatabaseGetData Where(string[] Table1ColumnName, string[] Table2ColumnName)
        {
            for (var i = 0; i < Table1ColumnName.Length; i++)
                sql.AddWhereByRelationShip(WhereRelation.And, Table1ColumnName[i], CommandComparison.Equals,
                    Table2ColumnName[i]);
            
            return this;
        }



        /// <summary>
        /// get data by filter
        /// 设定表中指定条件
        /// </summary>
        /// <param name="MatchColumn"></param>
        /// <param name="Comparison"></param>
        /// <param name="MatchValue"></param>
        /// <returns></returns>
        public DatabaseGetData Where(string MatchColumn,
            CommandComparison Comparison, object[] MatchValue)
        {
            sql.AddWhere(WhereRelation.And, MatchColumn, Comparison, MatchValue);

            return this;
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
        public DatabaseGetData Max(string ColumnName)
        {
            sql.AddSelectColumn("max(" + ColumnName + ")");
          
            return this;
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
    }
}