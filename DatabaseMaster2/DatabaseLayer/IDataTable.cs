using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data;
using System.IO;
using MongoDB.Bson;
using Newtonsoft.Json;

namespace DatabaseMaster2
{
    public class IDataTable
    {
        private DataTable _table;

        public DataTable table
        {
            set => _table = value;
        }

        /// <summary>
        ///  select Columns
        /// 按序号筛选列
        /// </summary>
        /// <param name="ColumnsIndex"></param>
        /// <returns></returns>
        public IDataTable SelectColumns(int[] ColumnsIndex)
        {
            for (var i = _table.Columns.Count - 1; i >= 0; i--)
                if (ColumnsIndex.Contains(i) == false)
                    _table.Columns.RemoveAt(i);

            var dt = new IDataTable {table = _table};
            return dt;
        }

        /// <summary>
        ///  select Columns
        /// 按名称筛选列
        /// </summary>
        /// <param name="ColumnsName"></param>
        /// <returns></returns>
        public IDataTable SelectColumns(string[] ColumnsName)
        {
            for (var i = _table.Columns.Count - 1; i >= 0; i--)
                if (ColumnsName.Contains(_table.Columns[i].ColumnName) == false)
                    _table.Columns.Remove(_table.Columns[i].ColumnName);

            var dt = new IDataTable {table = _table};
            return dt;
        }

        /// <summary>
        /// DataTable to Jason string
        /// 转Jason
        /// </summary>
        /// <returns></returns>
        public string ToJason()
        {
            var JsonString = string.Empty;

            JsonString = JsonConvert.SerializeObject(_table);

            return JsonString;
        }

        /// <summary>
        /// DataTable to Jason string
        /// 转 Jason数组
        /// </summary>
        /// <returns></returns>
        public string[] ToJasonArray()
        {
            return ConvertDataTableToJson(_table);
        }

        /// <summary>
        /// Get DataTable
        /// 返回DataTable
        /// </summary>
        /// <returns></returns>
        public DataTable ToDataTable()
        {
            return _table;
        }

        private string[] ConvertDataTableToJson(DataTable dt)

        {
           String[] str=new string[dt.Rows.Count];
           String split = "";

            if (dt.Rows.Count > 0)
            {
                for (int i = 0; i < dt.Rows.Count; i++)
                {
                    //获取列名

                    var result = "";

                    foreach (DataColumn dc in dt.Columns)
                    {
                        if (dt.Rows[i][dc.ColumnName].GetType() == typeof(String))
                            split = "\"";
                        result += string.Format("\"{0}\":{1},", dc.ColumnName, dt.Rows[i][dc.ColumnName]);
                    }
                    

                    result = result.Remove(result.Length-1,1);

                    result = "{" + result + "}";

                    str[i] = result;
                }

                return str;

            }
            else
            {
                return str;
            }
        }

        /// <summary>
        /// Lamdba
        /// 返回Lamdba
        /// </summary>
        /// <returns></returns>
        public EnumerableRowCollection<DataRow>  ToLamdba()
        {
            return _table.AsEnumerable();
        }

        /// <summary>
        /// Get DataTable page
        /// 返回分页
        /// </summary>
        /// <param name="StartNumber"></param>
        /// <param name="EndNumber"></param>
        /// <returns></returns>
        public IDataTable ToTablePage(int StartNumber, int EndNumber)
        {
            var dt = new IDataTable {table = _table.AsEnumerable().Skip(StartNumber).Take(EndNumber).CopyToDataTable()};
            return dt;
        }

        /// <summary>
        /// DataTable to DataView
        /// 返回DataView
        /// </summary>
        /// <returns></returns>
        public DataView ToDataView()
        {
            return _table.DefaultView;
        }

        /// <summary>
        /// DataTable to List
        /// 返回List行
        /// </summary>
        /// <returns></returns>
        public List<DataRow> ToList()
        {
            return _table.AsEnumerable().ToList();
        }

        /// <summary>
        ///  DataTable to List Page
        /// 返回列表分页
        /// </summary>
        /// <param name="StartNumber"></param>
        /// <param name="EndNumber"></param>
        /// <returns></returns>
        public List<DataRow> ToListPage(int StartNumber, int EndNumber)
        {
            return _table.AsEnumerable().Skip(StartNumber).Take(EndNumber).ToList();
        }

        /// <summary>
        /// DataTable to Hashtable
        /// 返回指定行Hashtable
        /// </summary>
        /// <param name="RowsIndex"></param>
        /// <returns></returns>
        public Hashtable ToHashTable(Int32 RowsIndex)
        {
            try
            {
                if (_table.Rows.Count == 0)
                    return null;

                if (RowsIndex>_table.Rows.Count)
                    throw  new Exception("Rows index error");

                DataRow row = _table.Rows[RowsIndex];
                Hashtable hash = new Hashtable();
                for (int i = 0; i < row.Table.Columns.Count; i++)
                {
                    hash.Add(row.Table.Columns[i].ColumnName,row[i]);
                }
                return hash;
            }
            catch
            {
                return null;
            }
        }

        /// <summary>
        /// DataTable to Hashtable
        /// 返回Hashtable数组
        /// </summary>
        /// <returns></returns>
        public Hashtable[] ToHashTable()
        {
            try
            {
                if (_table.Rows.Count == 0)
                    return null;
                Hashtable[] hash = new Hashtable[_table.Rows.Count];

                for (int i = 0; i < _table.Rows.Count; i++)
                {
                    DataRow row = _table.Rows[i];

                    hash[i]=new Hashtable();

                    for (int j = 0; j < row.Table.Columns.Count; j++)
                    {
                        hash[i].Add(row.Table.Columns[j].ColumnName, row[j]);
                    }
                }
              
                return hash;
            }
            catch
            {
                return null;
            }
        }

        /// <summary>
        /// DataTable to Hashtable
        /// 返回Hashtable数组
        /// </summary>
        /// <returns></returns>
        public List<Hashtable> ToListHashTable()
        {
            try
            {
                if (_table.Rows.Count == 0)
                    return null;
                List<Hashtable> hash = new List<Hashtable>();

                for (int i = 0; i < _table.Rows.Count; i++)
                {
                    DataRow row = _table.Rows[i];

                    hash[i] = new Hashtable();

                    for (int j = 0; j < row.Table.Columns.Count; j++)
                    {
                        hash[i].Add(row.Table.Columns[j].ColumnName, row[j]);
                    }
                }

                return hash;
            }
            catch
            {
                return null;
            }
        }

        /// <summary>
        /// 保存到TXT
        /// </summary>
        /// <param name="dt"></param>
        /// <param name="vOutputFilePath"></param>
        /// <returns></returns>
        public bool ToTxt(DataTable dt, string vOutputFilePath)
        {
            StringBuilder sTxtContent;

            try
            {
                if (File.Exists(vOutputFilePath))
                    File.Delete(vOutputFilePath);

                sTxtContent = new StringBuilder();

                //数据
                foreach (DataRow row in dt.Rows)
                    for (var i = 0; i < dt.Columns.Count; i++)
                    {
                        sTxtContent.Append(row[i].ToString().Trim());
                        sTxtContent.Append(i == dt.Columns.Count - 1 ? "\r\n" : "\t");
                    }

                File.WriteAllText(vOutputFilePath, sTxtContent.ToString(), Encoding.Unicode);
                return true;
            }
            catch (Exception ex)
            {
                return false;
            }
        }
    }
}