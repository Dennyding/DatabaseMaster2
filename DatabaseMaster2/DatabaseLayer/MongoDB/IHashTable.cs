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
using Newtonsoft.Json.Linq;
using Org.BouncyCastle.Crypto.Engines;

namespace DatabaseMaster2
{
    public class IHashTable
    {
        private List<Hashtable> _data=new List<Hashtable>();
        private DataTable _table;


        public IHashTable()
        {
            ;
        }

        /// <summary>
        /// 初始化hashtable
        /// </summary>
        /// <param name="hr"></param>
        public IHashTable(List<Hashtable> hr)
        {
            _data = JsonHashTable(hr);
            _table = HashTableToDataTable(hr);
        }

        /// <summary>
        ///  JsonHashTable to HashTable
        /// 将复杂Json Hashtable转为嵌套 HashTable
        /// </summary>
        /// <param name="hr"></param>
        /// <returns></returns>
        public List<Hashtable> JsonHashTable(List<Hashtable> hr)
        {
            List < Hashtable > v=new List<Hashtable>();

            foreach (var ht in hr)
            {
                Hashtable temp=new Hashtable();
                foreach (DictionaryEntry de in ht) //ht为一个Hashtable实例
                {
                    if (de.Value.GetType().ToString() == "Newtonsoft.Json.Linq.JObject")
                    {
                        temp.Add(de.Key, JsonConvert.DeserializeObject<Hashtable>(de.Value.ToString()));
                    }
                  else if (de.Value.GetType().ToString() == "Newtonsoft.Json.Linq.JArray")
                    {
                        if (de.Value.ToString().Split('{').Length <= 1)
                        {
                            temp.Add(de.Key, de.Value);
                        }
                        else
                        {
                            JArray arr = JArray.Parse(de.Value.ToString());
                            List<Hashtable> aHashtable = new List<Hashtable>();
                            foreach (var ss in arr)
                            {
                                aHashtable.Add(JsonConvert.DeserializeObject<Hashtable>(ss.ToString()));
                            }
                            aHashtable = JsonHashTable(aHashtable);
                            temp.Add(de.Key, aHashtable);
                        }
                        
                    }
                    else
                    {
                        temp.Add(de.Key,de.Value);
                    }
                }
                v.Add(temp);
            }

            return v;
        }

        /// <summary>
        /// HashTable to DataTable
        /// 将HashTable 转为DataTable
        /// </summary>
        /// <param name="tableList"></param>
        /// <returns></returns>
        private DataTable HashTableToDataTable(List<Hashtable> tableList)
        {
            DataTable dt = new DataTable();
            if (tableList != null && tableList.Count > 0)
            {
                //把Key 添加到Table中 成为列名称
                foreach (string item in tableList[0].Keys)
                {
                    dt.Columns.Add(item, typeof(string));
                }
                for (int i = 0; i < tableList.Count; i++)
                {
                    Hashtable ht = tableList[i];
                    DataRow dr = dt.NewRow();

                    foreach (DictionaryEntry de in ht)
                    {
                        //把Value 添加到对应的列名下边
                        if (de.Value == null)
                            dr[de.Key.ToString()] = "";
                        else
                            dr[de.Key.ToString()] = de.Value.ToString();
                    }
                    dt.Rows.Add(dr);

                }
            }

            return dt;
        }


        private DataTable table
        {
            set => _table = value;
        }

        /// <summary>
        /// Get Hashtable
        /// 返回Hashtable
        /// </summary>
        /// <returns></returns>
        public List<Hashtable> ToListHashTable()
        {
            return _data;
        }

        /// <summary>
        ///  select Columns
        /// 按序号筛选列
        /// </summary>
        /// <param name="ColumnsIndex"></param>
        /// <returns></returns>
        public IHashTable SelectColumns(int[] ColumnsIndex)
        {
            for (var i = _table.Columns.Count - 1; i >= 0; i--)
                if (ColumnsIndex.Contains(i) == false)
                    _table.Columns.RemoveAt(i);

            var dt = new IHashTable { table = _table};
            return dt;
        }

        /// <summary>
        ///  select Columns
        /// 按名称筛选列
        /// </summary>
        /// <param name="ColumnsName"></param>
        /// <returns></returns>
        public IHashTable SelectColumns(string[] ColumnsName)
        {
            for (var i = _table.Columns.Count - 1; i >= 0; i--)
                if (ColumnsName.Contains(_table.Columns[i].ColumnName) == false)
                    _table.Columns.Remove(_table.Columns[i].ColumnName);

            var dt = new IHashTable { table = _table};
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
        public IHashTable ToTablePage(int StartNumber, int EndNumber)
        {
            var dt = new IHashTable { table = _table.AsEnumerable().Skip(StartNumber).Take(EndNumber).CopyToDataTable()};
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


    }
}