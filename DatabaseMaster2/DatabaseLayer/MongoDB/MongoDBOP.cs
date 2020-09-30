using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data;
using System.Collections;
using MongoDB.Bson;
using MongoDB.Driver;

namespace DatabaseMaster2
{
    internal class MongoDBOP
    {
        /// <summary>
        /// 查询条件
        /// </summary>
        /// <param name="ColumnName"></param>
        /// <param name="Value"></param>
        /// <param name="comparison"></param>
        /// <returns></returns>
        public static FilterDefinition<BsonDocument> GetFilterOP(String ColumnName, Object Value,
            CommandComparison comparison)
        {
            switch ((int)comparison)
            {
                case 1:
                    return Builders<BsonDocument>.Filter.Eq(ColumnName, GetValueType(Value));
                case 2:
                    return Builders<BsonDocument>.Filter.Ne(ColumnName, GetValueType(Value));
                case 5:
                    return Builders<BsonDocument>.Filter.Gt(ColumnName, GetValueType(Value));
                case 6:
                    return Builders<BsonDocument>.Filter.Gte(ColumnName, GetValueType(Value));
                case 7:
                    return Builders<BsonDocument>.Filter.Lt(ColumnName, GetValueType(Value));
                case 8:
                    return Builders<BsonDocument>.Filter.Lte(ColumnName, GetValueType(Value));
                case 13:
                    return Builders<BsonDocument>.Filter.Eq(ColumnName, new ObjectId(Value.ToString()));
                default:
                    return new BsonDocument();
            }
        }

        /// <summary>
        /// 数据类型匹配
        /// </summary>
        /// <param name="Value"></param>
        /// <returns></returns>
        public static Object GetValueType(Object Value)
        {
            String type = Value.GetType().ToString();

            switch (type)
            {
                case "System.DateTime":
                    return Convert.ToString(Value);
                case "System.String":
                    return Convert.ToString(Value);
                default:
                    return Value;
            }
        }


        /// <summary>
        /// 多种查询条件组合
        /// </summary>
        /// <param name="ColumnName"></param>
        /// <param name="Value"></param>
        /// <returns></returns>
        public static FilterDefinition<BsonDocument> GetWhere(String[] ColumnName, Object[] Value)
        {
            FilterDefinition<BsonDocument> definition = new BsonDocument();

            for (int i = 0; i < ColumnName.Length; i++)
            {

                FilterDefinition<BsonDocument> def = MongoDBOP.GetFilterOP(ColumnName[i], Value[i], CommandComparison.Equals);

                definition = definition & def;

            }

            return definition;
        }

        /// <summary>
        /// 多种查询条件组合
        /// </summary>
        /// <param name="ColumnName"></param>
        /// <param name="Value"></param>
        /// <param name="comparison"></param>
        /// <param name="relation"></param>
        /// <returns></returns>
        public static FilterDefinition<BsonDocument> GetWhere(String[] ColumnName, Object[] Value,
            CommandComparison[] comparison, WhereRelation[] relation)
        {
            FilterDefinition<BsonDocument> definition = new BsonDocument();

            for (int i = 0; i < ColumnName.Length; i++)
            {

                FilterDefinition<BsonDocument> def = MongoDBOP.GetFilterOP(ColumnName[i], Value[i], comparison[i]);


                if (relation[i] == WhereRelation.And)
                    definition = definition & def;
                else
                    definition = definition | def;


            }

            return definition;
        }

        /// <summary>
        /// 将HashTable转为Jason
        /// </summary>
        /// <param name="hr"></param>
        /// <returns></returns>
        public static string HashtableToJson(Hashtable hr)
        {
            string json = "{";
            foreach (DictionaryEntry row in hr)
            {
                try
                {
                    string key = "\"" + row.Key + "\":";
                    if (row.Value is Hashtable)
                    {
                        Hashtable t = (Hashtable)row.Value;
                        if (t.Count > 0)
                        {
                            json += key + HashtableToJson(t) + ",";
                        }
                        else
                        {
                            json += key + "{},";
                        }
                    }
                    else if (row.Value.ToString().StartsWith("[") && row.Value.ToString().EndsWith("]"))
                    {
                        string value = row.Value.ToString() + ",";
                        json += key + value;
                    }
                    else
                    {
                        string value;
                        if (row.Value.GetType().ToString().Equals("System.String"))
                        {
                            if (row.Value.ToString().StartsWith("ISODate(") && row.Value.ToString().EndsWith(")"))
                                value = "ISODate(\"" +
                                        BsonDateTime.Create(row.Value.ToString().Replace("ISODate(", "")
                                            .Replace(")", "")) + "\"),";
                            else if (row.Value.ToString().Equals("new Date()"))
                            {
                                value = row.Value.ToString() + ",";
                            }
                            else if (row.Value.ToString().StartsWith("ObjectId(") && row.Value.ToString().EndsWith(")"))
                            {
                                value = row.Value.ToString() + ",";
                            }
                            else
                                value = "\"" + row.Value.ToString() + "\",";
                        }
                        else if (row.Value.GetType().ToString().Equals("System.DateTime"))
                        {
                            value = "\"" + row.Value.ToString() + "\",";
                        }
                        else
                        {
                            value = row.Value.ToString() + ",";
                        }

                        json += key + value;
                    }
                }
                catch
                {
                }
            }

            //  json = MyString.ClearEndChar(json);  


            json = json.Remove(json.Length - 1) + "}";
            return json;
        }

    }

}
