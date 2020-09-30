using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Xml;
using System.Configuration;
using System.Xml.Linq;
using System.Collections;

namespace DatabaseMaster2
{
    public class ConfigureSetting
    {
        /// <summary>
        /// 读取APP字符串
        /// </summary>
        /// <param name="Key">参数名字</param>
        /// <returns></returns>
        public static String GetAPPConfig(String Key)
        {
            return ConfigurationManager.AppSettings[Key];
        }

        /// <summary>
        /// 设置APP字符串
        /// </summary>
        /// <param name="Key">参数名字</param>
        /// <param name="Value">值</param>
        /// <returns></returns>
        public static void SetAPPConfig(String Key,String Value)
        {
           Configuration config= ConfigurationManager.OpenExeConfiguration(ConfigurationUserLevel.None);

            config.AppSettings.Settings[Key].Value = Value;

            config.Save();
        }

    }

    public class DatabaseXMLConfig
    {

        /// <summary>
        /// 生成数据库文件
        /// </summary>
        /// <param name="DefaultDatabase">数据库类型,MSSQL,MYSQL,Oracle,OleDB,SQLite</param>
        /// <param name="ConnectName">连接字符串名称</param>
        /// <param name="ServerIP">数据库IP地址</param>
        /// <param name="DatabaseName">数据库名字</param>
        /// <param name="UserName">用户名</param>
        /// <param name="Password">密码</param>
        public static void SetDatabaseString(String DefaultDatabase, String ConnectName, String ServerIP,String DatabaseName, String UserName,String Password)
        {
            String Value = "";

            if (DefaultDatabase == "MSSQL")
                Value = "Data Source=" + ServerIP + ";database=" + DatabaseName + ";uid=" + UserName + ";pwd=" + Password;
            else if (DefaultDatabase == "MYSQL")
                Value = "server=" + ServerIP + ";database=" + DatabaseName + ";uid=" + UserName + ";pwd=" + Password;
            else if (DefaultDatabase == "Oracle")
            Value = "Data Source = (DESCRIPTION = (ADDRESS = (PROTOCOL = TCP)(HOST = " + ServerIP 
                    + "(PORT = 1521));(CONNECT_DATA = (SID =" + DatabaseName + ")));User Id=" + UserName + ";Password=" + Password;
            else if (DefaultDatabase == "SQLite")
                Value = "Data Source=" + ServerIP + ";Version=3;";
            else
                Value = "";

            Value =StringEncrypt.DataEncrypt(StringEncrypt.EncryptType.MD5, Value);

            XmlStream.setXmlValue(AppDomain.CurrentDomain.BaseDirectory + "DatabaseConfig.xml", ConnectName, "ConnectString", Value);
            XmlStream.setXmlValue(AppDomain.CurrentDomain.BaseDirectory + "DatabaseConfig.xml", ConnectName, "Name", DefaultDatabase);
        }

        /// <summary>
        /// 生成数据库文件
        /// </summary>
        /// <param name="DefaultDatabase">数据库类型,MSSQL,MYSQL,Oracle,OleDB,SQLite</param>
        /// <param name="ConnectName">连接字符串名称</param>
        /// <param name="ServerIP">数据库IP地址</param>
        /// <param name="DatabaseName">数据库名字</param>
        /// <param name="UserName">用户名</param>
        /// <param name="Password">密码</param>
        /// <param name="Type"></param>
        public static void SetDatabaseString(String DefaultDatabase, String ConnectName, String ServerIP, String DatabaseName, String UserName, String Password, StringEncrypt.EncryptType Type)
        {
            String Value = "";

            if (DefaultDatabase == "MSSQL")
                Value = "Data Source=" + ServerIP + ";database=" + DatabaseName + ";uid=" + UserName + ";pwd=" + Password;
            else if (DefaultDatabase == "MYSQL")
                Value = "server=" + ServerIP + ";database=" + DatabaseName + ";uid=" + UserName + ";pwd=" + Password;
            else if (DefaultDatabase == "Oracle")
                Value = "Data Source = (DESCRIPTION = (ADDRESS = (PROTOCOL = TCP)(HOST = " + ServerIP
                        + "(PORT = 1521));(CONNECT_DATA = (SID =" + DatabaseName + ")));User Id=" + UserName + ";Password=" + Password;
            else if (DefaultDatabase == "SQLite")
                Value = "Data Source=" + ServerIP + ";Version=3;";
            else
                Value = "";

            if (Type != StringEncrypt.EncryptType.None)
                Value = StringEncrypt.DataEncrypt(Type, Value);

            XmlStream.setXmlValue(AppDomain.CurrentDomain.BaseDirectory + "DatabaseConfig.xml", ConnectName, "ConnectString", Value);
            XmlStream.setXmlValue(AppDomain.CurrentDomain.BaseDirectory + "DatabaseConfig.xml", ConnectName, "Name", DefaultDatabase);
        }

    }

    public class XmlStream
    {
        private static XDocument xmlDocSave;

        /// <summary>  
        /// 返回XMl文件指定元素的指定属性值  
        /// </summary>  
        /// <param name="FileName"></param>
        /// <param name="xmlElement">指定元素</param>  
        /// <param name="xmlAttribute">指定属性</param>  
        /// <returns></returns>  
        public static string getXmlValue(String FileName, string xmlElement, string xmlAttribute)
        {
            try
            {
                XDocument xmlDoc = XDocument.Load(FileName);
                var results = from c in xmlDoc.Descendants(xmlElement)
                              select c;
                string s = "";
                foreach (var result in results)
                {
                    s = result.Attribute(xmlAttribute).Value.ToString();
                }
                return s;
            }
            catch (System.Exception ex)
            {
                return ex.Message;
            }

        }

        /// <summary>  
        /// 返回XMl文件指定元素的指定属性值  
        /// </summary>  
        /// <param name="FileName"></param>
        /// <param name="xmlElement">指定元素</param>  
        /// <param name="xmlAttribute">指定属性</param>  
        /// <returns></returns>  
        public static string getXmlValueFromVar(String FileName, string xmlElement, string xmlAttribute)
        {
            try
            {
                XDocument xmlDoc = xmlDocSave;
                var results = from c in xmlDoc.Descendants(xmlElement)
                              select c;
                string s = "";
                foreach (var result in results)
                {
                    s = result.Attribute(xmlAttribute).Value.ToString();
                }
                return s;
            }
            catch (System.Exception ex)
            {
                return ex.Message;
            }

        }

        /// <summary>  
        /// 返回XMl文件指定元素的指定属性值  
        /// </summary>  
        /// <param name="FileName"></param>
        /// <param name="xmlElement">指定元素</param>  
        /// <param name="xmlAttribute">指定属性</param>  
        /// <returns></returns>  
        public static string getXmlValueFromVar(string xmlElement, string xmlAttribute)
        {
            try
            {
                XDocument xmlDoc = xmlDocSave;
                var results = from c in xmlDoc.Descendants(xmlElement)
                              select c;
                string s = "";
                foreach (var result in results)
                {
                    s = result.Attribute(xmlAttribute).Value.ToString();
                }
                return s;
            }
            catch (System.Exception ex)
            {
                return ex.Message;
            }

        }

        /// <summary>  
        /// 返回XMl文件指定元素的所有属性值  
        /// </summary>  
        /// <param name="FileName"></param>
        /// <param name="xmlElement">指定元素</param>  
        /// <param name="xmlAttribute">指定属性</param>  
        /// <returns></returns>  
        public static List<Hashtable> getAllXmlValue(String FileName, string xmlAttribute)
        {
            List<Hashtable> s = new List<Hashtable>();
            try
            {
                XDocument xmlDoc = XDocument.Load(FileName);
                var results = from c in xmlDoc.Descendants()
                              select c;
                
                foreach (var result in results)
                {
                    Hashtable ht = new Hashtable();
                    ht.Add("Name",result.Name.ToString());

                    string Parent = result.Parent != null ? result.Parent.Name.ToString() : "";
                    ht.Add("Parent", Parent);
                    if (result.Attribute(xmlAttribute)==null)
                        ht.Add("Value", "");
                    else
                        ht.Add("Value", result.Attribute(xmlAttribute).Value.ToString());

                    s.Add(ht);
                }

                return s;
            }
            catch (System.Exception)
            {
                return s;
            }

        }

        /// <summary>  
        /// 设置XMl文件指定元素的指定属性的值  
        /// </summary>  
        /// <param name="FileName"></param>
        /// <param name="xmlElement">指定元素</param>  
        /// <param name="xmlAttribute">指定属性</param>  
        /// <param name="xmlValue">指定值</param>  
        public static Boolean setXmlValue(String FileName,  string xmlElement, string xmlAttribute, string xmlValue)
        {

            try
            {
                XDocument xmlDoc = XDocument.Load(FileName);
                var results = from c in xmlDoc.Descendants(xmlElement)
                              select c;
                string s = "";
                foreach (var result in results)
                {
                    result.Attribute(xmlAttribute).Value = xmlValue;
                }

                xmlDoc.Save(FileName);
                return  true;
            }
            catch (System.Exception)
            {
                return false;
            }
        }

        /// <summary>  
        /// 加载XMl文件到临时变量 
        /// </summary>  
        /// <param name="FileName"></param>
        /// <returns></returns>
        public static Boolean LoadXmlFileToVar(String FileName)
        {

            try
            {
                xmlDocSave = XDocument.Load(FileName);
               
                return true;
            }
            catch (System.Exception)
            {
                return false;
            }
        }

        /// <summary>
        /// 设置XMl临时变量指定元素的指定属性的值  
        /// </summary>
        /// <param name="FileName"></param>
        /// <param name="xmlElement">指定元素</param>
        /// <param name="xmlAttribute">指定属性</param>
        /// <param name="xmlValue">指定值</param>
        /// <returns></returns>
        public static Boolean setXmlValueToVar(String FileName, string xmlElement, string xmlAttribute, string xmlValue)
        {

            try
            {
                var results = from c in xmlDocSave.Descendants(xmlElement)
                              select c;
                string s = "";
                foreach (var result in results)
                {
                    result.Attribute(xmlAttribute).Value = xmlValue;
                }

                return true;
            }
            catch (System.Exception)
            {
                return false;
            }
        }


        /// <summary>
        /// 设置XMl临时变量指定元素的指定属性的值  
        /// </summary>
        /// <param name="FileName"></param>
        /// <param name="xmlElement">指定元素</param>
        /// <param name="xmlAttribute">指定属性</param>
        /// <param name="xmlValue">指定值</param>
        /// <returns></returns>
        public static Boolean setXmlValueToVar(string xmlElement, string xmlAttribute, string xmlValue)
        {

            try
            {
                var results = from c in xmlDocSave.Descendants(xmlElement)
                              select c;
                string s = "";
                foreach (var result in results)
                {
                    result.Attribute(xmlAttribute).Value = xmlValue;
                }

                return true;
            }
            catch (System.Exception)
            {
                return false;
            }
        }


        /// <summary>
        /// 保存XML变量到文件  
        /// </summary>
        /// <param name="FileName"></param>
        /// <returns></returns>
        public static Boolean SaveXmlVarToFile(String FileName)
        {

            try
            {
                xmlDocSave.Save(FileName);
                return true;
            }
            catch (System.Exception)
            {
                return false;
            }
        }

        /// <summary>
        /// 新建XML文件
        /// </summary>
        /// <param name="FileName">文件位置</param>
        /// <param name="RootName">根节点</param>
        /// <returns></returns>
        public static string CreateXML(String FileName, String RootName)
        {
            try
            {
                XDocument doc = new XDocument(new XDeclaration("1.0", "utf-8", "yes")
                    , new XElement(RootName));//添加XML文件声明
                doc.Save(FileName);

                return "OK";
            }
            catch (System.Exception ex)
            {
                return ex.Message;
            }

        }

        /// <summary>
        /// 新建XMl文件指定元素的指定属性的值 
        /// </summary>
        /// <param name="FileName"></param>
        /// <param name="XElement">指定元素</param>
        /// <param name="XAttribute">指定属性</param>
        /// <param name="xmlValue">指定值</param>
        /// <returns></returns>
        public static Boolean CreateXMLXElement(String FileName, String XElement, String XAttribute, String xmlValue)
        {
            try
            {
                XDocument xmlDoc = XDocument.Load(FileName);

                XElement Root = xmlDoc.Root;

                XElement xEleName = new XElement(XElement);
                Root.Add(xEleName);

                XAttribute xAttr = new XAttribute(XAttribute, xmlValue);
                xEleName.Add(xAttr);

                xmlDoc.Save(FileName);
                return true;
            }
            catch (System.Exception)
            {
                return false;
            }

        }

        /// <summary>
        /// 新建XMl文件指定元素的指定属性的值 
        /// </summary>
        /// <param name="FileName"></param>
        /// <param name="RootName">指定节点</param>
        /// <param name="XElement">指定元素</param>
        /// <param name="XAttribute">指定属性</param>
        /// <param name="xmlValue">指定值</param>
        /// <returns></returns>
        public static Boolean CreateXMLXElement(String FileName, String RootName, String XElement, String XAttribute, String xmlValue)
        {
            try
            {
                XDocument xmlDoc = XDocument.Load(FileName);

                XElement Root = xmlDoc.Root;

                var nodes = Root.Descendants().FirstOrDefault(a => a.Name.LocalName == RootName);

                if (nodes==null)
                {
                    XElement Root1 = new XElement(RootName);
                    // 添加节点使用Add
                    Root.Add(Root1);
                    XElement xEleName = new XElement(XElement);
                    Root1.Add(xEleName);

                    XAttribute xAttr = new XAttribute(XAttribute, xmlValue);
                    xEleName.Add(xAttr);
                }
               else
                {
                    XElement xEleName = new XElement(XElement);
                    nodes.Add(xEleName);

                    XAttribute xAttr = new XAttribute(XAttribute, xmlValue);
                    xEleName.Add(xAttr);
                }

              

                xmlDoc.Save(FileName);
                return true;
            }
            catch (System.Exception)
            {
                return false;
            }

        }
    }


    public class XmlMultiStream
    {
        private  XDocument xmlDocSave;

        /// <summary>  
        /// 返回XMl文件指定元素的指定属性值  
        /// </summary>  
        /// <param name="FileName"></param>
        /// <param name="xmlElement">指定元素</param>  
        /// <param name="xmlAttribute">指定属性</param>  
        /// <returns></returns>  
        public  string getXmlValue(String FileName, string xmlElement, string xmlAttribute)
        {
            try
            {
                XDocument xmlDoc = XDocument.Load(FileName);
                var results = from c in xmlDoc.Descendants(xmlElement)
                              select c;
                string s = "";
                foreach (var result in results)
                {
                    s = result.Attribute(xmlAttribute).Value.ToString();
                }
                return s;
            }
            catch (System.Exception ex)
            {
                return ex.Message;
            }

        }

        /// <summary>  
        /// 返回XMl文件指定元素的指定属性值  
        /// </summary>  
        /// <param name="FileName"></param>
        /// <param name="xmlElement">指定元素</param>  
        /// <param name="xmlAttribute">指定属性</param>  
        /// <returns></returns>  
        public  string getXmlValueFromVar(String FileName, string xmlElement, string xmlAttribute)
        {
            try
            {
                XDocument xmlDoc = xmlDocSave;
                var results = from c in xmlDoc.Descendants(xmlElement)
                              select c;
                string s = "";
                foreach (var result in results)
                {
                    s = result.Attribute(xmlAttribute).Value.ToString();
                }
                return s;
            }
            catch (System.Exception ex)
            {
                return ex.Message;
            }

        }

        /// <summary>  
        /// 返回XMl文件指定元素的指定属性值  
        /// </summary>  
        /// <param name="FileName"></param>
        /// <param name="xmlElement">指定元素</param>  
        /// <param name="xmlAttribute">指定属性</param>  
        /// <returns></returns>  
        public  string getXmlValueFromVar(string xmlElement, string xmlAttribute)
        {
            try
            {
                XDocument xmlDoc = xmlDocSave;
                var results = from c in xmlDoc.Descendants(xmlElement)
                              select c;
                string s = "";
                foreach (var result in results)
                {
                    s = result.Attribute(xmlAttribute).Value.ToString();
                }
                return s;
            }
            catch (System.Exception ex)
            {
                return ex.Message;
            }

        }

        /// <summary>  
        /// 返回XMl文件指定元素的所有属性值  
        /// </summary>  
        /// <param name="FileName"></param>
        /// <param name="xmlElement">指定元素</param>  
        /// <param name="xmlAttribute">指定属性</param>  
        /// <returns></returns>  
        public  List<Hashtable> getAllXmlValue(String FileName, string xmlAttribute)
        {
            List<Hashtable> s = new List<Hashtable>();
            try
            {
                XDocument xmlDoc = XDocument.Load(FileName);
                var results = from c in xmlDoc.Descendants()
                              select c;

                foreach (var result in results)
                {
                    Hashtable ht = new Hashtable();
                    ht.Add("Name", result.Name.ToString());

                    string Parent = result.Parent != null ? result.Parent.Name.ToString() : "";
                    ht.Add("Parent", Parent);
                    if (result.Attribute(xmlAttribute) == null)
                        ht.Add("Value", "");
                    else
                        ht.Add("Value", result.Attribute(xmlAttribute).Value.ToString());

                    s.Add(ht);
                }

                return s;
            }
            catch (System.Exception)
            {
                return s;
            }

        }

        /// <summary>  
        /// 设置XMl文件指定元素的指定属性的值  
        /// </summary>  
        /// <param name="FileName"></param>
        /// <param name="xmlElement">指定元素</param>  
        /// <param name="xmlAttribute">指定属性</param>  
        /// <param name="xmlValue">指定值</param>  
        public  Boolean setXmlValue(String FileName, string xmlElement, string xmlAttribute, string xmlValue)
        {

            try
            {
                XDocument xmlDoc = XDocument.Load(FileName);
                var results = from c in xmlDoc.Descendants(xmlElement)
                              select c;
                string s = "";
                foreach (var result in results)
                {
                    result.Attribute(xmlAttribute).Value = xmlValue;
                }

                xmlDoc.Save(FileName);
                return true;
            }
            catch (System.Exception)
            {
                return false;
            }
        }

        /// <summary>  
        /// 加载XMl文件到临时变量 
        /// </summary>  
        /// <param name="FileName"></param>
        /// <returns></returns>
        public  Boolean LoadXmlFileToVar(String FileName)
        {

            try
            {
                xmlDocSave = XDocument.Load(FileName);

                return true;
            }
            catch (System.Exception)
            {
                return false;
            }
        }

        /// <summary>
        /// 设置XMl临时变量指定元素的指定属性的值  
        /// </summary>
        /// <param name="FileName"></param>
        /// <param name="xmlElement">指定元素</param>
        /// <param name="xmlAttribute">指定属性</param>
        /// <param name="xmlValue">指定值</param>
        /// <returns></returns>
        public Boolean setXmlValueToVar(String FileName, string xmlElement, string xmlAttribute, string xmlValue)
        {

            try
            {
                var results = from c in xmlDocSave.Descendants(xmlElement)
                              select c;
                string s = "";
                foreach (var result in results)
                {
                    result.Attribute(xmlAttribute).Value = xmlValue;
                }

                return true;
            }
            catch (System.Exception)
            {
                return false;
            }
        }


        /// <summary>
        /// 设置XMl临时变量指定元素的指定属性的值  
        /// </summary>
        /// <param name="FileName"></param>
        /// <param name="xmlElement">指定元素</param>
        /// <param name="xmlAttribute">指定属性</param>
        /// <param name="xmlValue">指定值</param>
        /// <returns></returns>
        public  Boolean setXmlValueToVar(string xmlElement, string xmlAttribute, string xmlValue)
        {

            try
            {
                var results = from c in xmlDocSave.Descendants(xmlElement)
                              select c;
                string s = "";
                foreach (var result in results)
                {
                    result.Attribute(xmlAttribute).Value = xmlValue;
                }

                return true;
            }
            catch (System.Exception)
            {
                return false;
            }
        }


        /// <summary>
        /// 保存XML变量到文件  
        /// </summary>
        /// <param name="FileName"></param>
        /// <returns></returns>
        public  Boolean SaveXmlVarToFile(String FileName)
        {

            try
            {
                xmlDocSave.Save(FileName);
                return true;
            }
            catch (System.Exception)
            {
                return false;
            }
        }

        /// <summary>
        /// 新建XML文件
        /// </summary>
        /// <param name="FileName">文件位置</param>
        /// <param name="RootName">根节点</param>
        /// <returns></returns>
        public  string CreateXML(String FileName, String RootName)
        {
            try
            {
                XDocument doc = new XDocument(new XDeclaration("1.0", "utf-8", "yes")
                    , new XElement(RootName));//添加XML文件声明
                doc.Save(FileName);

                return "OK";
            }
            catch (System.Exception ex)
            {
                return ex.Message;
            }

        }

        /// <summary>
        /// 新建XMl文件指定元素的指定属性的值 
        /// </summary>
        /// <param name="FileName"></param>
        /// <param name="XElement">指定元素</param>
        /// <param name="XAttribute">指定属性</param>
        /// <param name="xmlValue">指定值</param>
        /// <returns></returns>
        public  Boolean CreateXMLXElement(String FileName, String XElement, String XAttribute, String xmlValue)
        {
            try
            {
                XDocument xmlDoc = XDocument.Load(FileName);

                XElement Root = xmlDoc.Root;

                XElement xEleName = new XElement(XElement);
                Root.Add(xEleName);

                XAttribute xAttr = new XAttribute(XAttribute, xmlValue);
                xEleName.Add(xAttr);

                xmlDoc.Save(FileName);
                return true;
            }
            catch (System.Exception)
            {
                return false;
            }

        }

        /// <summary>
        /// 新建XMl文件指定元素的指定属性的值 
        /// </summary>
        /// <param name="FileName"></param>
        /// <param name="RootName">指定节点</param>
        /// <param name="XElement">指定元素</param>
        /// <param name="XAttribute">指定属性</param>
        /// <param name="xmlValue">指定值</param>
        /// <returns></returns>
        public  Boolean CreateXMLXElement(String FileName, String RootName, String XElement, String XAttribute, String xmlValue)
        {
            try
            {
                XDocument xmlDoc = XDocument.Load(FileName);

                XElement Root = xmlDoc.Root;

                var nodes = Root.Descendants().FirstOrDefault(a => a.Name.LocalName == RootName);

                if (nodes == null)
                {
                    XElement Root1 = new XElement(RootName);
                    // 添加节点使用Add
                    Root.Add(Root1);
                    XElement xEleName = new XElement(XElement);
                    Root1.Add(xEleName);

                    XAttribute xAttr = new XAttribute(XAttribute, xmlValue);
                    xEleName.Add(xAttr);
                }
                else
                {
                    XElement xEleName = new XElement(XElement);
                    nodes.Add(xEleName);

                    XAttribute xAttr = new XAttribute(XAttribute, xmlValue);
                    xEleName.Add(xAttr);
                }



                xmlDoc.Save(FileName);
                return true;
            }
            catch (System.Exception)
            {
                return false;
            }

        }
    }
}
