using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DatabaseMaster2
{
    public class DatabaseConfigXml
    {
        /// <summary>
        /// set SetString Encrypt default key
        /// 设置加密秘钥
        /// </summary>
        /// <param name="EncryptKey"></param>
        public static void SetStringEncrypt(String EncryptKey)
        {
            StringEncrypt.SetEncryptKey(EncryptKey);
        }

        /// <summary>
        /// set SetString Encrypt user key
        /// 使用默认秘钥
        /// </summary>
        public static void SetStringEncrypt()
        {
            StringEncrypt.SetEncryptKey();
        }

        /// <summary>
        /// return connstring from formatted of xml file
        /// 返回XML文件指定元素连接字符串
        /// </summary>
        /// <param name="fileName">XMl file path</param>
        /// <param name="xmlElement">XML element name, attribute name must be ConnectString</param>
        /// <returns></returns>
        public static String GetDbConfigXml(String fileName, String xmlElement)
        {
            String s = XmlStream.getXmlValue(fileName, xmlElement, "ConnectString");
            if (string.IsNullOrEmpty(s))
                throw new Exception("XML Attribute not found");
            return s;
        }

        /// <summary>
        ///  return connstring from formatted of xml file
        /// 返回XML文件指定元素属性连接字符串
        /// </summary>
        /// <param name="fileName">XMl file path</param>
        /// <param name="xmlElement">XML element name</param>
        /// <param name="xmlAttribute">XML Attribute name</param>
        /// <returns></returns>
        public static String GetDbConfigXml(String fileName, String xmlElement, String xmlAttribute)
        {
            String s= XmlStream.getXmlValue(fileName, xmlElement, xmlAttribute);
            if (string.IsNullOrEmpty(s))
                throw new Exception("XML Attribute not found");
            return s;
        }

        /// <summary>
        /// return connstring from formatted of xml file
        /// 返回XML文件指定元素加密方式连接字符串
        /// </summary>
        /// <param name="fileName">XMl file path</param>
        /// <param name="xmlElement">XML element name, attribute name must be ConnectString</param>
        /// <param name="encryptType">String Encrypt</param>
        /// <returns></returns>
        public static String GetDbConfigXml(String fileName, String xmlElement, StringEncrypt.EncryptType encryptType)
        {
            String s= XmlStream.getXmlValue(fileName, xmlElement, "ConnectString");
            if (string.IsNullOrEmpty(s))
                throw new Exception("XML Attribute not found");

            s = StringEncrypt.DataDecrypt(encryptType, s);
            return s;
        }

        /// <summary>
        ///  return connstring from formatted of xml file
        /// 返回XML文件指定元素属性加密方式连接字符串
        /// </summary>
        /// <param name="fileName">XMl file path</param>
        /// <param name="xmlElement">XML element name</param>
        /// <param name="xmlAttribute">XML Attribute name</param>
        /// <param name="encryptType">String Encrypt</param>
        /// <returns></returns>
        public static String GetDbConfigXml(String fileName, String xmlElement, String xmlAttribute, StringEncrypt.EncryptType encryptType)
        {
            String s = XmlStream.getXmlValue(fileName, xmlElement, xmlAttribute);
            if (string.IsNullOrEmpty(s))
                throw  new Exception("XML Attribute not found");
 
            s = StringEncrypt.DataDecrypt(encryptType, s);
            return s;
        }
    }

    public class ConnectionConfig
    {
        private String _connString;
        private DbType _dbType;
        private Boolean _isAutoclose=true;
        private Int32 _Timeout=30;
        private String _DatabaseName;

        /// <summary>
        /// set connection string, your can use DatabaseConfigXml class to read from xml file
        /// 设置连接字符串，可以用DatabaseConfigXml类从XML文件读取
        /// </summary>
        public String ConnectionString
        {
            get => _connString;
            set
            {
                if (string.IsNullOrEmpty(value))
                    throw new Exception("String is null");
                if (string.IsNullOrEmpty(value) == false && value.Length > 0)
                    _connString = value;
            }
        }

        /// <summary>
        /// database type
        /// 数据库类型
        /// </summary>
        public DbType DBType
        {
            get => _dbType;
            set => _dbType = value;
        }

        /// <summary>
        /// connect wait time
        /// 连接超时时间
        /// </summary>
        public Int32 WaitTimeout
        {
            get => _Timeout;
            set => _Timeout = value;
        }


        /// <summary>
        /// if value=true Close the connection at the end of each operation ,otherwise connection need call close()
        /// 如果为真 每次操作会自动断开连接，否则需要自行调用连接关闭操作
        /// </summary>
        public Boolean IsAutoCloseConnection
        {
            get => _isAutoclose;
            set => _isAutoclose = value;
        }

        /// <summary>
        /// set database name
        /// 设置数据库名称，NOSQL使用
        /// </summary>
        public String DatabaseName
        {
            get => _DatabaseName;
            set => _DatabaseName = value;
        }
    }
}
