using System;
using System.Text;
using System.Security.Cryptography;
using System.IO;


namespace DatabaseMaster2
{
    public static class StringEncrypt
    {
        private static String Skey= GetEncryptKey();

        public enum EncryptType
        {
            None=-1,
            ASCII = 0,
            UTF8 = 1,
            MD5 = 2,
            DES = 3,
            RC2 = 4,
            AES = 5
        };

        /// <summary>
        /// 获取字符串的MD5码
        /// </summary>
        /// <param name="input"></param>
        /// <returns></returns>
        private static string GetMD5Hash(string input)
        {
            // Use input string to calculate MD5 hash
            MD5 md5 = System.Security.Cryptography.MD5.Create();
            byte[] inputBytes = System.Text.Encoding.ASCII.GetBytes(input);
            byte[] hashBytes = md5.ComputeHash(inputBytes);

            // Convert the byte array to hexadecimal string
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < hashBytes.Length; i++)
            {
                sb.Append(hashBytes[i].ToString("X2"));
                // To force the hex string to lower-case letters instead of
                // upper-case, use he following line instead:
                // sb.Append(hashBytes[i].ToString("x2")); 
            }
            return sb.ToString();
        }

        /// <summary>
        /// 计算解密密匙
        /// </summary>
        /// <returns></returns>
        private static String GetEncryptKey()
        {
            try
            {
                if (File.Exists(AppDomain.CurrentDomain.BaseDirectory + "DatabaseCon.xml"))
                {
                    string s = XmlStream.getXmlValue(AppDomain.CurrentDomain.BaseDirectory + "DatabaseCon.xml", "String", "Value");

                    s = AESDecrypt(s, "Database");

                    return GetMD5Hash(s).Substring(0, 8);
                }
                else
                {
                    return "RBACSkey";
                }
            }
            catch (Exception)
            {
                return "RBACSkey";
            }
           

        }

        /// <summary>
        /// 设置解密密匙
        /// </summary>
        /// <returns></returns>
        public static void SetEncryptKey(String KeyString)
        {
            Skey = KeyString;
        }

        /// <summary>
        /// 设置解密密匙
        /// </summary>
        /// <returns></returns>
        public static void SetEncryptKey()
        {
            Skey = GetEncryptKey();
        }


        /// <summary>
        /// 加密通用函数
        /// </summary>
        /// <param name="EncryptType">加密方法</param>
        /// <param name="EncryptText">待加密的密文</param>
        /// <param name="EncryptKey">密匙</param>
        /// <returns></returns>
        public static String DataEncrypt(EncryptType Type, String EncryptText)
        {
            if (String.IsNullOrEmpty(EncryptText))
                return "Paramters no value";

            switch (Type)
            {
                case EncryptType.ASCII:
                    return ASCIIEncrypt(EncryptText, Skey);
                case EncryptType.UTF8:
                    return UTF8Encrypt(EncryptText, Skey);
                case EncryptType.MD5:
                    return MD5Encrypt(EncryptText, Skey);
                case EncryptType.DES:
                    return DESEncrypt(EncryptText, Skey);
                case EncryptType.RC2:
                    return RC2Encrypt(EncryptText, Skey);
                case EncryptType.AES:
                    return AESEncrypt(EncryptText, Skey);
                default:
                    return "Not support this Encrypt type.";
            }
        }

        /// <summary>
        /// 加密通用函数
        /// </summary>
        /// <param name="EncryptType">加密方法</param>
        /// <param name="EncryptText">待加密的密文</param>
        /// <param name="EncryptKey1">密匙1</param>
        /// <param name="EncryptKey2">密匙2</param>
        /// <param name="EncryptKey3">密匙3</param>
        /// <returns></returns>
        public static String DataEncrypt(String EncryptType,String EncryptText,String EncryptKey1,String EncryptKey2,String EncryptKey3)
        {
            if (String.IsNullOrEmpty(EncryptText) || String.IsNullOrEmpty(EncryptKey1) || String.IsNullOrEmpty(EncryptKey2) || String.IsNullOrEmpty(EncryptKey3))
                return "Paramters no value";

            return DES3Encrypt(EncryptText,EncryptKey1,EncryptKey2,EncryptKey3);
        }

        /// <summary>
        /// 解密通用函数
        /// </summary>
        /// <param name="DecryptType">解密方法</param>
        /// <param name="DecryptText">待解密的密文</param>
        /// <param name="DecryptKey">密匙</param>
        /// <returns></returns>
        public static String DataDecrypt(EncryptType Type, String DecryptText)
        {
            if (String.IsNullOrEmpty(DecryptText))
                return "Paramters no value";

            switch (Type)
            {
                case EncryptType.ASCII:
                    return ASCIIDecrypt(DecryptText, Skey);
                case EncryptType.UTF8:
                    return UTF8Decrypt(DecryptText, Skey);
                case EncryptType.MD5:
                    return MD5Decrypt(DecryptText, Skey);
                case EncryptType.DES:
                    return DESDecrypt(DecryptText, Skey);
                case EncryptType.RC2:
                    return RC2Decrypt(DecryptText, Skey);
                case EncryptType.AES:
                    return AESDecrypt(DecryptText, Skey);
                default:
                    return "Not support this Decrypt type.";
            }
        }

        /// <summary>
        /// 加密通用函数
        /// </summary>
        /// <param name="EncryptType">加密方法</param>
        /// <param name="EncryptText">待加密的密文</param>
        /// <param name="EncryptKey1">密匙1</param>
        /// <param name="EncryptKey2">密匙2</param>
        /// <param name="EncryptKey3">密匙3</param>
        /// <returns></returns>
        public static String DataDecrypt(String DecryptType, String DecryptText, String DecryptKey1, String DecryptKey2, String DecryptKey3)
        {
            if (String.IsNullOrEmpty(DecryptText) || String.IsNullOrEmpty(DecryptKey1) || String.IsNullOrEmpty(DecryptKey2) || String.IsNullOrEmpty(DecryptKey2))
                return "Paramters no value";

            return DES3Encrypt(DecryptText, DecryptKey1, DecryptKey2, DecryptKey3);
        }


        /// <summary>
        /// ASCII加密
        /// </summary>
        /// <param name="encryptString">待加密的密文</param>
        /// <param name="encryptKey">密匙</param>
        /// <returns></returns>
        private static string ASCIIEncrypt(string strText, string key)
        {
            try
            {
                byte[] buffer = new MD5CryptoServiceProvider().ComputeHash(Encoding.ASCII.GetBytes(key));
                TripleDESCryptoServiceProvider provider = new TripleDESCryptoServiceProvider();
                provider.Key = buffer;
                provider.Mode = CipherMode.ECB;
                byte[] bytes = Encoding.ASCII.GetBytes(strText);
                string str = Convert.ToBase64String(provider.CreateEncryptor().TransformFinalBlock(bytes, 0, bytes.Length));
                provider = null;
                return str;
            }
            catch (System.Exception ex)
            {
                throw ex;
            }

        }

        /// <summary>
        /// ASCII加密
        /// </summary>
        /// <param name="encryptString">待加密的密文</param>
        /// <param name="encryptKey">密匙</param>
        /// <returns></returns>
        private static string UTF8Encrypt(string strText, string key)
        {
            try
            {
                byte[] buffer = new MD5CryptoServiceProvider().ComputeHash(Encoding.UTF8.GetBytes(key));
                TripleDESCryptoServiceProvider provider = new TripleDESCryptoServiceProvider();
                provider.Key = buffer;
                provider.Mode = CipherMode.ECB;
                byte[] bytes = Encoding.UTF8.GetBytes(strText);
                string str = Convert.ToBase64String(provider.CreateEncryptor().TransformFinalBlock(bytes, 0, bytes.Length));
                provider = null;
                return str;
            }
            catch (System.Exception ex)
            {
                throw ex;
            }

        }

        /// <summary>
        /// ASCII解密
        /// </summary>
        /// <param name="encryptString">待解密的密文</param>
        /// <param name="encryptKey">密匙</param>
        /// <returns></returns>
        private static string ASCIIDecrypt(string strText, string key)
        {
            try
            {
                byte[] buffer = new MD5CryptoServiceProvider().ComputeHash(Encoding.ASCII.GetBytes(key));
                TripleDESCryptoServiceProvider provider = new TripleDESCryptoServiceProvider();
                provider.Key = buffer;
                provider.Mode = CipherMode.ECB;
                byte[] inputBuffer = Convert.FromBase64String(strText);
                return Encoding.ASCII.GetString(provider.CreateDecryptor().TransformFinalBlock(inputBuffer, 0, inputBuffer.Length));
            }
            catch (System.Exception ex)
            {
                throw ex;
            }

        }

        /// <summary>
        /// ASCII解密
        /// </summary>
        /// <param name="encryptString">待解密的密文</param>
        /// <param name="encryptKey">密匙</param>
        /// <returns></returns>
        private static string UTF8Decrypt(string strText, string key)
        {
            try
            {
                byte[] buffer = new MD5CryptoServiceProvider().ComputeHash(Encoding.UTF8.GetBytes(key));
                TripleDESCryptoServiceProvider provider = new TripleDESCryptoServiceProvider();
                provider.Key = buffer;
                provider.Mode = CipherMode.ECB;
                byte[] inputBuffer = Convert.FromBase64String(strText);
                return Encoding.UTF8.GetString(provider.CreateDecryptor().TransformFinalBlock(inputBuffer, 0, inputBuffer.Length));
            }
            catch (System.Exception ex)
            {
                throw ex;
            }

        }



        
        /// <summary>
        /// MD5加密
        /// </summary>
        /// <param name="pToEncrypt">待加密的密文</param>
        /// <param name="sKey">密匙</param>
        /// <returns></returns>
        private static string MD5Encrypt(string pToEncrypt, string sKey)
        {
            try
            {
                DESCryptoServiceProvider des = new DESCryptoServiceProvider();
                byte[] inputByteArray = Encoding.Default.GetBytes(pToEncrypt);
                des.Key = ASCIIEncoding.ASCII.GetBytes(sKey);
                des.IV = ASCIIEncoding.ASCII.GetBytes(sKey);
                MemoryStream ms = new MemoryStream();
                CryptoStream cs = new CryptoStream(ms, des.CreateEncryptor(), CryptoStreamMode.Write);
                cs.Write(inputByteArray, 0, inputByteArray.Length);
                cs.FlushFinalBlock();
                StringBuilder ret = new StringBuilder();
                foreach (byte b in ms.ToArray())
                {
                    ret.AppendFormat("{0:X2}", b);
                }
                ret.ToString();
                return ret.ToString();
            }
            catch (System.Exception ex)
            {
                throw ex;
            }

        }

        /// <summary>
        /// MD5解密
        /// </summary>
        /// <param name="pToEncrypt">待解密的密文</param>
        /// <param name="sKey">密匙</param>
        /// <returns></returns>
        private static string MD5Decrypt(string pToDecrypt, string sKey)
        {
            try
            {
                DESCryptoServiceProvider des = new DESCryptoServiceProvider();

                byte[] inputByteArray = new byte[pToDecrypt.Length / 2];
                for (int x = 0; x < pToDecrypt.Length / 2; x++)
                {
                    int i = (Convert.ToInt32(pToDecrypt.Substring(x * 2, 2), 16));
                    inputByteArray[x] = (byte)i;
                }

                des.Key = ASCIIEncoding.ASCII.GetBytes(sKey);
                des.IV = ASCIIEncoding.ASCII.GetBytes(sKey);
                MemoryStream ms = new MemoryStream();
                CryptoStream cs = new CryptoStream(ms, des.CreateDecryptor(), CryptoStreamMode.Write);
                cs.Write(inputByteArray, 0, inputByteArray.Length);
                cs.FlushFinalBlock();

                StringBuilder ret = new StringBuilder();

                return System.Text.Encoding.Default.GetString(ms.ToArray());
            }
            catch (System.Exception ex)
            {
                throw ex;
            }

        }


        /// <summary>
        /// DES加密
        /// </summary>
        /// <param name="encryptString">待加密的密文</param>
        /// <param name="encryptKey">密匙（8位）</param>
        /// <returns></returns>
        private static string DESEncrypt(string encryptString, string encryptKey)
        {
            string returnValue;

            if (encryptKey.Length != 8)
                throw new Exception("Encrypt key need greater than 8 bit");

            try
            {
                byte[] temp = { 0x12, 0x34, 0x56, 0x78, 0x90, 0xAB, 0xCD, 0xEF };
                DESCryptoServiceProvider dES = new DESCryptoServiceProvider();
                byte[] byteEncrypt = Encoding.Default.GetBytes(encryptString);
                MemoryStream memoryStream = new MemoryStream();
                CryptoStream cryptoStream = new CryptoStream(memoryStream, dES.CreateEncryptor(Encoding.Default.GetBytes(encryptKey), temp), CryptoStreamMode.Write);
                cryptoStream.Write(byteEncrypt, 0, byteEncrypt.Length);
                cryptoStream.FlushFinalBlock();
                returnValue = Convert.ToBase64String(memoryStream.ToArray());

            }
            catch (Exception ex)
            {
                throw ex;
            }
            return returnValue;
        }

        /// <summary>
        /// DES解密
        /// </summary>
        /// <param name="decryptString">密文</param>
        /// <param name="decryptKey">密匙（8位）</param>
        /// <returns></returns>
        private static string DESDecrypt(string decryptString, string decryptKey)
        {
            string returnValue;

            if (decryptKey.Length != 8)
                throw new Exception("Encrypt key need greater than 8 bit");

            try
            {
                byte[] temp = { 0x12, 0x34, 0x56, 0x78, 0x90, 0xAB, 0xCD, 0xEF };
                DESCryptoServiceProvider dES = new DESCryptoServiceProvider();
                byte[] byteDecryptString = Convert.FromBase64String(decryptString);
                MemoryStream memoryStream = new MemoryStream();
                CryptoStream cryptoStream = new CryptoStream(memoryStream, dES.CreateDecryptor(Encoding.Default.GetBytes(decryptKey), temp), CryptoStreamMode.Write);

                cryptoStream.Write(byteDecryptString, 0, byteDecryptString.Length);

                cryptoStream.FlushFinalBlock();

                returnValue = Encoding.Default.GetString(memoryStream.ToArray());

            }
            catch (Exception ex)
            {
                throw ex;
            }
            return returnValue;

        }

        /// <summary>
        /// RC2加密
        /// </summary>
        /// <param name="encryptString">待加密的密文</param>
        /// <param name="encryptKey">密匙(必须为5-16位)</param>
        /// <returns></returns>
        private static string RC2Encrypt(string encryptString, string encryptKey)
        {
            string returnValue;

            if (encryptKey.Length < 5 && encryptKey.Length > 16)
                throw new Exception("Encrypt key need between 5-16");

            try
            {
                byte[] temp = { 0x12, 0x34, 0x56, 0x78, 0x90, 0xAB, 0xCD, 0xEF };
                RC2CryptoServiceProvider rC2 = new RC2CryptoServiceProvider();
                byte[] byteEncryptString = Encoding.Default.GetBytes(encryptString);
                MemoryStream memorystream = new MemoryStream();
                CryptoStream cryptoStream = new CryptoStream(memorystream, rC2.CreateEncryptor(Encoding.Default.GetBytes(encryptKey), temp), CryptoStreamMode.Write);
                cryptoStream.Write(byteEncryptString, 0, byteEncryptString.Length);
                cryptoStream.FlushFinalBlock();
                returnValue = Convert.ToBase64String(memorystream.ToArray());

            }
            catch (Exception ex)
            {
                throw ex;
            }
            return returnValue;

        }
        /// <summary>
        /// RC2解密
        /// </summary>
        /// <param name="decryptString">密文</param>
        /// <param name="decryptKey">密匙(必须为5-16位)</param>
        /// <returns></returns>
        private static string RC2Decrypt(string decryptString, string decryptKey)
        {
            string returnValue;

            if (decryptKey.Length < 5 && decryptKey.Length > 16)
                throw new Exception("Encrypt key need between 5-16");

            try
            {
                byte[] temp = { 0x12, 0x34, 0x56, 0x78, 0x90, 0xAB, 0xCD, 0xEF };
                RC2CryptoServiceProvider rC2 = new RC2CryptoServiceProvider();
                byte[] byteDecrytString = Convert.FromBase64String(decryptString);
                MemoryStream memoryStream = new MemoryStream();
                CryptoStream cryptoStream = new CryptoStream(memoryStream, rC2.CreateDecryptor(Encoding.Default.GetBytes(decryptKey), temp), CryptoStreamMode.Write);
                cryptoStream.Write(byteDecrytString, 0, byteDecrytString.Length);
                cryptoStream.FlushFinalBlock();
                returnValue = Encoding.Default.GetString(memoryStream.ToArray());
            }
            catch (Exception ex)
            {
                throw ex;
            }
            return returnValue;
        }
        /// <summary>
        /// 3DES 加密
        /// </summary>
        /// <param name="encryptString">待加密密文</param>
        /// <param name="encryptKey1">密匙1(长度必须为8位)</param>
        /// <param name="encryptKey2">密匙2(长度必须为8位)</param>
        /// <param name="encryptKey3">密匙3(长度必须为8位)</param>
        /// <returns></returns>
        private static string DES3Encrypt(string encryptString, string encryptKey1, string encryptKey2, string encryptKey3)
        {

            string returnValue;

            if (encryptKey1.Length != 8 || encryptKey2.Length != 8 || encryptKey3.Length != 8)
                throw new Exception("Encrypt key need between 5-16");

            try
            {
                returnValue = DESEncrypt(encryptString, encryptKey3);
                returnValue = DESEncrypt(returnValue, encryptKey2);
                returnValue = DESEncrypt(returnValue, encryptKey1);

            }
            catch (Exception ex)
            {
                throw ex;
            }
            return returnValue;

        }
        /// <summary>
        /// 3DES 解密
        /// </summary>
        /// <param name="decryptString">待解密密文</param>
        /// <param name="decryptKey1">密匙1(长度必须为8位)</param>
        /// <param name="decryptKey2">密匙2(长度必须为8位)</param>
        /// <param name="decryptKey3">密匙3(长度必须为8位)</param>
        /// <returns></returns>
        private static string DES3Decrypt(string decryptString, string decryptKey1, string decryptKey2, string decryptKey3)
        {

            string returnValue;

            if (decryptKey1.Length != 8 || decryptKey2.Length != 8 || decryptKey3.Length != 8)
                throw new Exception("Encrypt key need between 5-16");

            try
            {
                returnValue = DESDecrypt(decryptString, decryptKey1);
                returnValue = DESDecrypt(returnValue, decryptKey2);
                returnValue = DESDecrypt(returnValue, decryptKey3);

            }
            catch (Exception ex)
            {
                throw ex;
            }
            return returnValue;
        }
        /// <summary>
        /// AES加密
        /// </summary>
        /// <param name="encryptString">待加密的密文</param>
        /// <param name="encryptKey">加密密匙</param>
        /// <returns></returns>
        private static string AESEncrypt(string encryptString, string encryptKey)
        {
            string returnValue;
            byte[] temp = Convert.FromBase64String("Rkb4jvUy/ye7Cd7k89QQgQ==");
            Rijndael AESProvider = Rijndael.Create();
            try
            {
                byte[] byteEncryptString = Encoding.Default.GetBytes(encryptString);
                MemoryStream memoryStream = new MemoryStream();
                CryptoStream cryptoStream = new CryptoStream(memoryStream, AESProvider.CreateEncryptor(Encoding.Default.GetBytes(encryptKey), temp), CryptoStreamMode.Write);
                cryptoStream.Write(byteEncryptString, 0, byteEncryptString.Length);
                cryptoStream.FlushFinalBlock();
                returnValue = Convert.ToBase64String(memoryStream.ToArray());
            }

            catch (Exception ex)
            {
                throw ex;
            }

            return returnValue;

        }
        /// <summary>
        ///AES 解密
        /// </summary>
        /// <param name="decryptString">待解密密文</param>
        /// <param name="decryptKey">解密密钥</param>
        /// <returns></returns>
        private static string AESDecrypt(string decryptString, string decryptKey)
        {
            string returnValue = "";
            byte[] temp = Convert.FromBase64String("Rkb4jvUy/ye7Cd7k89QQgQ==");
            Rijndael AESProvider = Rijndael.Create();
            try
            {
                byte[] byteDecryptString = Convert.FromBase64String(decryptString);
                MemoryStream memoryStream = new MemoryStream();
                CryptoStream cryptoStream = new CryptoStream(memoryStream, AESProvider.CreateDecryptor(Encoding.Default.GetBytes(decryptKey), temp), CryptoStreamMode.Write);
                cryptoStream.Write(byteDecryptString, 0, byteDecryptString.Length);
                cryptoStream.FlushFinalBlock();
                returnValue = Encoding.Default.GetString(memoryStream.ToArray());
            }
            catch (Exception ex)
            {
                throw ex;
            }
            return returnValue;
        }

        /// <summary>          
        /// 对文件内容进行DES加密          
        /// </summary>          
        /// <param name="sourceFile">待加密的文件绝对路径</param>          
        /// <param name="destFile">加密后的文件保存的绝对路径</param>          
        public static void EncryptFile(string sourceFile, string destFile)
        {
            if (!File.Exists(sourceFile)) throw new FileNotFoundException("指定的文件路径不存在！", sourceFile);
            byte[] btKey = Encoding.Default.GetBytes(Skey);
            byte[] btIV = { 0x12, 0x34, 0x56, 0x78, 0x90, 0xAB, 0xCD, 0xEF };
            DESCryptoServiceProvider des = new DESCryptoServiceProvider();
            byte[] btFile = File.ReadAllBytes(sourceFile);
            using (FileStream fs = new FileStream(destFile, FileMode.Create, FileAccess.Write))
            {
                try
                {
                    using (CryptoStream cs = new CryptoStream(fs, des.CreateEncryptor(btKey, btIV), CryptoStreamMode.Write))
                    {
                        cs.Write(btFile, 0, btFile.Length);
                        cs.FlushFinalBlock();
                    }
                }
                catch
                {
                    throw;
                }
                finally
                {
                    fs.Close();
                }
            }
        }
        /// <summary>          
        /// 对文件内容进行DES加密，加密后覆盖掉原来的文件          
        /// </summary>          
        /// <param name="sourceFile">待加密的文件的绝对路径</param>          
        public static void EncryptFile(string sourceFile)
        {
            EncryptFile(sourceFile, sourceFile);
        }
        /// <summary>          
        /// 对文件内容进行DES解密          
        /// </summary>          
        /// <param name="sourceFile">待解密的文件绝对路径</param>          
        /// <param name="destFile">解密后的文件保存的绝对路径</param>          
        public static void DecryptFile(string sourceFile, string destFile)
        {
            if (!File.Exists(sourceFile)) throw new FileNotFoundException("指定的文件路径不存在！", sourceFile);
            byte[] btKey = Encoding.Default.GetBytes(Skey);
            byte[] btIV = { 0x12, 0x34, 0x56, 0x78, 0x90, 0xAB, 0xCD, 0xEF };
            DESCryptoServiceProvider des = new DESCryptoServiceProvider();
            byte[] btFile = File.ReadAllBytes(sourceFile);
            using (FileStream fs = new FileStream(destFile, FileMode.Create, FileAccess.Write))
            {
                try
                {
                    using (CryptoStream cs = new CryptoStream(fs, des.CreateDecryptor(btKey, btIV), CryptoStreamMode.Write))
                    {
                        cs.Write(btFile, 0, btFile.Length);
                        cs.FlushFinalBlock();
                    }
                }
                catch
                {
                    throw;
                }
                finally
                {
                    fs.Close();
                }
            }
        }
        /// <summary>          
        /// 对文件内容进行DES解密，加密后覆盖掉原来的文件          
        /// </summary>          
        /// <param name="sourceFile">待解密的文件的绝对路径</param>          
        public static void DecryptFile(string sourceFile)
        {
            DecryptFile(sourceFile, sourceFile);
        }      

    }
}
