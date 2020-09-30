using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data;
using System.IO;
using System.Collections;
using System.Data.OleDb;
using DatabaseMaster;

namespace DatabaseLayer
{
    public class OleDBExcel
    {
        /// <summary>
        /// 读取EXCEL数据
        /// </summary>
        /// <param name="FileName"></param>
        /// <param name="SqlCommand"></param>
        /// <param name="dt"></param>
        /// <returns></returns>
        public static Int16 GetExcelData(Boolean BeforeExcel2007, String FileName, String SqlCommand, DataTable dt)
        {
            OleDbConnection odbcconn = new OleDbConnection();
            //查询EXCEL
            try
            {
                String connectstring;

                if (BeforeExcel2007 == true)
                {
                    connectstring = "Provider=Microsoft.Jet.OLEDB.4.0;Data Source=" + FileName + ";Extended Properties=\"Excel 8.0;HDR=YES;IMEX=0\"";
                }
                else
                {
                    connectstring = "Provider=Microsoft.ACE.OLEDB.12.0;Data Source=" + FileName + ";Extended Properties=\"Excel 12.0;HDR=YES;IMEX=0\"";
                }

                odbcconn = new OleDbConnection(connectstring);
                OleDbDataAdapter da = new OleDbDataAdapter(SqlCommand, odbcconn);
                odbcconn.Open();
                da.Fill(dt);
            }
            catch (System.Exception)
            {
                return -2;
            }
            finally
            {
                odbcconn.Close();
            }

            if (dt.Rows.Count == 0)
            {
                return -1;
            }

            return 0;

        }
    }

}