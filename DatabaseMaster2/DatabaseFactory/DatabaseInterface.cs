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
    /// <summary>
    /// Specifies MySQL specific data type of a field, property, for use in a <see cref="T:MySql.Data.MySqlClient.MySqlParameter" />.
    /// </summary>
    public enum MySqlType
    {
        /// <summary>
        /// <see cref="F:MySql.Data.MySqlClient.MySqlDbType.Decimal" />
        /// <para>A fixed precision and scale numeric value between -1038
        /// -1 and 10 38 -1.</para>
        /// </summary>
        Decimal = 0,
        /// <summary>
        /// <see cref="F:MySql.Data.MySqlClient.MySqlDbType.Byte" /><para>The signed range is -128 to 127. The unsigned
        /// range is 0 to 255.</para>
        /// </summary>
        Byte = 1,
        /// <summary>
        /// <see cref="F:MySql.Data.MySqlClient.MySqlDbType.Int16" /><para>A 16-bit signed integer. The signed range is
        /// -32768 to 32767. The unsigned range is 0 to 65535</para>
        /// </summary>
        Int16 = 2,
        /// <summary>
        /// <see cref="F:MySql.Data.MySqlClient.MySqlDbType.Int32" /><para>A 32-bit signed integer</para>
        /// </summary>
        Int32 = 3,
        /// <summary>
        /// <see cref="T:System.Single" /><para>A small (single-precision) floating-point
        /// number. Allowable values are -3.402823466E+38 to -1.175494351E-38,
        /// 0, and 1.175494351E-38 to 3.402823466E+38.</para>
        /// </summary>
        Float = 4,
        /// <summary>
        /// <see cref="F:MySql.Data.MySqlClient.MySqlDbType.Double" /><para>A normal-size (double-precision)
        /// floating-point number. Allowable values are -1.7976931348623157E+308
        /// to -2.2250738585072014E-308, 0, and 2.2250738585072014E-308 to
        /// 1.7976931348623157E+308.</para>
        /// </summary>
        Double = 5,
        /// <summary>
        /// A timestamp. The range is '1970-01-01 00:00:00' to sometime in the
        /// year 2037
        /// </summary>
        Timestamp = 7,
        /// <summary>
        /// <see cref="F:MySql.Data.MySqlClient.MySqlDbType.Int64" /><para>A 64-bit signed integer.</para>
        /// </summary>
        Int64 = 8,
        /// <summary>Specifies a 24 (3 byte) signed or unsigned value.</summary>
        Int24 = 9,
        /// <summary>
        /// Date The supported range is '1000-01-01' to '9999-12-31'.
        /// </summary>
        Date = 10, // 0x0000000A
        /// <summary>
        /// Time <para>The range is '-838:59:59' to '838:59:59'.</para>
        /// </summary>
        Time = 11, // 0x0000000B
        /// <summary>
        /// DateTime The supported range is '1000-01-01 00:00:00' to
        /// '9999-12-31 23:59:59'.
        /// </summary>
        DateTime = 12, // 0x0000000C
        /// <summary>
        /// Datetime The supported range is '1000-01-01 00:00:00' to
        /// '9999-12-31 23:59:59'.
        /// </summary>
        [Obsolete("The Datetime enum value is obsolete.  Please use DateTime.")] Datetime = 12, // 0x0000000C
        /// <summary>
        /// A year in 2- or 4-digit format (default is 4-digit). The
        /// allowable values are 1901 to 2155, 0000 in the 4-digit year
        /// format, and 1970-2069 if you use the 2-digit format (70-69).
        /// </summary>
        Year = 13, // 0x0000000D
        /// <summary>
        /// <b>Obsolete</b>  Use Datetime or Date type
        /// </summary>
        Newdate = 14, // 0x0000000E
        /// <summary>
        /// A variable-length string containing 0 to 65535 characters
        /// </summary>
        VarString = 15, // 0x0000000F
        /// <summary>Bit-field data type</summary>
        Bit = 16, // 0x00000010
        /// <summary>JSON</summary>
        JSON = 245, // 0x000000F5
        /// <summary>New Decimal</summary>
        NewDecimal = 246, // 0x000000F6
        /// <summary>
        /// An enumeration. A string object that can have only one value,
        /// chosen from the list of values 'value1', 'value2', ..., NULL
        /// or the special "" error value. An ENUM can have a maximum of
        /// 65535 distinct values
        /// </summary>
        Enum = 247, // 0x000000F7
        /// <summary>
        /// A set. A string object that can have zero or more values, each
        /// of which must be chosen from the list of values 'value1', 'value2',
        /// ... A SET can have a maximum of 64 members.
        /// </summary>
        Set = 248, // 0x000000F8
        /// <summary>
        /// A binary column with a maximum length of 255 (2^8 - 1)
        /// characters
        /// </summary>
        TinyBlob = 249, // 0x000000F9
        /// <summary>
        /// A binary column with a maximum length of 16777215 (2^24 - 1) bytes.
        /// </summary>
        MediumBlob = 250, // 0x000000FA
        /// <summary>
        /// A binary column with a maximum length of 4294967295 or
        /// 4G (2^32 - 1) bytes.
        /// </summary>
        LongBlob = 251, // 0x000000FB
        /// <summary>
        /// A binary column with a maximum length of 65535 (2^16 - 1) bytes.
        /// </summary>
        Blob = 252, // 0x000000FC
        /// <summary>A variable-length string containing 0 to 255 bytes.</summary>
        VarChar = 253, // 0x000000FD
        /// <summary>A fixed-length string.</summary>
        String = 254, // 0x000000FE
        /// <summary>Geometric (GIS) data type.</summary>
        Geometry = 255, // 0x000000FF
        /// <summary>Unsigned 8-bit value.</summary>
        UByte = 501, // 0x000001F5
        /// <summary>Unsigned 16-bit value.</summary>
        UInt16 = 502, // 0x000001F6
        /// <summary>Unsigned 32-bit value.</summary>
        UInt32 = 503, // 0x000001F7
        /// <summary>Unsigned 64-bit value.</summary>
        UInt64 = 508, // 0x000001FC
        /// <summary>Unsigned 24-bit value.</summary>
        UInt24 = 509, // 0x000001FD
        /// <summary>
        /// A text column with a maximum length of 255 (2^8 - 1) characters.
        /// </summary>
        TinyText = 749, // 0x000002ED
        /// <summary>
        /// A text column with a maximum length of 16777215 (2^24 - 1) characters.
        /// </summary>
        MediumText = 750, // 0x000002EE
        /// <summary>
        /// A text column with a maximum length of 4294967295 or
        /// 4G (2^32 - 1) characters.
        /// </summary>
        LongText = 751, // 0x000002EF
        /// <summary>
        /// A text column with a maximum length of 65535 (2^16 - 1) characters.
        /// </summary>
        Text = 752, // 0x000002F0
        /// <summary>Variable length binary string.</summary>
        VarBinary = 753, // 0x000002F1
        /// <summary>Fixed length binary string.</summary>
        Binary = 754, // 0x000002F2
        /// <summary>A guid column.</summary>
        Guid = 854, // 0x00000356
    }

    public enum OracleType
    {
        BFile = 101, // 0x00000065
        Blob = 102, // 0x00000066
        Byte = 103, // 0x00000067
        Char = 104, // 0x00000068
        Clob = 105, // 0x00000069
        Date = 106, // 0x0000006A
        Decimal = 107, // 0x0000006B
        Double = 108, // 0x0000006C
        Long = 109, // 0x0000006D
        LongRaw = 110, // 0x0000006E
        Int16 = 111, // 0x0000006F
        Int32 = 112, // 0x00000070
        Int64 = 113, // 0x00000071
        IntervalDS = 114, // 0x00000072
        IntervalYM = 115, // 0x00000073
        NClob = 116, // 0x00000074
        NChar = 117, // 0x00000075
        NVarchar2 = 119, // 0x00000077
        Raw = 120, // 0x00000078
        RefCursor = 121, // 0x00000079
        Single = 122, // 0x0000007A
        TimeStamp = 123, // 0x0000007B
        TimeStampLTZ = 124, // 0x0000007C
        TimeStampTZ = 125, // 0x0000007D
        Varchar2 = 126, // 0x0000007E
        XmlType = 127, // 0x0000007F
        BinaryDouble = 132, // 0x00000084
        BinaryFloat = 133, // 0x00000085
        Boolean = 134, // 0x00000086
    }

    public enum NpgsqlType
    {
        /// <summary>
        /// Corresponds to the PostgreSQL "array" type, a variable-length multidimensional array of
        /// another type. This value must be combined with another value from <see cref="T:NpgsqlTypes.NpgsqlDbType" />
        /// via a bit OR (e.g. NpgsqlDbType.Array | NpgsqlDbType.Integer)
        /// </summary>
        /// <remarks>See http://www.postgresql.org/docs/current/static/arrays.html</remarks>
        Array = -2147483648, // 0x80000000
        /// <summary>Corresponds to the PostgreSQL 8-byte "bigint" type.</summary>
        /// <remarks>See http://www.postgresql.org/docs/current/static/datatype-numeric.html</remarks>
        Bigint = 1,
        /// <summary>Corresponds to the PostgreSQL "boolean" type.</summary>
        /// <remarks>See http://www.postgresql.org/docs/current/static/datatype-boolean.html</remarks>
        Boolean = 2,
        /// <summary>Corresponds to the PostgreSQL geometric "box" type.</summary>
        /// <remarks>See http://www.postgresql.org/docs/current/static/datatype-geometric.html</remarks>
        Box = 3,
        /// <summary>
        /// Corresponds to the PostgreSQL "bytea" type, holding a raw byte string.
        /// </summary>
        /// <remarks>See http://www.postgresql.org/docs/current/static/datatype-binary.html</remarks>
        Bytea = 4,
        /// <summary>
        /// Corresponds to the PostgreSQL geometric "circle" type.
        /// </summary>
        /// <remarks>See http://www.postgresql.org/docs/current/static/datatype-geometric.html</remarks>
        Circle = 5,
        /// <summary>Corresponds to the PostgreSQL "char(n)"type.</summary>
        /// <remarks>See http://www.postgresql.org/docs/current/static/datatype-character.html</remarks>
        Char = 6,
        /// <summary>Corresponds to the PostgreSQL "date" type.</summary>
        /// <remarks>See http://www.postgresql.org/docs/current/static/datatype-datetime.html</remarks>
        Date = 7,
        /// <summary>
        /// Corresponds to the PostgreSQL 8-byte floating-point "double" type.
        /// </summary>
        /// <remarks>See http://www.postgresql.org/docs/current/static/datatype-numeric.html</remarks>
        Double = 8,
        /// <summary>Corresponds to the PostgreSQL 4-byte "integer" type.</summary>
        /// <remarks>See http://www.postgresql.org/docs/current/static/datatype-numeric.html</remarks>
        Integer = 9,
        /// <summary>Corresponds to the PostgreSQL geometric "line" type.</summary>
        /// <remarks>See http://www.postgresql.org/docs/current/static/datatype-geometric.html</remarks>
        Line = 10, // 0x0000000A
        /// <summary>Corresponds to the PostgreSQL geometric "lseg" type.</summary>
        /// <remarks>See http://www.postgresql.org/docs/current/static/datatype-geometric.html</remarks>
        LSeg = 11, // 0x0000000B
        /// <summary>Corresponds to the PostgreSQL "money" type.</summary>
        /// <remarks>See http://www.postgresql.org/docs/current/static/datatype-money.html</remarks>
        Money = 12, // 0x0000000C
        /// <summary>
        /// Corresponds to the PostgreSQL arbitrary-precision "numeric" type.
        /// </summary>
        /// <remarks>See http://www.postgresql.org/docs/current/static/datatype-numeric.html</remarks>
        Numeric = 13, // 0x0000000D
        /// <summary>Corresponds to the PostgreSQL geometric "path" type.</summary>
        /// <remarks>See http://www.postgresql.org/docs/current/static/datatype-geometric.html</remarks>
        Path = 14, // 0x0000000E
        /// <summary>Corresponds to the PostgreSQL geometric "point" type.</summary>
        /// <remarks>See http://www.postgresql.org/docs/current/static/datatype-geometric.html</remarks>
        Point = 15, // 0x0000000F
        /// <summary>
        /// Corresponds to the PostgreSQL geometric "polygon" type.
        /// </summary>
        /// <remarks>See http://www.postgresql.org/docs/current/static/datatype-geometric.html</remarks>
        Polygon = 16, // 0x00000010
        /// <summary>
        /// Corresponds to the PostgreSQL floating-point "real" type.
        /// </summary>
        /// <remarks>See http://www.postgresql.org/docs/current/static/datatype-numeric.html</remarks>
        Real = 17, // 0x00000011
        /// <summary>Corresponds to the PostgreSQL 2-byte "smallint" type.</summary>
        /// <remarks>See http://www.postgresql.org/docs/current/static/datatype-numeric.html</remarks>
        Smallint = 18, // 0x00000012
        /// <summary>Corresponds to the PostgreSQL "text" type.</summary>
        /// <remarks>See http://www.postgresql.org/docs/current/static/datatype-character.html</remarks>
        Text = 19, // 0x00000013
        /// <summary>Corresponds to the PostgreSQL "time" type.</summary>
        /// <remarks>See http://www.postgresql.org/docs/current/static/datatype-datetime.html</remarks>
        Time = 20, // 0x00000014
        /// <summary>Corresponds to the PostgreSQL "timestamp" type.</summary>
        /// <remarks>See http://www.postgresql.org/docs/current/static/datatype-datetime.html</remarks>
        Timestamp = 21, // 0x00000015
        /// <summary>Corresponds to the PostgreSQL "varchar" type.</summary>
        /// <remarks>See http://www.postgresql.org/docs/current/static/datatype-character.html</remarks>
        Varchar = 22, // 0x00000016
        /// <summary>Corresponds to the PostgreSQL "refcursor" type.</summary>
        Refcursor = 23, // 0x00000017
        /// <summary>Corresponds to the PostgreSQL "inet" type.</summary>
        /// <remarks>See http://www.postgresql.org/docs/current/static/datatype-net-types.html</remarks>
        Inet = 24, // 0x00000018
        /// <summary>Corresponds to the PostgreSQL "bit" type.</summary>
        /// <remarks>See http://www.postgresql.org/docs/current/static/datatype-bit.html</remarks>
        Bit = 25, // 0x00000019
        /// <summary>
        /// Corresponds to the PostgreSQL "timestamp with time zone" type.
        /// </summary>
        /// <remarks>See http://www.postgresql.org/docs/current/static/datatype-datetime.html</remarks>
        TimestampTZ = 26, // 0x0000001A
        /// <summary>Corresponds to the PostgreSQL "uuid" type.</summary>
        /// <remarks>See http://www.postgresql.org/docs/current/static/datatype-uuid.html</remarks>
        Uuid = 27, // 0x0000001B
        /// <summary>Corresponds to the PostgreSQL "xml" type.</summary>
        /// <remarks>See http://www.postgresql.org/docs/current/static/datatype-xml.html</remarks>
        Xml = 28, // 0x0000001C
        /// <summary>
        /// Corresponds to the PostgreSQL internal "oidvector" type.
        /// </summary>
        /// <remarks>See http://www.postgresql.org/docs/current/static/datatype-oid.html</remarks>
        Oidvector = 29, // 0x0000001D
        /// <summary>Corresponds to the PostgreSQL "interval" type.</summary>
        /// <remarks>See http://www.postgresql.org/docs/current/static/datatype-datetime.html</remarks>
        Interval = 30, // 0x0000001E
        /// <summary>
        /// Corresponds to the PostgreSQL "time with time zone" type.
        /// </summary>
        /// <remarks>See http://www.postgresql.org/docs/current/static/datatype-datetime.html</remarks>
        TimeTZ = 31, // 0x0000001F
        /// <summary>Corresponds to the PostgreSQL internal "name" type.</summary>
        /// <remarks>See http://www.postgresql.org/docs/current/static/datatype-character.html</remarks>
        Name = 32, // 0x00000020
        /// <summary>
        /// Corresponds to the obsolete PostgreSQL "abstime" type.
        /// </summary>
        /// <remarks>See http://www.postgresql.org/docs/current/static/datatype-datetime.html</remarks>
        [Obsolete("The PostgreSQL abstime time is obsolete.")] Abstime = 33, // 0x00000021
        /// <summary>
        /// Corresponds to the PostgreSQL "macaddr" type, a field storing a 6-byte physical address.
        /// </summary>
        /// <remarks>See http://www.postgresql.org/docs/current/static/datatype-net-types.html</remarks>
        MacAddr = 34, // 0x00000022
        /// <summary>
        /// Corresponds to the PostgreSQL "json" type, a field storing JSON in text format.
        /// </summary>
        /// <remarks>See http://www.postgresql.org/docs/current/static/datatype-json.html</remarks>
        /// <seealso cref="F:NpgsqlTypes.NpgsqlDbType.Jsonb" />
        Json = 35, // 0x00000023
        /// <summary>
        /// Corresponds to the PostgreSQL "jsonb" type, a field storing JSON in an optimized binary
        /// format.
        /// </summary>
        /// <remarks>
        /// Supported since PostgreSQL 9.4.
        /// See http://www.postgresql.org/docs/current/static/datatype-json.html
        /// </remarks>
        Jsonb = 36, // 0x00000024
        /// <summary>
        /// Corresponds to the PostgreSQL "hstore" type, a dictionary of string key-value pairs.
        /// </summary>
        /// <remarks>See http://www.postgresql.org/docs/current/static/hstore.html</remarks>
        Hstore = 37, // 0x00000025
        /// <summary>Corresponds to the PostgreSQL "char" type.</summary>
        /// <remarks>
        /// This is an internal field and should normally not be used for regular applications.
        /// 
        /// See http://www.postgresql.org/docs/current/static/datatype-text.html
        /// </remarks>
        InternalChar = 38, // 0x00000026
        /// <summary>
        /// Corresponds to the PostgreSQL "varbit" type, a field storing a variable-length string of bits.
        /// </summary>
        /// <remarks>See http://www.postgresql.org/docs/current/static/datatype-boolean.html</remarks>
        Varbit = 39, // 0x00000027
        /// <summary>
        /// A special value that can be used to send parameter values to the database without
        /// specifying their type, allowing the database to cast them to another value based on context.
        /// The value will be converted to a string and send as text.
        /// </summary>
        /// <remarks>
        /// This value shouldn't ordinarily be used, and makes sense only when sending a data type
        /// unsupported by Npgsql.
        /// </remarks>
        Unknown = 40, // 0x00000028
        /// <summary>Corresponds to the PostgreSQL "oid" type.</summary>
        /// <remarks>See http://www.postgresql.org/docs/current/static/datatype-oid.html</remarks>
        Oid = 41, // 0x00000029
        /// <summary>
        /// Corresponds to the PostgreSQL "xid" type, an internal transaction identifier.
        /// </summary>
        /// <remarks>See http://www.postgresql.org/docs/current/static/datatype-oid.html</remarks>
        Xid = 42, // 0x0000002A
        /// <summary>
        /// Corresponds to the PostgreSQL "cid" type, an internal command identifier.
        /// </summary>
        /// <remarks>See http://www.postgresql.org/docs/current/static/datatype-oid.html</remarks>
        Cid = 43, // 0x0000002B
        /// <summary>
        /// Corresponds to the PostgreSQL "cidr" type, a field storing an IPv4 or IPv6 network.
        /// </summary>
        /// <remarks>See http://www.postgresql.org/docs/current/static/datatype-net-types.html</remarks>
        Cidr = 44, // 0x0000002C
        /// <summary>Corresponds to the PostgreSQL "tsvector" type.</summary>
        /// <remarks>See http://www.postgresql.org/docs/current/static/datatype-textsearch.html</remarks>
        TsVector = 45, // 0x0000002D
        /// <summary>Corresponds to the PostgreSQL "tsquery" type.</summary>
        /// <remarks>See http://www.postgresql.org/docs/current/static/datatype-textsearch.html</remarks>
        TsQuery = 46, // 0x0000002E
        /// <summary>Corresponds to the PostgreSQL "enum" type.</summary>
        /// <remarks>See http://www.postgresql.org/docs/current/static/datatype-enum.html</remarks>
        Enum = 47, // 0x0000002F
        /// <summary>Corresponds to the PostgreSQL "composite" type.</summary>
        /// <remarks>See http://www.postgresql.org/docs/current/static/rowtypes.html</remarks>
        Composite = 48, // 0x00000030
        /// <summary>
        /// Corresponds to the PostgreSQL "regtype" type, a numeric (OID) ID of a type in the pg_type table.
        /// </summary>
        Regtype = 49, // 0x00000031
        /// <summary>
        /// The geometry type for postgresql spatial extension postgis.
        /// </summary>
        Geometry = 50, // 0x00000032
        /// <summary>
        /// Corresponds to the PostgreSQL "citext" type for the citext module.
        /// </summary>
        /// <remarks>See http://www.postgresql.org/docs/current/static/citext.html</remarks>
        Citext = 51, // 0x00000033
        /// <summary>
        /// Corresponds to the PostgreSQL internal "int2vector" type.
        /// </summary>
        Int2Vector = 52, // 0x00000034
        /// <summary>
        /// Corresponds to the PostgreSQL "tid" type, a tuple id identifying the physical location of a row within its table.
        /// </summary>
        Tid = 53, // 0x00000035
        /// <summary>
        /// Corresponds to the PostgreSQL "macaddr8" type, a field storing a 6-byte or 8-byte physical address.
        /// </summary>
        /// <remarks>See http://www.postgresql.org/docs/current/static/datatype-net-types.html</remarks>
        MacAddr8 = 54, // 0x00000036
        /// <summary>
        /// Corresponds to the PostgreSQL "array" type, a variable-length multidimensional array of
        /// another type. This value must be combined with another value from <see cref="T:NpgsqlTypes.NpgsqlDbType" />
        /// via a bit OR (e.g. NpgsqlDbType.Array | NpgsqlDbType.Integer)
        /// </summary>
        /// <remarks>
        /// Supported since PostgreSQL 9.2.
        /// See http://www.postgresql.org/docs/9.2/static/rangetypes.html
        /// </remarks>
        Range = 1073741824, // 0x40000000
    }

    /// <summary>
    /// input parameter
    /// 输入参数
    /// </summary>
    public struct ParameterClass
    {
        public String ParameterName;
        public object Value;
    }

    /// <summary>
    /// output parameter
    /// 输出参数
    /// </summary>
    public struct ParameterOutClass
    {
        public String ParameterName;
        public DbType SqliteDbType;
        public SqlDbType SqlDbType;
        public MySqlType MySqlDbType;
        public OracleType OracleDbType;
        public NpgsqlType NpgsqlDbType;
        public object Value;
    }

    /// <summary>
    /// return parameter
    /// 返回参数
    /// </summary>
    public struct ParameterReturnClass
    {
        public String ParameterName;
        public DbType SqliteDbType;
        public SqlDbType SqlDbType;
        public MySqlType MySqlDbType;
        public OracleType OracleDbType;
        public NpgsqlType NpgsqlDbType;
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
            ParameterClass[] ParameterIn, ParameterOutClass[] ParameterOut, Int32 Timeout = 30);

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
