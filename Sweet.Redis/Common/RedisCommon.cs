﻿#region License
//  The MIT License (MIT)
//
//  Copyright (c) 2017, Cagatay Dogan
//
//  Permission is hereby granted, free of charge, to any person obtaining a copy
//  of this software and associated documentation files (the "Software"), to deal
//  in the Software without restriction, including without limitation the rights
//  to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
//  copies of the Software, and to permit persons to whom the Software is
//  furnished to do so, subject to the following conditions:
//
//      The above copyright notice and this permission notice shall be included in
//      all copies or substantial portions of the Software.
//
//      THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
//      IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
//      FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
//      AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
//      LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
//      OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
//      THE SOFTWARE.
#endregion License

using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Net.Sockets;
using System.Text;

namespace Sweet.Redis
{
    public static class RedisCommon
    {
        #region Static Members

        private static readonly uint[] DefaultCRC32Vector;
        private static readonly DateTime UnixBaseTimeStamp = new DateTime(1970, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc);

        #endregion Static Members

        #region Constants

        private const uint CrcPoly = 0xedb88320;
        private const uint CrcInit = 0xffffffff;
        private const int CRC32TableLength = 256;

        #endregion Constants

        #region Properties

        public static bool IsLinux
        {
            get
            {
                int p = (int)Environment.OSVersion.Platform;
                return (p == 4) || (p == 6) || (p == 128);
            }
        }

        public static bool IsWindows
        {
            get
            {
                var pid = Environment.OSVersion.Platform;
                switch (pid)
                {
                    case PlatformID.Win32NT:
                    case PlatformID.Win32S:
                    case PlatformID.Win32Windows:
                    case PlatformID.WinCE:
                        return true;
                    default:
                        return false;
                }
            }
        }

        #endregion Properties

        #region .Ctors

        static RedisCommon()
        {
            DefaultCRC32Vector = CalculateCRC32Vector();
        }

        #endregion .Ctors

        #region Methods

        #region CRC32

        public static uint CRC32ChecksumOf(byte[] bytes)
        {
            if (bytes != null)
            {
                var vector = NewCRC32Vector();
                var bytesLength = bytes.Length;

                var crc = CrcInit;
                for (var i = 0; i < bytesLength; ++i)
                {
                    var index = (byte)(((crc) & 0xff) ^ bytes[i]);
                    crc = (uint)((crc >> 8) ^ vector[index]);
                }
                return ~crc;
            }
            return 0u;
        }


        public static byte[] CRC32ChecksumBytesOf(byte[] bytes)
        {
            return BitConverter.GetBytes(CRC32ChecksumOf(bytes));
        }

        private static uint[] NewCRC32Vector()
        {
            var result = new uint[CRC32TableLength];
            Array.Copy(DefaultCRC32Vector, result, CRC32TableLength);
            return result;
        }

        private static uint[] CalculateCRC32Vector()
        {
            var result = new uint[CRC32TableLength];

            var crcItem = 0u;
            for (var i = 0u; i < CRC32TableLength; ++i)
            {
                crcItem = i;
                for (var j = 8; j > 0; --j)
                {
                    if ((crcItem & 1) == 1)
                        crcItem = (uint)((crcItem >> 1) ^ CrcPoly);
                    else
                        crcItem >>= 1;
                }
                result[i] = crcItem;
            }
            return result;
        }

        #endregion CRC32

        #region General

        internal static RedisEndPoint ToRedisEndPoint(this string host, int port = RedisConstants.DefaultPort)
        {
            host = (host ?? String.Empty).Trim();
            if (!host.IsEmpty())
            {
                port = port < 1 ? RedisConstants.DefaultPort : port;

                var colonPos = host.LastIndexOf(':');
                if (colonPos == -1 || colonPos == host.Length - 1)
                    return new RedisEndPoint(host, port);

                var isIP4 = host.IndexOf('.') > -1;
                if (!isIP4)
                {
                    var colonCount = host.Count((ch) => ch == ':');

                    var isIP6 = (colonCount > 1);
                    if (isIP6 && colonCount == 2)
                        colonPos = -1;
                }

                var name = (host.Substring(0, colonPos) ?? String.Empty).TrimEnd();
                if (name.IsEmpty())
                    name = RedisConstants.LocalHost;

                if (colonPos == host.Length - 1)
                    return new RedisEndPoint(name, port);

                var portStr = (host.Substring(colonPos + 1) ?? String.Empty).TrimStart();
                if (portStr.IsEmpty())
                    return new RedisEndPoint(name, port);

                var portInt = int.Parse(portStr);
                port = portInt < 1 ? port : portInt;

                return new RedisEndPoint(name, port);
            }
            return RedisEndPoint.Empty;
        }

        internal static RedisRole ToRedisRole(this string roleStr)
        {
            if (!roleStr.IsEmpty())
            {
                roleStr = roleStr.ToLowerInvariant();
                switch (roleStr)
                {
                    case "master":
                        return RedisRole.Master;
                    case "slave":
                        return RedisRole.Slave;
                    case "sentinel":
                        return RedisRole.Sentinel;
                    default:
                        break;
                }
            }
            return RedisRole.Undefined;
        }

        internal static SocketError GetSocketErrorCode(this Exception exception)
        {
            while (exception != null)
            {
                var redisError = exception as RedisException;
                if (redisError == null)
                {
                    if (exception is SocketException)
                        return ((SocketException)exception).SocketErrorCode;
                }
                else
                {
                    var errorCode = redisError.ErrorCode;
                    switch (errorCode)
                    {
                        case RedisErrorCode.ConnectionError:
                        case RedisErrorCode.CorruptData:
                        case RedisErrorCode.CorruptResponse:
                        case RedisErrorCode.SocketError:
                            return (SocketError)errorCode;
                    }
                }
                exception = exception.InnerException;
            }
            return SocketError.Success;
        }

        internal static bool IsSocketError(this Exception exception)
        {
            while (exception != null)
            {
                var redisError = exception as RedisException;
                if (redisError == null)
                {
                    if (exception is SocketException)
                        return true;
                }
                else
                {
                    switch (redisError.ErrorCode)
                    {
                        case RedisErrorCode.ConnectionError:
                        case RedisErrorCode.CorruptData:
                        case RedisErrorCode.CorruptResponse:
                        case RedisErrorCode.SocketError:
                            return true;
                    }
                }
                exception = exception.InnerException;
            }
            return false;
        }

        internal static bool IsEmpty(this string obj)
        {
            return (obj == null || obj.Length == 0);
        }

        internal static bool IsEmpty(this ICollection obj)
        {
            return (obj == null || obj.Count == 0);
        }

        internal static bool IsEmpty(this RedisEndPoint endPoint)
        {
            return (endPoint == null || endPoint.IsEmpty);
        }

        internal static bool IsAlive(this IRedisDisposableBase obj)
        {
            return (obj != null) && !obj.Disposed;
        }

        internal static byte[] Clone(this byte[] x, int offset = 0, int length = -1)
        {
            if (x != null)
            {
                var xLength = x.Length;
                if (offset < xLength)
                {
                    if (xLength == 0)
                        return new byte[0];

                    if (offset < 0) offset = 0;

                    if (length < 0) length = xLength;

                    length = Math.Min(length, xLength - offset);
                    if (length > -1)
                    {
                        var result = new byte[length];
                        if (length == 0)
                            return result;

                        Array.Copy(x, offset, result, 0, length);
                        return result;
                    }
                }
            }
            return null;
        }

        internal static string ToUTF8String(this byte[] x, int offset = 0, int length = -1)
        {
            if (x != null)
            {
                var xLength = x.Length;
                if (offset < xLength)
                {
                    if (xLength == 0)
                        return String.Empty;

                    if (offset < 0) offset = 0;

                    if (length < 0) length = xLength;

                    length = Math.Min(length, xLength - offset);
                    if (length > -1)
                    {
                        if (length == 0)
                            return String.Empty;
                        return Encoding.UTF8.GetString(x, offset, length);
                    }
                }
            }
            return null;
        }

        internal static bool EqualTo(this byte[] x, byte[] y)
        {
            if (ReferenceEquals(x, y))
                return true;

            if (ReferenceEquals(x, null))
                return ReferenceEquals(y, null);

            if (ReferenceEquals(y, null))
                return false;

            var l1 = x.Length;
            var l2 = y.Length;

            if (l1 != l2)
                return false;

            for (var i = 0; i < l1; i++)
                if (x[i] != y[i])
                    return false;

            return true;
        }

        internal static bool EqualTo(this byte[] data, object obj)
        {
            if (ReferenceEquals(data, null))
                return ReferenceEquals(obj, null);

            if (ReferenceEquals(obj, null))
                return false;

            if (ReferenceEquals(data, obj))
                return true;

            if (obj is byte[])
                return data.EqualTo((byte[])obj);

            if (obj is RedisParam)
                return data.EqualTo(((RedisParam)obj).Data);

            if (obj is string)
                return data.EqualTo(Encoding.UTF8.GetBytes((string)obj));

            if (obj is long)
                return data.EqualTo(BitConverter.GetBytes((long)obj));

            if (obj is int)
                return data.EqualTo(BitConverter.GetBytes((int)obj));

            if (obj is short)
                return data.EqualTo(BitConverter.GetBytes((short)obj));

            if (obj is double)
                return data.EqualTo(BitConverter.GetBytes((double)obj));

            if (obj is byte)
                return data.EqualTo(new byte[] { (byte)obj });

            if (obj is ulong)
                return data.EqualTo(BitConverter.GetBytes((ulong)obj));

            if (obj is uint)
                return data.EqualTo(BitConverter.GetBytes((uint)obj));

            if (obj is ushort)
                return data.EqualTo(BitConverter.GetBytes((ushort)obj));

            if (obj is DateTime)
                return data.EqualTo(BitConverter.GetBytes(((DateTime)obj).Ticks));

            if (obj is TimeSpan)
                return data.EqualTo(BitConverter.GetBytes(((TimeSpan)obj).Ticks));

            if (obj is char)
                return data.EqualTo(BitConverter.GetBytes((char)obj));

            if (obj is bool)
                return data.EqualTo(BitConverter.GetBytes((bool)obj));

            if (obj is long?)
            {
                var nullable = (long?)obj;
                return nullable.HasValue && data.EqualTo(BitConverter.GetBytes(nullable.Value));
            }

            if (obj is int?)
            {
                var nullable = (int?)obj;
                return nullable.HasValue && data.EqualTo(BitConverter.GetBytes(nullable.Value));
            }

            if (obj is short?)
            {
                var nullable = (short?)obj;
                return nullable.HasValue && data.EqualTo(BitConverter.GetBytes(nullable.Value));
            }

            if (obj is double?)
            {
                var nullable = (long?)obj;
                return nullable.HasValue && data.EqualTo(BitConverter.GetBytes(nullable.Value));
            }

            if (obj is byte?)
            {
                var nullable = (byte?)obj;
                return nullable.HasValue && data.EqualTo(new byte[] { nullable.Value });
            }

            if (obj is ulong?)
            {
                var nullable = (ulong?)obj;
                return nullable.HasValue && data.EqualTo(BitConverter.GetBytes(nullable.Value));
            }

            if (obj is uint?)
            {
                var nullable = (uint?)obj;
                return nullable.HasValue && data.EqualTo(BitConverter.GetBytes(nullable.Value));
            }

            if (obj is ushort?)
            {
                var nullable = (ushort?)obj;
                return nullable.HasValue && data.EqualTo(BitConverter.GetBytes(nullable.Value));
            }

            if (obj is DateTime?)
            {
                var nullable = (DateTime?)obj;
                return nullable.HasValue && data.EqualTo(BitConverter.GetBytes(nullable.Value.Ticks));
            }

            if (obj is TimeSpan?)
            {
                var nullable = (TimeSpan?)obj;
                return nullable.HasValue && data.EqualTo(BitConverter.GetBytes(nullable.Value.Ticks));
            }

            if (obj is char?)
            {
                var nullable = (char?)obj;
                return nullable.HasValue && data.EqualTo(BitConverter.GetBytes(nullable.Value));
            }

            if (obj is bool?)
            {
                var nullable = (bool?)obj;
                return nullable.HasValue && data.EqualTo(BitConverter.GetBytes(nullable.Value));
            }

            return false;
        }

        internal static int IndexOf(this byte[] data, byte b, int startPos = 0, int length = -1)
        {
            if (data != null && length != 0)
            {
                var dataLength = data.Length;

                startPos = Math.Max(0, startPos);
                if (dataLength > 0 && startPos < dataLength)
                {
                    var endPos = dataLength;
                    if (length > 0)
                        endPos = Math.Min(dataLength, startPos + length);

                    for (var i = startPos; i < endPos; i++)
                        if (data[i] == b)
                            return i;
                }
            }
            return -1;
        }

        internal static int ScanCRLF(this byte[] data, int index = 0, int length = -1)
        {
            if (data != null)
            {
                var dataLen = data.Length;
                if (dataLen > 0)
                {
                    length = Math.Max(0, Math.Min(dataLen - index, length));
                    if (length > 0)
                    {
                        var end = Math.Min(dataLen, index + length);
                        for (var i = index; i < end; i++)
                            if (data[i] == '\n' && i >= index && data[i - 1] == '\r')
                                return i;
                    }
                }
            }
            return -1;
        }

        internal static bool Equals<T>(this T[] source, T[] destination, Func<T, T, bool> comparer)
        {
            if (comparer == null)
                throw new ArgumentNullException("comparer");

            if (source == null)
                return destination == null;

            if (destination == null)
                return source == null;

            var sourceLen = source.Length;
            if (sourceLen == destination.Length)
            {
                if (sourceLen > 0)
                {
                    for (var i = 0; i < sourceLen; i++)
                    {
                        if (!comparer(source[i], destination[i]))
                            return false;
                    }
                }
                return true;
            }
            return false;
        }

        internal static string NewGuidID()
        {
            return Guid.NewGuid().ToString("N").ToUpper();
        }

        #endregion General

        #region Conversion

        internal static RedisRawObjectType ResponseType(this byte b)
        {
            switch (b)
            {
                case (byte)'+':
                    return RedisRawObjectType.SimpleString;
                case (byte)'-':
                    return RedisRawObjectType.Error;
                case (byte)'$':
                    return RedisRawObjectType.BulkString;
                case (byte)':':
                    return RedisRawObjectType.Integer;
                case (byte)'*':
                    return RedisRawObjectType.Array;
                default:
                    return RedisRawObjectType.Undefined;
            }
        }

        internal static byte ResponseTypeByte(this RedisRawObjectType b)
        {
            switch (b)
            {
                case RedisRawObjectType.SimpleString:
                    return (byte)'+';
                case RedisRawObjectType.Error:
                    return (byte)'-';
                case RedisRawObjectType.BulkString:
                    return (byte)'$';
                case RedisRawObjectType.Integer:
                    return (byte)':';
                case RedisRawObjectType.Array:
                    return (byte)'*';
                default:
                    return (byte)'?';
            }
        }

        internal static double EpochNow()
        {
            return (DateTime.UtcNow - UnixBaseTimeStamp).TotalSeconds;
        }

        internal static double UnixTimeNow()
        {
            return EpochNow();
        }

        internal static double ToUnixTimeStamp(this DateTime date)
        {
            if (date.Kind != DateTimeKind.Utc)
                date = date.ToUniversalTime();
            return (date - UnixBaseTimeStamp).TotalSeconds;
        }

        internal static DateTime FromUnixTimeStamp(this int seconds)
        {
            return FromUnixTimeStamp((long)seconds);
        }

        internal static DateTime FromUnixTimeStamp(this long seconds)
        {
            return UnixBaseTimeStamp.AddSeconds(seconds).ToLocalTime();
        }

        internal static DateTime FromUnixTimeStamp(this double seconds)
        {
            return UnixBaseTimeStamp.AddSeconds(seconds).ToLocalTime();
        }

        internal static DateTime FromUnixTimeStamp(this int seconds, int microSeconds)
        {
            return FromUnixTimeStamp((long)seconds, microSeconds);
        }

        internal static DateTime FromUnixTimeStamp(this long seconds, int microSeconds)
        {
            var date = UnixBaseTimeStamp.AddSeconds(seconds).ToLocalTime();
            return date.AddTicks(microSeconds * 10);
        }

        internal static DateTime FromUnixTimeStamp(this double seconds, int microSeconds)
        {
            var date = UnixBaseTimeStamp.AddSeconds(seconds).ToLocalTime();
            return date.AddTicks(microSeconds * 10);
        }

        internal static byte[] ToBytes(this object obj)
        {
            if (obj != null)
            {
                var tc = Type.GetTypeCode(obj.GetType());
                switch (tc)
                {
                    case TypeCode.Object:
                        if (obj is RedisParam)
                            return ((RedisParam)obj).Data;
                        return Encoding.UTF8.GetBytes(obj.ToString());
                    case TypeCode.String:
                        return Encoding.UTF8.GetBytes((string)obj);
                    case TypeCode.Int32:
                        return Encoding.UTF8.GetBytes(((int)obj).ToString(RedisConstants.InvariantCulture));
                    case TypeCode.Int64:
                        return Encoding.UTF8.GetBytes(((long)obj).ToString(RedisConstants.InvariantCulture));
                    case TypeCode.Decimal:
                        return Encoding.UTF8.GetBytes(((decimal)obj).ToString(RedisConstants.InvariantCulture));
                    case TypeCode.Double:
                        return Encoding.UTF8.GetBytes(((double)obj).ToString(RedisConstants.InvariantCulture));
                    case TypeCode.Boolean:
                        return Encoding.UTF8.GetBytes((bool)obj ? Boolean.TrueString : Boolean.FalseString);
                    case TypeCode.Single:
                        return Encoding.UTF8.GetBytes(((float)obj).ToString(RedisConstants.InvariantCulture));
                    case TypeCode.Int16:
                        return Encoding.UTF8.GetBytes(((short)obj).ToString(RedisConstants.InvariantCulture));
                    case TypeCode.UInt32:
                        return Encoding.UTF8.GetBytes(((uint)obj).ToString(RedisConstants.InvariantCulture));
                    case TypeCode.UInt64:
                        return Encoding.UTF8.GetBytes(((ulong)obj).ToString(RedisConstants.InvariantCulture));
                    case TypeCode.UInt16:
                        return Encoding.UTF8.GetBytes(((ushort)obj).ToString(RedisConstants.InvariantCulture));
                    case TypeCode.DateTime:
                        return Encoding.UTF8.GetBytes(((DateTime)obj).Ticks.ToString(RedisConstants.InvariantCulture));
                    case TypeCode.Char:
                        return Encoding.UTF8.GetBytes(new char[] { (char)obj });
                    case TypeCode.Byte:
                        return new byte[] { (byte)obj };
                    default:
                        break;
                }
            }
            return null;
        }

        internal static int ToInt(this string s, int defaultValue = int.MinValue)
        {
            if (!s.IsEmpty())
            {
                int result;
                if (int.TryParse(s, out result))
                    return result;
            }
            return defaultValue;
        }

        internal static long ToLong(this string s, long defaultValue = long.MinValue)
        {
            if (!s.IsEmpty())
            {
                long result;
                if (long.TryParse(s, out result))
                    return result;
            }
            return defaultValue;
        }

        internal static RedisResult ToRedisResult(this object obj)
        {
            if (!ReferenceEquals(obj, null))
            {
                var tc = Type.GetTypeCode(obj.GetType());
                switch (tc)
                {
                    case TypeCode.Object:
                        if (obj is RedisParam)
                            return (RedisBytes)((RedisParam)obj).Data;
                        if (obj is RedisResult)
                            return (RedisResult)obj;
                        if (obj is byte[])
                            return (RedisBytes)(byte[])obj;
                        if (obj is byte[][])
                            return (RedisMultiBytes)(byte[][])obj;
                        if (obj is string[])
                            return (RedisMultiString)(string[])obj;
                        if (obj is long[])
                            return (RedisMultiInteger)(long[])obj;
                        if (obj is int[])
                            return (RedisMultiInteger)(int[])obj;
                        if (obj is double[])
                            return (RedisMultiDouble)(double[])obj;
                        if (obj is decimal[])
                            return (RedisMultiDouble)(decimal[])obj;
                        if (obj is float[])
                            return (RedisMultiDouble)(float[])obj;
                        if (obj is short[])
                            return (RedisMultiInteger)(short[])obj;
                        return (RedisString)obj.ToString();
                    case TypeCode.String:
                        return (RedisString)(string)obj;
                    case TypeCode.Int32:
                        return (RedisInteger)(int)obj;
                    case TypeCode.Int64:
                        return (RedisInteger)(long)obj;
                    case TypeCode.Decimal:
                        return (RedisDouble)(decimal)obj;
                    case TypeCode.Double:
                        return (RedisDouble)(double)obj;
                    case TypeCode.Boolean:
                        return (RedisBool)(bool)obj;
                    case TypeCode.Single:
                        return (RedisDouble)(float)obj;
                    case TypeCode.Int16:
                        return (RedisInteger)(short)obj;
                    case TypeCode.UInt32:
                        return (RedisInteger)(uint)obj;
                    case TypeCode.UInt64:
                        return (RedisInteger)(ulong)obj;
                    case TypeCode.UInt16:
                        return (RedisInteger)(ushort)obj;
                    case TypeCode.DateTime:
                        return (RedisInteger)((DateTime)obj).Ticks;
                    case TypeCode.Char:
                        return (RedisString)obj.ToString();
                    case TypeCode.Byte:
                        return (RedisBytes)(new byte[] { (byte)obj });
                    default:
                        break;
                }
            }
            return null;
        }

        internal static RedisResult<T> ToRedisResult<T>(this T obj)
        {
            if (!ReferenceEquals(obj, null))
            {
                var tc = Type.GetTypeCode(typeof(T));
                switch (tc)
                {
                    case TypeCode.Object:
                        if (obj is RedisResult)
                            return (RedisResult<T>)(RedisResult)(object)obj;
                        if (obj is byte[])
                            return (RedisResult<T>)(object)(RedisBytes)(byte[])(object)obj;
                        if (obj is byte[][])
                            return (RedisResult<T>)(object)(RedisMultiBytes)(byte[][])(object)obj;
                        if (obj is string[])
                            return (RedisResult<T>)(object)(RedisMultiString)(string[])(object)obj;
                        if (obj is long[])
                            return (RedisResult<T>)(object)(RedisMultiInteger)(long[])(object)obj;
                        if (obj is int[])
                            return (RedisResult<T>)(object)(RedisMultiInteger)(int[])(object)obj;
                        if (obj is double[])
                            return (RedisResult<T>)(object)(RedisMultiDouble)(double[])(object)obj;
                        if (obj is decimal[])
                            return (RedisResult<T>)(object)(RedisMultiDouble)(decimal[])(object)obj;
                        if (obj is float[])
                            return (RedisResult<T>)(object)(RedisMultiDouble)(float[])(object)obj;
                        if (obj is short[])
                            return (RedisResult<T>)(object)(RedisMultiInteger)(short[])(object)obj;
                        return (RedisResult<T>)(object)(RedisString)obj.ToString();
                    case TypeCode.String:
                        return (RedisResult<T>)(object)(RedisString)(string)(object)obj;
                    case TypeCode.Int32:
                        return (RedisResult<T>)(object)(RedisInteger)(int)(object)obj;
                    case TypeCode.Int64:
                        return (RedisResult<T>)(object)(RedisInteger)(long)(object)obj;
                    case TypeCode.Decimal:
                        return (RedisResult<T>)(object)(RedisDouble)(decimal)(object)obj;
                    case TypeCode.Double:
                        return (RedisResult<T>)(object)(RedisDouble)(double)(object)obj;
                    case TypeCode.Boolean:
                        return (RedisResult<T>)(object)(RedisBool)(bool)(object)obj;
                    case TypeCode.Single:
                        return (RedisResult<T>)(object)(RedisDouble)(float)(object)obj;
                    case TypeCode.Int16:
                        return (RedisResult<T>)(object)(RedisInteger)(short)(object)obj;
                    case TypeCode.UInt32:
                        return (RedisResult<T>)(object)(RedisInteger)(uint)(object)obj;
                    case TypeCode.UInt64:
                        return (RedisResult<T>)(object)(RedisInteger)(ulong)(object)obj;
                    case TypeCode.UInt16:
                        return (RedisResult<T>)(object)(RedisInteger)(ushort)(object)obj;
                    case TypeCode.DateTime:
                        return (RedisResult<T>)(object)(RedisInteger)((DateTime)(object)obj).Ticks;
                    case TypeCode.Char:
                        return (RedisResult<T>)(object)(RedisString)obj.ToString();
                    case TypeCode.Byte:
                        return (RedisResult<T>)(object)(RedisBytes)(new byte[] { (byte)(object)obj });
                    default:
                        break;
                }
            }
            return null;
        }

        #endregion Conversion

        #region ToBytesArray

        internal static byte[][] ToBytesArray(this RedisParam param)
        {
            if (!param.IsNull)
                return new byte[][] { param.Data };
            return null;
        }

        internal static byte[][] ToBytesArray(this RedisParam[] parameters)
        {
            if (parameters != null)
            {
                var length = parameters.Length;

                var result = new byte[length][];
                if (length > 0)
                {
                    for (var i = 0; i < length; i++)
                        result[i] = parameters[i].Data;
                }
                return result;
            }
            return null;
        }

        internal static byte[][] ToBytesArray(this string[] strings)
        {
            if (strings != null)
            {
                var length = strings.Length;

                var result = new byte[length][];
                if (length > 0)
                {
                    for (var i = 0; i < length; i++)
                    {
                        var str = strings[i];
                        result[i] = str != null ? str.ToBytes() : null;
                    }
                }
                return result;
            }
            return null;
        }

        internal static byte[][] ToBytesArray(this IList<string> strings)
        {
            if (strings != null)
            {
                var count = strings.Count;

                var result = new byte[count][];
                if (count > 0)
                {
                    for (var i = 0; i < count; i++)
                    {
                        var str = strings[i];
                        result[i] = str != null ? str.ToBytes() : null;
                    }
                }
                return result;
            }
            return null;
        }

        internal static byte[][] ToBytesArray(this byte[] bytes)
        {
            if (bytes != null)
                return new byte[][] { bytes };
            return null;
        }

        #endregion ToBytesArray

        #region Split

        internal static T[] Split<T>(this T[] source, int index, int length)
        {
            if (source == null)
                throw new ArgumentNullException("source");

            if (length < 0)
                throw new ArgumentException("Length can not be less than zero", "length");

            if (index < 0)
                throw new ArgumentException("Index can not be less than zero", "index");

            if (index > source.Length - 1)
                throw new ArgumentException("Index can not be greater than array length", "index");

            if (index + length > source.Length)
                throw new ArgumentException("Length can not be exceed array length", "length");

            var destination = new T[length];
            Array.Copy(source, index, destination, 0, length);

            return destination;
        }

        internal static T[] Split<T>(this T[] source, int index)
        {
            if (source == null)
                throw new ArgumentNullException("source");

            if (index < 0)
                throw new ArgumentException("Index can not be less than zero", "index");

            if (index > source.Length - 1)
                throw new ArgumentException("Index can not be greater than array length", "index");

            var length = source.Length - index;

            var destination = new T[length];
            Array.Copy(source, index, destination, 0, length);

            return destination;
        }

        #endregion Split

        #region ConvertToByteArray

        internal static byte[][] ConvertToByteArray(this string[] keys)
        {
            if (keys == null)
                throw new ArgumentNullException("keys");

            var keysLength = keys.Length;
            if (keysLength == 0)
                throw new ArgumentNullException("keys");

            var result = new byte[keysLength][];

            for (var i = 0; i < keysLength; i++)
                result[i] = keys[i].ToBytes();

            return result;
        }

        internal static byte[][] ConvertToByteArray(this RedisParam[] keys)
        {
            if (keys == null)
                throw new ArgumentNullException("keys");

            var keysLength = keys.Length;
            if (keysLength == 0)
                throw new ArgumentNullException("keys");

            var result = new byte[keysLength][];

            for (var i = 0; i < keysLength; i++)
                result[i] = keys[i].ToBytes();

            return result;
        }

        #endregion ConvertToByteArray

        #region Merge

        internal static byte[][] Merge(this RedisParam[] keys, RedisParam[] values)
        {
            var keysLength = (keys != null) ? keys.Length : -1;
            var valuesLength = (values != null) ? values.Length : -1;

            if (keysLength != valuesLength)
                throw new ArgumentException("keys length is not equal to values length", "keys");

            if (keysLength < 0)
                return null;

            if (keysLength == 0)
                return new byte[0][];

            var result = new byte[2 * keysLength][];

            for (int i = 0, index = 0; i < keysLength; i++)
            {
                result[index++] = keys[i].Data;
                result[index++] = values[i].Data;
            }
            return result;
        }

        internal static byte[][] Merge(this byte[][] keys, RedisParam[] values)
        {
            var keysLength = (keys != null) ? keys.Length : -1;
            var valuesLength = (values != null) ? values.Length : -1;

            if (keysLength != valuesLength)
                throw new ArgumentException("keys length is not equal to values length", "keys");

            if (keysLength < 0)
                return null;

            if (keysLength == 0)
                return new byte[0][];

            var result = new byte[2 * keysLength][];

            for (int i = 0, index = 0; i < keysLength; i++)
            {
                result[index++] = keys[i];
                result[index++] = values[i].Data;
            }
            return result;
        }

        internal static byte[][] Merge(this RedisParam[] keys, byte[][] values)
        {
            var keysLength = (keys != null) ? keys.Length : -1;
            var valuesLength = (values != null) ? values.Length : -1;

            if (keysLength != valuesLength)
                throw new ArgumentException("keys length is not equal to values length", "keys");

            if (keysLength < 0)
                return null;

            if (keysLength == 0)
                return new byte[0][];

            var result = new byte[2 * keysLength][];

            for (int i = 0, index = 0; i < keysLength; i++)
            {
                result[index++] = keys[i].Data;
                result[index++] = values[i];
            }
            return result;
        }

        internal static byte[][] Merge(this byte[][] keys, byte[][] values)
        {
            var keysLength = (keys != null) ? keys.Length : -1;
            var valuesLength = (values != null) ? values.Length : -1;

            if (keysLength != valuesLength)
                throw new ArgumentException("keys length is not equal to values length", "keys");

            if (keysLength < 0)
                return null;

            if (keysLength == 0)
                return new byte[0][];

            var result = new byte[2 * keysLength][];

            for (int i = 0, index = 0; i < keysLength; i++)
            {
                result[index++] = keys[i];
                result[index++] = values[i];
            }
            return result;
        }

        internal static byte[][] Merge(this byte[][] keys, string[] values)
        {
            var keysLength = (keys != null) ? keys.Length : -1;
            var valuesLength = (values != null) ? values.Length : -1;

            if (keysLength != valuesLength)
                throw new ArgumentException("keys length is not equal to values length", "keys");

            if (keysLength < 0)
                return null;

            if (keysLength == 0)
                return new byte[0][];

            var result = new byte[2 * keysLength][];

            for (int i = 0, index = 0; i < keysLength; i++)
            {
                result[index++] = keys[i];

                var s = values[i];
                result[index++] = (s != null) ? Encoding.UTF8.GetBytes(s) : null;
            }
            return result;
        }

        internal static byte[][] Merge(this string[] keys, byte[][] values)
        {
            var keysLength = (keys != null) ? keys.Length : -1;
            var valuesLength = (values != null) ? values.Length : -1;

            if (keysLength != valuesLength)
                throw new ArgumentException("keys length is not equal to values length", "keys");

            if (keysLength < 0)
                return null;

            if (keysLength == 0)
                return new byte[0][];

            var result = new byte[2 * keysLength][];

            for (int i = 0, index = 0; i < keysLength; i++)
            {
                var s = keys[i];

                result[index++] = (s != null) ? Encoding.UTF8.GetBytes(s) : null;
                result[index++] = values[i];
            }
            return result;
        }

        internal static byte[][] Merge(this string[] keys, string[] values)
        {
            var keysLength = (keys != null) ? keys.Length : -1;
            var valuesLength = (values != null) ? values.Length : -1;

            if (keysLength != valuesLength)
                throw new ArgumentException("keys length is not equal to values length", "keys");

            if (keysLength < 0)
                return null;

            if (keysLength == 0)
                return new byte[0][];

            var result = new byte[2 * keysLength][];

            for (int i = 0, index = 0; i < keysLength; i++)
            {
                var s = keys[i];
                result[index++] = (s != null) ? Encoding.UTF8.GetBytes(s) : null;

                s = values[i];
                result[index++] = (s != null) ? Encoding.UTF8.GetBytes(s) : null;
            }
            return result;
        }

        #endregion Merge

        #region Join

        internal static byte[][] Join(this RedisParam[] values1, RedisParam[] values2)
        {
            var values1Length = (values1 != null) ? values1.Length : -1;
            var values2Length = (values2 != null) ? values2.Length : -1;

            if (values1Length < 0 && values2Length < 0)
                return null;

            if (values1Length == 0 && values2Length == 0)
                return new byte[0][];

            values1Length = Math.Max(0, values1Length);
            values2Length = Math.Max(0, values2Length);

            var resultLen = values1Length + values2Length;
            var result = new byte[resultLen][];

            var i = 0;
            for (; i < values1Length; i++)
                result[i] = values1[i].Data;

            for (; i < resultLen; i += 2)
                result[i] = values2[i - values1Length].Data;
            return result;
        }

        internal static byte[][] Join(this RedisParam[] values1, byte[][] values2)
        {
            var values1Length = (values1 != null) ? values1.Length : -1;
            var values2Length = (values2 != null) ? values2.Length : -1;

            if (values1Length < 0 && values2Length < 0)
                return null;

            if (values1Length == 0 && values2Length == 0)
                return new byte[0][];

            values1Length = Math.Max(0, values1Length);
            values2Length = Math.Max(0, values2Length);

            var resultLen = values1Length + values2Length;
            var result = new byte[resultLen][];

            var i = 0;
            for (; i < values1Length; i++)
                result[i] = values1[i].Data;

            for (; i < resultLen; i += 2)
                result[i] = values2[i - values1Length];
            return result;
        }

        internal static byte[][] Join(this byte[][] values1, RedisParam[] values2)
        {
            var values1Length = (values1 != null) ? values1.Length : -1;
            var values2Length = (values2 != null) ? values2.Length : -1;

            if (values1Length < 0 && values2Length < 0)
                return null;

            if (values1Length == 0 && values2Length == 0)
                return new byte[0][];

            values1Length = Math.Max(0, values1Length);
            values2Length = Math.Max(0, values2Length);

            var resultLen = values1Length + values2Length;
            var result = new byte[resultLen][];

            var i = 0;
            for (; i < values1Length; i++)
                result[i] = values1[i];

            for (; i < resultLen; i += 2)
                result[i] = values2[i - values1Length].Data;
            return result;
        }


        internal static byte[][] Join(this byte[][] values1, byte[][] values2)
        {
            var values1Length = (values1 != null) ? values1.Length : -1;
            var values2Length = (values2 != null) ? values2.Length : -1;

            if (values1Length < 0 && values2Length < 0)
                return null;

            if (values1Length == 0 && values2Length == 0)
                return new byte[0][];

            values1Length = Math.Max(0, values1Length);
            values2Length = Math.Max(0, values2Length);

            var resultLen = values1Length + values2Length;
            var result = new byte[resultLen][];

            var i = 0;
            for (; i < values1Length; i++)
                result[i] = values1[i];

            for (; i < resultLen; i += 2)
                result[i] = values2[i - values1Length];
            return result;
        }

        internal static byte[][] Join(this RedisParam value, byte[][] values)
        {
            var valueLength = !value.IsNull ? value.Length : -1;
            var valuesLength = (values != null) ? values.Length : -1;

            if (valueLength < 0 && valuesLength < 0)
                return new byte[][] { value };

            if (valueLength == 0 && valuesLength == 0)
                return new byte[][] { value };

            valueLength = Math.Max(0, valueLength);
            valuesLength = Math.Max(0, valuesLength);

            var resultLength = 1 + valuesLength;
            var result = new byte[resultLength][];

            result[0] = value.Data;

            for (var i = 1; i < resultLength; i++)
                result[i] = values[i - 1];
            return result;
        }

        internal static byte[][] Join(this byte[] value, byte[][] values)
        {
            var valueLength = (value != null) ? value.Length : -1;
            var valuesLength = (values != null) ? values.Length : -1;

            if (valueLength < 0 && valuesLength < 0)
                return new byte[][] { value };

            if (valueLength == 0 && valuesLength == 0)
                return new byte[][] { value };

            valueLength = Math.Max(0, valueLength);
            valuesLength = Math.Max(0, valuesLength);

            var resultLength = 1 + valuesLength;
            var result = new byte[resultLength][];

            result[0] = value;

            for (var i = 1; i < resultLength; i++)
                result[i] = values[i - 1];
            return result;
        }

        internal static byte[][] Join(this byte[][] values, byte[] value)
        {
            var valueLength = (value != null) ? value.Length : -1;
            var valuesLength = (values != null) ? values.Length : -1;

            if (valueLength < 0 && valuesLength < 0)
                return new byte[][] { value };

            if (valueLength == 0 && valuesLength == 0)
                return new byte[][] { value };

            valueLength = Math.Max(0, valueLength);
            valuesLength = Math.Max(0, valuesLength);

            var resultLength = 1 + valuesLength;
            var result = new byte[resultLength][];

            result[resultLength - 1] = value;

            for (var i = 0; i < valuesLength; i++)
                result[i] = values[i];
            return result;
        }

        internal static byte[][] Join(this RedisParam value, string[] values)
        {
            var valueLength = !value.IsNull ? value.Length : -1;
            var valuesLength = (values != null) ? values.Length : -1;

            if (valueLength < 0 && valuesLength < 0)
                return new byte[][] { value };

            if (valueLength == 0 && valuesLength == 0)
                return new byte[][] { value };

            valueLength = Math.Max(0, valueLength);
            valuesLength = Math.Max(0, valuesLength);

            var resultLength = 1 + valuesLength;
            var result = new byte[resultLength][];

            result[0] = value;

            for (var i = 1; i < resultLength; i++)
            {
                var s = values[i - 1];
                if (s != null)
                    result[i] = Encoding.UTF8.GetBytes(s);
            }
            return result;
        }

        internal static byte[][] Join(this RedisParam value, RedisParam[] values)
        {
            var valueLength = !value.IsNull ? value.Length : -1;
            var valuesLength = (values != null) ? values.Length : -1;

            if (valueLength < 0 && valuesLength < 0)
                return new byte[][] { value };

            if (valueLength == 0 && valuesLength == 0)
                return new byte[][] { value };

            valueLength = Math.Max(0, valueLength);
            valuesLength = Math.Max(0, valuesLength);

            var resultLength = 1 + valuesLength;
            var result = new byte[resultLength][];

            result[0] = value;

            for (var i = 1; i < resultLength; i++)
                result[i] = values[i - 1].Data;

            return result;
        }

        internal static byte[][] Join(this byte[] value, string[] values)
        {
            var valueLength = (value != null) ? value.Length : -1;
            var valuesLength = (values != null) ? values.Length : -1;

            if (valueLength < 0 && valuesLength < 0)
                return new byte[][] { value };

            if (valueLength == 0 && valuesLength == 0)
                return new byte[][] { value };

            valueLength = Math.Max(0, valueLength);
            valuesLength = Math.Max(0, valuesLength);

            var resultLength = 1 + valuesLength;
            var result = new byte[resultLength][];

            result[0] = value;

            for (var i = 1; i < resultLength; i++)
            {
                var s = values[i - 1];
                if (s != null)
                    result[i] = Encoding.UTF8.GetBytes(s);
            }
            return result;
        }

        internal static byte[][] Join(this RedisParam[] values1, string[] values2)
        {
            var values1Length = (values1 != null) ? values1.Length : -1;
            var values2Length = (values2 != null) ? values2.Length : -1;

            if (values1Length < 0 && values2Length < 0)
                return null;

            if (values1Length == 0 && values2Length == 0)
                return new byte[0][];

            values1Length = Math.Max(0, values1Length);
            values2Length = Math.Max(0, values2Length);

            var resultLength = values1Length + values2Length;
            var result = new byte[resultLength][];

            if (values1Length > 0)
            {
                for (var i = 0; i < values1Length; i++)
                    result[i] = values1[i].Data;
            }

            for (var i = values1Length; i < resultLength; i++)
            {
                var s = values2[i - values1Length];
                if (s != null)
                    result[i] = Encoding.UTF8.GetBytes(s);
            }
            return result;
        }

        internal static byte[][] Join(this byte[][] values1, string[] values2)
        {
            var values1Length = (values1 != null) ? values1.Length : -1;
            var values2Length = (values2 != null) ? values2.Length : -1;

            if (values1Length < 0 && values2Length < 0)
                return null;

            if (values1Length == 0 && values2Length == 0)
                return new byte[0][];

            values1Length = Math.Max(0, values1Length);
            values2Length = Math.Max(0, values2Length);

            var resultLength = values1Length + values2Length;
            var result = new byte[resultLength][];

            if (values1Length > 0)
                Array.Copy(result, values1, values1Length);

            for (var i = values1Length; i < resultLength; i++)
            {
                var s = values2[i - values1Length];
                if (s != null)
                    result[i] = Encoding.UTF8.GetBytes(s);
            }
            return result;
        }

        internal static byte[][] Join(this byte[] value1, byte[] value2)
        {
            return new byte[2][] { value1, value2 };
        }

        internal static byte[][] Join(this RedisParam value1, byte[] value2)
        {
            return new byte[2][] { value1.Data, value2 };
        }

        internal static byte[][] Join(this RedisParam value1, RedisParam value2)
        {
            return new byte[2][] { value1.Data, value2.Data };
        }

        internal static byte[][] Join(this string[] values1, RedisParam[] values2)
        {
            var values1Length = (values1 != null) ? values1.Length : -1;
            var values2Length = (values2 != null) ? values2.Length : -1;

            if (values1Length < 0 && values2Length < 0)
                return null;

            if (values1Length == 0 && values2Length == 0)
                return new byte[0][];

            values1Length = Math.Max(0, values1Length);
            values2Length = Math.Max(0, values2Length);

            var resultLength = values1Length + values2Length;
            var result = new byte[resultLength][];

            var i = 0;
            for (; i < values1Length; i++)
            {
                var s = values1[i];
                if (s != null)
                    result[i] = Encoding.UTF8.GetBytes(s);
            }

            for (; i < resultLength; i++)
                result[i] = values2[i - values1Length].Data;
            return result;

        }

        internal static byte[][] Join(this string[] values1, string[] values2)
        {
            var values1Length = (values1 != null) ? values1.Length : -1;
            var values2Length = (values2 != null) ? values2.Length : -1;

            if (values1Length < 0 && values2Length < 0)
                return null;

            if (values1Length == 0 && values2Length == 0)
                return new byte[0][];

            values1Length = Math.Max(0, values1Length);
            values2Length = Math.Max(0, values2Length);

            var resultLength = values1Length + values2Length;
            var result = new byte[resultLength][];

            var i = 0;
            for (; i < values1Length; i++)
            {
                var s = values1[i];
                if (s != null)
                    result[i] = Encoding.UTF8.GetBytes(s);
            }

            for (; i < resultLength; i++)
            {
                var s = values2[i - values1Length];
                if (s != null)
                    result[i] = Encoding.UTF8.GetBytes(s);
            }
            return result;

        }

        #endregion Join

        #region Commands

        internal static bool IsDbRequiredCommand(this byte[] cmd)
        {
            if (!cmd.IsEmpty())
                return !RedisConstants.CommandsNotRequireDB.Contains(cmd);
            return false;
        }

        internal static bool IsUpdateCommand(this byte[] cmd)
        {
            if (!cmd.IsEmpty())
                return RedisConstants.CommandsThatUpdate.Contains(cmd);
            return false;
        }

        internal static bool IsAnyRoleCommand(this byte[] cmd)
        {
            if (!cmd.IsEmpty())
                return RedisConstants.CommandsForAnyRole.Contains(cmd);
            return false;
        }

        internal static RedisRole CommandRole(this byte[] cmd)
        {
            if (cmd.IsEmpty())
                return RedisRole.Undefined;
            if (cmd == RedisCommandList.Sentinel)
                return RedisRole.Sentinel;
            if (cmd.IsAnyRoleCommand())
                return RedisRole.Any;
            if (cmd.IsUpdateCommand())
                return RedisRole.Master;
            return RedisRole.Slave;
        }

        #endregion Commands

        #region Socket

        internal static bool IsConnected(this Socket socket, int poll = -1)
        {
            if (socket != null && socket.Connected)
            {
                if (poll > -1)
                    return !(socket.Poll(poll, SelectMode.SelectRead) && (socket.Available == 0));
                return true;
            }
            return false;
        }

        internal static bool IsConnected(this RedisSocket socket, int poll = -1)
        {
            if (socket != null && socket.Connected)
            {
                if (poll > -1)
                    return !(socket.Poll(poll, SelectMode.SelectRead) && (socket.Available == 0));
                return true;
            }
            return false;
        }

        internal static void DisposeSocket(this Socket socket)
        {
            if (socket != null)
            {
                Action<object> action = (state) =>
                {
                    var sck = state as Socket;
                    if (sck != null)
                    {
                        try
                        {
                            sck.Shutdown(SocketShutdown.Both); 
                            if (sck.IsConnected(10))
                                sck.Close();
                        }
                        catch (Exception)
                        { }
                        try
                        {
                            sck.Dispose();
                        }
                        catch (Exception)
                        { }
                    }
                };

                try
                {
                    var eventQ = RedisEventQueue.Default;
                    if (!eventQ.IsAlive())
                        action(socket);
                    else
                        eventQ.Enqueu(action, socket);
                }
                catch (Exception)
                { }
            }
        }

        internal static void DisposeSocket(this RedisSocket socket)
        {
            if (socket != null)
            {
                Action<object> action = (state) =>
                {
                    var sck = state as RedisSocket;
                    if (sck != null)
                    {
                        try
                        {
                            sck.Shutdown(SocketShutdown.Both); 
                            if (sck.IsConnected(10))
                                sck.Close();
                        }
                        catch (Exception)
                        { }
                        try
                        {
                            sck.Dispose();
                        }
                        catch (Exception)
                        { }
                    }
                };

                try
                {
                    var eventQ = RedisEventQueue.Default;
                    if (!eventQ.IsAlive())
                        action(socket);
                    else
                        eventQ.Enqueu(action, socket);
                }
                catch (Exception)
                { }
            }
        }

        #endregion Socket

        #endregion Methods
    }
}
