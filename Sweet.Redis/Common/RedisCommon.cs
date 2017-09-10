﻿using System;
using System.Net.Sockets;
using System.Text;

namespace Sweet.Redis
{
    public static class RedisCommon
    {
        #region Static Members

        private static readonly DateTime UnixBaseTimeStamp = new DateTime(1970, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc);

        #endregion Static Members

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

        #region Methods

        internal static bool IsConnected(this Socket socket, int poll = -1)
        {
            if (socket == null || !socket.Connected)
                return false;
            return !((poll > -1) && socket.Poll(poll, SelectMode.SelectRead) && (socket.Available == 0));
        }

        internal static void DisposeSocket(this Socket socket)
        {
            if (socket != null && socket.IsBound)
            {
                if (!socket.Connected)
                {
                    try
                    {
                        socket.Dispose();
                    }
                    catch (Exception)
                    { }
                }
                else
                {
                    socket.DisconnectAsync(false).ContinueWith(_ =>
                    {
                        if (socket != null)
                            socket.Dispose();
                    });
                }
            }
        }

        internal static RedisObjectType ResponseType(this byte b)
        {
            switch (b)
            {
                case (byte)'+':
                    return RedisObjectType.SimpleString;
                case (byte)'-':
                    return RedisObjectType.Error;
                case (byte)'$':
                    return RedisObjectType.BulkString;
                case (byte)':':
                    return RedisObjectType.Integer;
                case (byte)'*':
                    return RedisObjectType.Array;
                default:
                    return RedisObjectType.Undefined;
            }
        }

        internal static byte ResponseTypeByte(this RedisObjectType b)
        {
            switch (b)
            {
                case RedisObjectType.SimpleString:
                    return (byte)'+';
                case RedisObjectType.Error:
                    return (byte)'-';
                case RedisObjectType.BulkString:
                    return (byte)'$';
                case RedisObjectType.Integer:
                    return (byte)':';
                case RedisObjectType.Array:
                    return (byte)'*';
                default:
                    return (byte)'?';
            }
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
                    case TypeCode.String:
                        return Encoding.UTF8.GetBytes((string)obj);
                    case TypeCode.Int32:
                        return Encoding.UTF8.GetBytes(((int)obj).ToString(RedisConstants.InvariantCulture));
                    case TypeCode.Int64:
                        return Encoding.UTF8.GetBytes(((long)obj).ToString(RedisConstants.InvariantCulture));
                    case TypeCode.Object:
                        return Encoding.UTF8.GetBytes(obj.ToString());
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
                }
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

        internal static byte[][] ToBytesArray(this byte[] bytes)
        {
            if (bytes != null)
                return new byte[1][] { bytes };
            return null;
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

        internal static byte[][] ConvertToByteArray(this string[] keys)
        {
            if (keys == null)
                throw new ArgumentNullException("keys");

            var keysLength = keys.Length;
            if (keysLength == 0)
                throw new ArgumentNullException("keys");

            var result = new byte[keysLength][];
            for (var i = 0; i < keysLength; i++)
            {
                result[i] = keys[i].ToBytes();
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

            var resultLen = 2 * keysLength;

            var result = new byte[resultLen][];
            for (var i = 0; i < resultLen; i += 2)
            {
                result[i] = keys[i];
                result[i + 1] = values[i];
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

            var resultLen = 2 * keysLength;

            var result = new byte[resultLen][];
            for (var i = 0; i < resultLen; i += 2)
            {
                result[i] = keys[i];

                var s = values[i];
                if (s != null)
                    result[i + 1] = Encoding.UTF8.GetBytes(s);
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

            var resultLen = 2 * keysLength;

            var result = new byte[resultLen][];
            for (var i = 0; i < resultLen; i += 2)
            {
                var s = keys[i];
                if (s != null)
                    result[i] = Encoding.UTF8.GetBytes(s);

                result[i + 1] = values[i];
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

            var resultLen = 2 * keysLength;

            var result = new byte[resultLen][];
            for (var i = 0; i < resultLen; i += 2)
            {
                var s = keys[i];
                if (s != null)
                    result[i] = Encoding.UTF8.GetBytes(s);

                s = values[i];
                if (s != null)
                    result[i + 1] = Encoding.UTF8.GetBytes(s);
            }
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

        internal static byte[][] Join(this byte[] value, byte[][] values)
        {
            var valueLength = (value != null) ? value.Length : -1;
            var valuesLength = (values != null) ? values.Length : -1;

            if (valueLength < 0 && valuesLength < 0)
                return new byte[1][] { value };

            if (valueLength == 0 && valuesLength == 0)
                return new byte[1][] { value };

            valueLength = Math.Max(0, valueLength);
            valuesLength = Math.Max(0, valuesLength);

            var resultLength = 1 + valuesLength;
            var result = new byte[resultLength][];

            result[0] = value;

            for (var i = 1; i < resultLength; i++)
                result[i] = values[i];
            return result;
        }

        internal static byte[][] Join(this byte[][] values, byte[] value)
        {
            var valueLength = (value != null) ? value.Length : -1;
            var valuesLength = (values != null) ? values.Length : -1;

            if (valueLength < 0 && valuesLength < 0)
                return new byte[1][] { value };

            if (valueLength == 0 && valuesLength == 0)
                return new byte[1][] { value };

            valueLength = Math.Max(0, valueLength);
            valuesLength = Math.Max(0, valuesLength);

            var resultLength = 1 + valuesLength;
            var result = new byte[resultLength][];

            result[resultLength - 1] = value;

            for (var i = 0; i < valuesLength; i++)
                result[i] = values[i];
            return result;
        }

        internal static byte[][] Join(this byte[] value, string[] values)
        {
            var valueLength = (value != null) ? value.Length : -1;
            var valuesLength = (values != null) ? values.Length : -1;

            if (valueLength < 0 && valuesLength < 0)
                return new byte[1][] { value };

            if (valueLength == 0 && valuesLength == 0)
                return new byte[1][] { value };

            valueLength = Math.Max(0, valueLength);
            valuesLength = Math.Max(0, valuesLength);

            var resultLength = 1 + valuesLength;
            var result = new byte[resultLength][];

            result[0] = value;

            for (var i = 1; i < resultLength; i++)
            {
                var s = values[i];
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

        internal static int ToInt(this string s, int defaultValue = int.MinValue)
        {
            if (!String.IsNullOrEmpty(s))
            {
                int result;
                if (int.TryParse(s, out result))
                    return result;
            }
            return defaultValue;
        }

        internal static long ToLong(this string s, long defaultValue = long.MinValue)
        {
            if (!String.IsNullOrEmpty(s))
            {
                long result;
                if (long.TryParse(s, out result))
                    return result;
            }
            return defaultValue;
        }

        #endregion Methods
    }
}
