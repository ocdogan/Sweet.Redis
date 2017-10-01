#region License
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
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading.Tasks;

namespace Sweet.Redis
{
    internal class RedisCommand : RedisDisposable, IRedisCommand
    {
        #region Field Members

        private int m_DbIndex;
        private byte[] m_Command;
        private byte[][] m_Arguments;
        private RedisCommandType m_CommandType;

        #endregion Field Members

        #region .Ctors

        public RedisCommand(int db, byte[] command, RedisCommandType commandType = RedisCommandType.SendAndReceive,
                            params byte[][] args)
        {
            if (command == null)
                throw new ArgumentNullException("command");

            m_Arguments = args;
            m_Command = command;
            m_CommandType = commandType;
            m_DbIndex = db;
        }

        #endregion .Ctors

        #region Destructors

        protected override void OnDispose(bool disposing)
        {
            m_Arguments = null;
        }

        #endregion Destructors

        #region Properties

        public byte[][] Arguments { get { return m_Arguments; } }

        public byte[] Command { get { return m_Command; } }

        public RedisCommandType CommandType { get { return m_CommandType; } }

        public int DbIndex { get { return m_DbIndex; } }

        #endregion Properties

        #region Methods

        #region IRedisConnection Execution Methods

        public RedisBool ExpectSimpleString(IRedisConnection connection, string expectedResult, bool throwException = true)
        {
            var result = ExpectSimpleStringInternal(connection, throwException);
            if (!String.IsNullOrEmpty(result))
            {
                if (!String.IsNullOrEmpty(expectedResult))
                    return result.Equals(expectedResult, StringComparison.OrdinalIgnoreCase);

                if (result.StartsWith("-", StringComparison.Ordinal))
                    return false;

                return true;
            }
            return false;
        }

        public RedisString ExpectSimpleString(IRedisConnection connection, bool throwException = true)
        {
            return ExpectSimpleStringInternal(connection, throwException);
        }

        private string ExpectSimpleStringInternal(IRedisConnection connection, bool throwException = true)
        {
            var bytes = ExpectSimpleStringBytes(connection, throwException);
            if (bytes == null)
                return null;
            return Encoding.UTF8.GetString(bytes);
        }

        public RedisBool ExpectSimpleStringBytes(IRedisConnection connection, byte[] expectedResult, bool throwException = true)
        {
            var result = ExpectSimpleStringBytesInternal(connection, throwException);
            if (result == null)
                return expectedResult == null;

            if (expectedResult != null)
                return result == expectedResult;

            if (result.Length > 0 && result[0] == (byte)'-')
                return false;

            return false;
        }

        public RedisVoid ExpectNothing(IRedisConnection connection, bool throwException = true)
        {
            ExecuteInternal(connection, throwException, true);
            return new RedisVoid(true);
        }

        public RedisBytes ExpectSimpleStringBytes(IRedisConnection connection, bool throwException = true)
        {
            return ExpectSimpleStringBytesInternal(connection, throwException);
        }

        private byte[] ExpectSimpleStringBytesInternal(IRedisConnection connection, bool throwException = true)
        {
            using (var response = ExecuteInternal(connection, throwException))
            {
                if (response == null)
                {
                    if (throwException)
                        throw new RedisException("No data returned");
                    return null;
                }

                if (response.Type != RedisRawObjType.SimpleString)
                {
                    if (throwException)
                        throw new RedisException("Invalid data returned");
                    return null;
                }
                return response.ReleaseData();
            }
        }

        public RedisString ExpectBulkString(IRedisConnection connection, bool throwException = true)
        {
            var bytes = ExpectBulkStringBytes(connection, throwException);
            if (bytes == null)
                return null;
            return Encoding.UTF8.GetString(bytes);
        }

        public RedisBytes ExpectBulkStringBytes(IRedisConnection connection, bool throwException = true)
        {
            using (var response = ExecuteInternal(connection, throwException))
            {
                if (response == null)
                {
                    if (throwException)
                        throw new RedisException("No data returned");
                    return null;
                }

                if (response.Type != RedisRawObjType.BulkString)
                {
                    if (throwException)
                        throw new RedisException("Invalid data returned");
                    return null;
                }
                return response.ReleaseData();
            }
        }

        public RedisInt ExpectInteger(IRedisConnection connection, bool throwException = true)
        {
            var result = ExpectNullableInteger(connection, throwException);
            if (result == null)
                return long.MinValue;
            return result.Value;
        }

        public RedisNullableInt ExpectNullableInteger(IRedisConnection connection, bool throwException = true)
        {
            using (var response = ExecuteInternal(connection, throwException))
            {
                return ForNullableInteger(response, throwException);
            }
        }

        public RedisDouble ExpectDouble(IRedisConnection connection, bool throwException = true)
        {
            using (var response = ExecuteInternal(connection, throwException))
            {
                return ForDouble(response, throwException);
            }
        }

        public RedisNullableDouble ExpectNullableDouble(IRedisConnection connection, bool throwException = true)
        {
            using (var response = ExecuteInternal(connection, throwException))
            {
                return ForNullableDouble(response, throwException);
            }
        }

        public RedisRaw ExpectArray(IRedisConnection connection, bool throwException = true)
        {
            using (var response = ExecuteInternal(connection, throwException))
            {
                if (response == null)
                {
                    if (throwException)
                        throw new RedisException("No data returned");
                    return null;
                }
                return new RedisRaw(RedisRawObj.ToObject(response));
            }
        }

        public RedisMultiString ExpectMultiDataStrings(IRedisConnection connection, bool throwException = true)
        {
            using (var response = ExecuteInternal(connection, throwException))
            {
                return ForMutiDataStrings(response, throwException);
            }
        }

        public RedisMultiBytes ExpectMultiDataBytes(IRedisConnection connection, bool throwException = true)
        {
            using (var response = ExecuteInternal(connection, throwException))
            {
                return ForMultiDataBytes(response, throwException);
            }
        }

        public IRedisResponse Execute(IRedisConnection connection, bool throwException = true)
        {
            ValidateNotDisposed();
            return ExecuteInternal(connection, throwException);
        }

        private IRedisResponse ExecuteInternal(IRedisConnection connection, bool throwException = true, bool sendNotReceive = false)
        {
            if (connection == null)
            {
                if (throwException)
                    throw new ArgumentNullException("connection");
                return null;
            }

            IRedisResponse response = RedisResponse.Void;
            if (sendNotReceive || !m_CommandType.HasFlag(RedisCommandType.SendAndReceive))
                connection.Send(this);
            else
            {
                response = connection.SendReceive(this);

                if (response == null && throwException)
                    throw new RedisException("Corrupted redis response data");
                HandleError(response);
            }
            return response;
        }

        #endregion IRedisConnection Execution Methods

        #region RedisSocket Execution Methods

        public RedisBool ExpectSimpleString(RedisSocket socket, RedisSettings settings, string expectedResult, bool throwException = true)
        {
            var result = ExpectSimpleStringInternal(socket, settings, throwException);
            if (!String.IsNullOrEmpty(result))
            {
                if (!String.IsNullOrEmpty(expectedResult))
                    return result.Equals(expectedResult, StringComparison.OrdinalIgnoreCase);

                if (result.StartsWith("-", StringComparison.Ordinal))
                    return false;

                return true;
            }
            return false;
        }

        public RedisString ExpectSimpleString(RedisSocket socket, RedisSettings settings, bool throwException = true)
        {
            return ExpectSimpleStringInternal(socket, settings, throwException);
        }

        private string ExpectSimpleStringInternal(RedisSocket socket, RedisSettings settings, bool throwException = true)
        {
            var bytes = ExpectSimpleStringBytes(socket, settings, throwException);
            if (bytes == null)
                return null;
            return Encoding.UTF8.GetString(bytes);
        }

        public RedisBool ExpectSimpleStringBytes(RedisSocket socket, RedisSettings settings, byte[] expectedResult, bool throwException = true)
        {
            var result = ExpectSimpleStringBytesInternal(socket, settings, throwException);
            if (result == null)
                return expectedResult == null;

            if (expectedResult != null)
                return result == expectedResult;

            if (result.Length > 0 && result[0] == (byte)'-')
                return false;

            return false;
        }

        public RedisVoid ExpectNothing(RedisSocket socket, RedisSettings settings, bool throwException = true)
        {
            ExecuteInternal(socket, settings, throwException, true);
            return new RedisVoid(true);
        }

        public RedisBytes ExpectSimpleStringBytes(RedisSocket socket, RedisSettings settings, bool throwException = true)
        {
            return ExpectSimpleStringBytesInternal(socket, settings, throwException);
        }

        private byte[] ExpectSimpleStringBytesInternal(RedisSocket socket, RedisSettings settings, bool throwException = true)
        {
            using (var response = ExecuteInternal(socket, settings, throwException))
            {
                if (response == null)
                {
                    if (throwException)
                        throw new RedisException("No data returned");
                    return null;
                }

                if (response.Type != RedisRawObjType.SimpleString)
                {
                    if (throwException)
                        throw new RedisException("Invalid data returned");
                    return null;
                }
                return response.ReleaseData();
            }
        }

        public RedisString ExpectBulkString(RedisSocket socket, RedisSettings settings, bool throwException = true)
        {
            var bytes = ExpectBulkStringBytes(socket, settings, throwException);
            if (bytes == null)
                return null;
            return Encoding.UTF8.GetString(bytes);
        }

        public RedisBytes ExpectBulkStringBytes(RedisSocket socket, RedisSettings settings, bool throwException = true)
        {
            using (var response = ExecuteInternal(socket, settings, throwException))
            {
                if (response == null)
                {
                    if (throwException)
                        throw new RedisException("No data returned");
                    return null;
                }

                if (response.Type != RedisRawObjType.BulkString)
                {
                    if (throwException)
                        throw new RedisException("Invalid data returned");
                    return null;
                }
                return response.ReleaseData();
            }
        }

        public RedisInt ExpectInteger(RedisSocket socket, RedisSettings settings, bool throwException = true)
        {
            var result = ExpectNullableInteger(socket, settings, throwException);
            if (result == null)
                return long.MinValue;
            return result.Value;
        }

        public RedisNullableInt ExpectNullableInteger(RedisSocket socket, RedisSettings settings, bool throwException = true)
        {
            using (var response = ExecuteInternal(socket, settings, throwException))
            {
                return ForNullableInteger(response, throwException);
            }
        }

        public RedisDouble ExpectDouble(RedisSocket socket, RedisSettings settings, bool throwException = true)
        {
            using (var response = ExecuteInternal(socket, settings, throwException))
            {
                return ForDouble(response, throwException);
            }
        }

        public RedisNullableDouble ExpectNullableDouble(RedisSocket socket, RedisSettings settings, bool throwException = true)
        {
            using (var response = ExecuteInternal(socket, settings, throwException))
            {
                return ForNullableDouble(response, throwException);
            }
        }

        public RedisRaw ExpectArray(RedisSocket socket, RedisSettings settings, bool throwException = true)
        {
            using (var response = ExecuteInternal(socket, settings, throwException))
            {
                if (response == null)
                {
                    if (throwException)
                        throw new RedisException("No data returned");
                    return null;
                }
                return new RedisRaw(RedisRawObj.ToObject(response));
            }
        }

        public RedisMultiString ExpectMultiDataStrings(RedisSocket socket, RedisSettings settings, bool throwException = true)
        {
            using (var response = ExecuteInternal(socket, settings, throwException))
            {
                return ForMutiDataStrings(response, throwException);
            }
        }

        public RedisMultiBytes ExpectMultiDataBytes(RedisSocket socket, RedisSettings settings, bool throwException = true)
        {
            using (var response = ExecuteInternal(socket, settings, throwException))
            {
                return ForMultiDataBytes(response, throwException);
            }
        }

        public IRedisResponse Execute(RedisSocket socket, RedisSettings settings, bool throwException = true)
        {
            ValidateNotDisposed();
            return ExecuteInternal(socket, settings, throwException);
        }

        private IRedisResponse ExecuteInternal(RedisSocket socket, RedisSettings settings, bool throwException = true, bool sendNotReceive = false)
        {
            if (socket == null)
            {
                if (throwException)
                    throw new ArgumentNullException("socket");
                return null;
            }

            IRedisResponse response = RedisResponse.Void;
            if (sendNotReceive || !m_CommandType.HasFlag(RedisCommandType.SendAndReceive))
                WriteTo(socket);
            else
            {
                WriteTo(socket);
                using (var reader = new RedisSingleResponseReader(settings))
                    response = reader.Execute(socket);

                if (response == null && throwException)
                    throw new RedisException("Corrupted redis response data");
                HandleError(response);
            }
            return response;
        }

        #endregion RedisSocket Execution Methods

        #region Common Execution Methods

        protected static double ForDouble(IRedisResponse response, bool throwException)
        {
            if (response == null)
            {
                if (throwException)
                    throw new RedisException("No data returned");
                return double.MinValue;
            }

            if (response.Type == RedisRawObjType.Array ||
                response.Type == RedisRawObjType.Undefined)
            {
                if (throwException)
                    throw new RedisException("Invalid data returned");
                return double.MinValue;
            }

            var data = response.Data;
            if (data == null || data.Length == 0)
            {
                if (throwException)
                    throw new RedisException("No data returned");
                return double.MinValue;
            }

            double result;
            if (double.TryParse(Encoding.UTF8.GetString(data), out result))
                return result;

            if (throwException)
                throw new RedisException("Not a double result");

            return double.MinValue;
        }

        protected static long? ForNullableInteger(IRedisResponse response, bool throwException)
        {
            if (response == null)
            {
                if (throwException)
                    throw new RedisException("No data returned");
                return null;
            }

            if (response.Type != RedisRawObjType.Integer)
            {
                if (throwException)
                    throw new RedisException("Invalid data returned");
                return null;
            }

            var data = response.Data;
            if (data == null || data.Length == 0)
            {
                if (throwException)
                    throw new RedisException("No data returned");
                return null;
            }

            if (data == RedisConstants.Nil)
                return null;

            long result;
            if (long.TryParse(Encoding.UTF8.GetString(data), out result))
                return result;

            if (throwException)
                throw new RedisException("Not an integer result");

            return null;
        }


        protected static double? ForNullableDouble(IRedisResponse response, bool throwException)
        {
            if (response == null)
            {
                if (throwException)
                    throw new RedisException("No data returned");
                return null;
            }

            if (response.Type == RedisRawObjType.Array ||
                response.Type == RedisRawObjType.Undefined)
            {
                if (throwException)
                    throw new RedisException("Invalid data returned");
                return null;
            }

            var data = response.Data;
            if (data == null)
                return null;

            if (data.Length == 0)
            {
                if (throwException)
                    throw new RedisException("No data returned");
                return null;
            }

            if (data == RedisConstants.Nil)
                return null;

            double result;
            if (double.TryParse(Encoding.UTF8.GetString(data), out result))
                return result;

            if (throwException)
                throw new RedisException("Not a double result");

            return null;
        }

        protected static string[] ForMutiDataStrings(IRedisResponse response, bool throwException)
        {
            if (response == null)
            {
                if (throwException)
                    throw new RedisException("No data returned");
                return null;
            }

            var data = response.Data;
            switch (response.Type)
            {
                case RedisRawObjType.SimpleString:
                case RedisRawObjType.BulkString:
                case RedisRawObjType.Integer:
                    return data != null ? new string[] { Encoding.UTF8.GetString(data) } : null;
                case RedisRawObjType.Error:
                    {
                        if (!throwException)
                            return data != null ? new string[] { Encoding.UTF8.GetString(data) } : null;
                        throw new RedisException(data != null && data.Length > 0 ? Encoding.UTF8.GetString(data) : "No data returned");
                    }
                case RedisRawObjType.Undefined:
                    if (throwException)
                        throw new RedisException("Undefined respone data");
                    return null;
                case RedisRawObjType.Array:
                    {
                        var len = response.Length;
                        if (len < 0)
                            return null;
                        if (len == 0)
                            return new string[0] { };

                        var items = response.Items;
                        if (items != null)
                        {
                            var list = new List<string>();

                            len = items.Count;
                            for (var i = 0; i < len; i++)
                            {
                                var item = items[i];
                                if (item == null)
                                    list.Add(null);
                                else
                                {
                                    if (item.Type == RedisRawObjType.Undefined)
                                    {
                                        if (throwException)
                                            throw new RedisException("Undefined respone data");
                                        return null;
                                    }

                                    if (item.Type == RedisRawObjType.Array)
                                    {
                                        if (throwException)
                                            throw new RedisException("Multi-array is not allowed for multi-data respone");
                                        return null;
                                    }

                                    data = item.Data;
                                    list.Add(data != null ? Encoding.UTF8.GetString(data) : null);
                                }
                            }
                            return list.ToArray();
                        }

                        return null;
                    }
            }
            return null;
        }

        protected static byte[][] ForMultiDataBytes(IRedisResponse response, bool throwException)
        {
            if (response == null)
            {
                if (throwException)
                    throw new RedisException("No data returned");
                return null;
            }

            var data = response.Data;
            switch (response.Type)
            {
                case RedisRawObjType.SimpleString:
                case RedisRawObjType.BulkString:
                case RedisRawObjType.Integer:
                    return data != null ? new byte[1][] { data } : null;
                case RedisRawObjType.Error:
                    {
                        if (!throwException)
                            return data != null ? new byte[1][] { data } : null;
                        throw new RedisException(data != null && data.Length > 0 ? Encoding.UTF8.GetString(data) : "No data returned");
                    }
                case RedisRawObjType.Undefined:
                    if (throwException)
                        throw new RedisException("Undefined respone data");
                    return null;
                case RedisRawObjType.Array:
                    {
                        var len = response.Length;
                        if (len < 0)
                            return null;
                        if (len == 0)
                            return new byte[0][] { };

                        var items = response.Items;
                        if (items != null)
                        {
                            var list = new List<byte[]>();

                            len = items.Count;
                            for (var i = 0; i < len; i++)
                            {
                                var item = items[i];
                                if (item == null)
                                    list.Add(null);
                                else
                                {
                                    if (item.Type == RedisRawObjType.Undefined)
                                    {
                                        if (throwException)
                                            throw new RedisException("Undefined respone data");
                                        return null;
                                    }

                                    if (item.Type == RedisRawObjType.Array)
                                    {
                                        if (throwException)
                                            throw new RedisException("Multi-array is not allowed for multi-data respone");
                                        return null;
                                    }

                                    list.Add(item.Data);
                                }
                            }
                            return list.ToArray();
                        }

                        return null;
                    }
            }
            return null;
        }

        protected static void HandleError(IRedisResponse response)
        {
            if (response != null)
            {
                if (response.Type == RedisRawObjType.Error)
                {
                    var data = response.Data;
                    throw new RedisException(data != null && data.Length > 0 ? Encoding.UTF8.GetString(data) : "No data returned");
                }

                var items = response.Items;
                if (items != null)
                    for (var i = items.Count - 1; i > -1; i--)
                        HandleError(items[i]);
            }
        }

        #endregion Common Execution Methods

        #region WriteTo Methods

        public void WriteTo(Stream stream)
        {
            if (stream == null)
                throw new ArgumentNullException("stream");

            if (!stream.CanWrite)
                throw new ArgumentException("Can not write to closed stream", "stream");

            using (var writer = new RedisStreamWriter(stream, true))
            {
                WriteTo(writer);
            }
        }

        public void WriteTo(RedisSocket socket)
        {
            if (socket == null)
                throw new ArgumentNullException("socket");

            WriteTo(socket.GetBufferedStream());
        }

        public Task WriteToAsync(Stream stream)
        {
            if (stream == null)
                throw new ArgumentNullException("stream");

            if (!stream.CanWrite)
                throw new ArgumentException("Can not write to closed stream", "stream");

            Action action = () =>
            {
                using (var writer = new RedisStreamWriter(stream, true))
                {
                    WriteTo(writer);
                }
            };
            return action.InvokeAsync();
        }

        public Task WriteToAsync(RedisSocket socket)
        {
            if (socket == null)
                throw new ArgumentNullException("socket");

            Action<Stream> action = (stream) =>
            {
                if (stream != null)
                    WriteTo(stream);
            };
            return action.InvokeAsync(socket.GetBufferedStream());
        }

        protected virtual void WriteTo(IRedisWriter writer)
        {
            var argsLength = m_Arguments != null ? m_Arguments.Length : 0;

            writer.Write((byte)'*');
            writer.Write(argsLength + 1);
            writer.Write(RedisConstants.LineEnd);
            writer.Write((byte)'$');
            writer.Write(m_Command.Length);
            writer.Write(RedisConstants.LineEnd);
            writer.Write(m_Command);
            writer.Write(RedisConstants.LineEnd);

            if (argsLength > 0)
            {
                byte[] arg;
                for (var i = 0; i < argsLength; i++)
                {
                    arg = m_Arguments[i];

                    if (arg == null)
                    {
                        writer.Write(RedisConstants.NullBulkString);
                    }
                    else if (arg.Length == 0)
                    {
                        writer.Write(RedisConstants.EmptyBulkString);
                        writer.Write(RedisConstants.LineEnd);
                    }
                    else
                    {
                        writer.Write((byte)'$');
                        writer.Write(arg.Length);
                        writer.Write(RedisConstants.LineEnd);
                        writer.Write(arg);
                    }
                    writer.Write(RedisConstants.LineEnd);
                }
            }
        }

        #endregion WriteTo Methods

        #region Overriden Methods

        public override string ToString()
        {
            var sBuilder = new StringBuilder();

            sBuilder.Append("[DbIndex=");
            sBuilder.Append(DbIndex);
            sBuilder.Append(", Command=");
            sBuilder.Append(Command != null ? Encoding.UTF8.GetString(Command) : "(nil)");
            sBuilder.Append(", Arguments=");

            var args = Arguments;
            if (args == null)
                sBuilder.Append("(nil)]");
            else
            {
                var length = args.Length;
                if (length == 0)
                    sBuilder.Append("(empty)]");
                else
                {
                    var itemLen = 0;
                    for (var i = 0; i < length; i++)
                    {
                        var item = args[i];
                        if (i > 0)
                            sBuilder.Append(", ");

                        if (args == null)
                        {
                            itemLen += 5;
                            sBuilder.Append("(nil)");
                        }
                        else if (item.Length == 0)
                        {
                            itemLen += 7;
                            sBuilder.Append("(empty)");
                        }
                        else
                        {
                            var data = Encoding.UTF8.GetString(item);

                            var len = 1000 - itemLen;
                            if (len >= data.Length)
                                sBuilder.Append(data);
                            else
                            {
                                if (len > 0)
                                    sBuilder.Append(data.Substring(len));
                                sBuilder.Append("...");
                            }

                            itemLen += data.Length;
                        }

                        if (itemLen >= 1000)
                            break;
                    }

                    sBuilder.Append(']');
                }
            }
            return sBuilder.ToString();
        }

        #endregion Overriden Methods

        #endregion Methods
    }
}
