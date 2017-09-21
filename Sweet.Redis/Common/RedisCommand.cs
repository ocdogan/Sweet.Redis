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

        private int m_Db;
        private byte[] m_Command;
        private byte[][] m_Arguments;

        #endregion Field Members

        #region .Ctors

        public RedisCommand(int db, byte[] command, params byte[][] args)
        {
            if (command == null)
                throw new ArgumentNullException("command");

            m_Db = db;
            m_Command = command;
            m_Arguments = args;
        }

        #endregion .Ctors

        #region Destructors

        protected override void OnDispose(bool disposing)
        {
            m_Arguments = null;
        }

        #endregion Destructors

        #region Properties

        public int Db { get { return m_Db; } }

        public byte[] Command { get { return m_Command; } }

        public byte[][] Arguments { get { return m_Arguments; } }

        #endregion Properties

        #region Methods

        private byte[] PrepareData()
        {
            using (var buffer = new RedisChunkBuffer(RedisConstants.ReadBufferSize))
            {
                WriteTo(buffer);
                return buffer.ReleaseBuffer();
            }
        }

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

        private static long? ForNullableInteger(IRedisResponse response, bool throwException)
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

        public RedisDouble ExpectDouble(IRedisConnection connection, bool throwException = true)
        {
            using (var response = ExecuteInternal(connection, throwException))
            {
                return ForDouble(response, throwException);
            }
        }

        private static double ForDouble(IRedisResponse response, bool throwException)
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

        private static string[] ForMutiDataStrings(IRedisResponse response, bool throwException)
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

        public RedisMultiBytes ExpectMultiDataBytes(IRedisConnection connection, bool throwException = true)
        {
            using (var response = ExecuteInternal(connection, throwException))
            {
                return ForMultiDataBytes(response, throwException);
            }
        }

        private static byte[][] ForMultiDataBytes(IRedisResponse response, bool throwException)
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

        public IRedisResponse Execute(IRedisConnection connection, bool throwException = true)
        {
            if (connection == null)
                throw new ArgumentNullException("connection");

            ValidateNotDisposed();

            return ExecuteInternal(connection, throwException);
        }

        private IRedisResponse ExecuteInternal(IRedisConnection connection, bool throwException = true)
        {
            var data = PrepareData();
            var response = connection.SendReceive(this);
            if (response == null && throwException)
                throw new RedisException("Corrupted redis response data");
            HandleError(response);

            return response;
        }

        private static void HandleError(IRedisResponse response)
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

        private void WriteTo(IRedisWriter writer)
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

        public void WriteTo(Stream stream)
        {
            if (stream == null)
                throw new ArgumentNullException("stream");

            if (!stream.CanWrite)
                throw new ArgumentException("Can not write to closed stream", "stream");

            using (var writer = new RedisStreamWriter(stream, true))
            {
                WriteTo(writer);
                stream.Flush();
            }
        }

        public void WriteTo(RedisSocket socket)
        {
            if (socket == null)
                throw new ArgumentNullException("socket");

            WriteTo(socket.GetWriteStream());
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
                    stream.Flush();
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
            return action.InvokeAsync(socket.GetWriteStream());
        }

        #endregion Methods
    }
}
