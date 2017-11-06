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
using System.Threading;
using System.Threading.Tasks;

namespace Sweet.Redis
{
    public class RedisCommand : RedisDisposable, IRedisCommand
    {
        #region Field Members

        private RedisRole? m_Role;

        private int m_DbIndex;
        private byte[] m_Command;
        private byte[][] m_Arguments;
        private RedisCommandType m_CommandType;

        private long m_IsUpdater = RedisConstants.MinusOne;

        #endregion Field Members

        #region .Ctors

        protected internal RedisCommand(int dbIndex, byte[] command, RedisCommandType commandType = RedisCommandType.SendAndReceive,
                            params byte[][] args)
        {
            if (command == null)
                throw new ArgumentNullException("command");

            m_Arguments = args;
            m_Command = command;
            m_CommandType = commandType;
            m_DbIndex = dbIndex;
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

        public bool IsUpdater
        {
            get
            {
                var updater = Interlocked.Read(ref m_IsUpdater);
                if (updater == RedisConstants.MinusOne)
                {
                    Interlocked.Exchange(ref m_IsUpdater,
                        m_Command.IsUpdateCommand() ?
                        RedisConstants.One :
                        RedisConstants.Zero);
                }
                return updater == RedisConstants.One;
            }
        }

        public RedisRole Role
        {
            get
            {
                if (!m_Role.HasValue)
                    m_Role = m_Command.CommandRole();
                return m_Role.Value;
            }
        }

        #endregion Properties

        #region Methods

        #region Execution Methods

        #region IRedisConnection Execution Methods

        public RedisBool ExpectOK(IRedisConnection connection, bool throwException = true)
        {
            var result = ExpectSimpleStringInternal(connection, throwException);
            if (!result.IsEmpty())
                return result.Equals(RedisConstants.OK, StringComparison.OrdinalIgnoreCase);
            return false;
        }

        public RedisBool ExpectSimpleString(IRedisConnection connection, string expectedResult, bool throwException = true)
        {
            var result = ExpectSimpleStringInternal(connection, throwException);
            if (!result.IsEmpty())
            {
                if (!expectedResult.IsEmpty())
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
            if (bytes == (byte[])null)
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
                        throw new RedisException("No data returned", RedisErrorCode.CorruptResponse);
                    return null;
                }

                if (response.Type != RedisRawObjectType.SimpleString)
                {
                    if (throwException)
                        throw new RedisException("Invalid data returned", RedisErrorCode.CorruptResponse);
                    return null;
                }
                return response.ReleaseData();
            }
        }

        public RedisString ExpectBulkString(IRedisConnection connection, bool throwException = true)
        {
            var bytes = ExpectBulkStringBytes(connection, throwException);
            if (bytes == (byte[])null)
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
                        throw new RedisException("No data returned", RedisErrorCode.CorruptResponse);
                    return null;
                }

                if (response.Type != RedisRawObjectType.BulkString)
                {
                    if (throwException)
                        throw new RedisException("Invalid data returned", RedisErrorCode.CorruptResponse);
                    return null;
                }
                return response.ReleaseData();
            }
        }

        public RedisBool ExpectOne(IRedisConnection connection, bool throwException = true)
        {
            var result = ExpectNullableInteger(connection, throwException);
            if (result != null)
            {
                var value = result.Value;
                return value.HasValue && value.Value == RedisConstants.One;
            }
            return false;
        }

        public RedisInteger ExpectInteger(IRedisConnection connection, bool throwException = true)
        {
            var result = ExpectNullableInteger(connection, throwException);
            if (result == null)
                return long.MinValue;
            return result.Value;
        }

        public RedisNullableInteger ExpectNullableInteger(IRedisConnection connection, bool throwException = true)
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
                        throw new RedisException("No data returned", RedisErrorCode.CorruptResponse);
                    return null;
                }
                return new RedisRaw(RedisRawObject.ToObject(response));
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

        public RedisResponse Execute(IRedisConnection connection, bool throwException = true)
        {
            ValidateNotDisposed();
            return new RedisResponse(ExecuteInternal(connection, throwException));
        }

        private RedisRawResponse ExecuteInternal(IRedisConnection connection, bool throwException = true, bool sendNotReceive = false)
        {
            if (connection == null)
            {
                if (throwException)
                    throw new RedisFatalException(new ArgumentNullException("connection"), RedisErrorCode.MissingParameter);
                return null;
            }

            RedisRawResponse response = RedisVoidResponse.Void;
            if (sendNotReceive || !m_CommandType.HasFlag(RedisCommandType.SendAndReceive))
                connection.Send(this);
            else
            {
                response = connection.SendReceive(this);
                if (ReferenceEquals(response, null) && throwException)
                    throw new RedisException("Corrupted redis response data", RedisErrorCode.CorruptResponse);
                response.HandleError();
            }
            return response;
        }

        #endregion IRedisConnection Execution Methods

        #region RedisSocket Execution Methods

        public RedisBool ExpectOK(RedisSocketContext context, bool throwException = true)
        {
            var result = ExpectSimpleStringInternal(context, throwException);
            if (!result.IsEmpty())
                return result.Equals(RedisConstants.OK, StringComparison.OrdinalIgnoreCase);
            return false;
        }

        public RedisBool ExpectSimpleString(RedisSocketContext context, string expectedResult, bool throwException = true)
        {
            var result = ExpectSimpleStringInternal(context, throwException);
            if (!result.IsEmpty())
            {
                if (!expectedResult.IsEmpty())
                    return result.Equals(expectedResult, StringComparison.OrdinalIgnoreCase);

                if (result.StartsWith("-", StringComparison.Ordinal))
                    return false;

                return true;
            }
            return false;
        }

        public RedisString ExpectSimpleString(RedisSocketContext context, bool throwException = true)
        {
            return ExpectSimpleStringInternal(context, throwException);
        }

        private string ExpectSimpleStringInternal(RedisSocketContext context, bool throwException = true)
        {
            var bytes = ExpectSimpleStringBytes(context, throwException);
            if (bytes == (byte[])null)
                return null;
            return Encoding.UTF8.GetString(bytes);
        }

        public RedisBool ExpectSimpleStringBytes(RedisSocketContext context, byte[] expectedResult, bool throwException = true)
        {
            var result = ExpectSimpleStringBytesInternal(context, throwException);
            if (result == null)
                return expectedResult == null;

            if (expectedResult != null)
                return result == expectedResult;

            if (result.Length > 0 && result[0] == (byte)'-')
                return false;

            return false;
        }

        public RedisVoid ExpectNothing(RedisSocketContext context, bool throwException = true)
        {
            ExecuteInternal(context, throwException, true);
            return new RedisVoid(true);
        }

        public RedisBytes ExpectSimpleStringBytes(RedisSocketContext context, bool throwException = true)
        {
            return ExpectSimpleStringBytesInternal(context, throwException);
        }

        private byte[] ExpectSimpleStringBytesInternal(RedisSocketContext context, bool throwException = true)
        {
            using (var response = ExecuteInternal(context, throwException))
            {
                if (response == null)
                {
                    if (throwException)
                        throw new RedisException("No data returned", RedisErrorCode.CorruptResponse);
                    return null;
                }

                if (response.Type != RedisRawObjectType.SimpleString)
                {
                    if (throwException)
                        throw new RedisException("Invalid data returned", RedisErrorCode.CorruptResponse);
                    return null;
                }
                return response.ReleaseData();
            }
        }

        public RedisString ExpectBulkString(RedisSocketContext context, bool throwException = true)
        {
            var bytes = ExpectBulkStringBytes(context, throwException);
            if (bytes == (byte[])null)
                return null;
            return Encoding.UTF8.GetString(bytes);
        }

        public RedisBytes ExpectBulkStringBytes(RedisSocketContext context, bool throwException = true)
        {
            using (var response = ExecuteInternal(context, throwException))
            {
                if (response == null)
                {
                    if (throwException)
                        throw new RedisException("No data returned", RedisErrorCode.CorruptResponse);
                    return null;
                }

                if (response.Type != RedisRawObjectType.BulkString)
                {
                    if (throwException)
                        throw new RedisException("Invalid data returned", RedisErrorCode.CorruptResponse);
                    return null;
                }
                return response.ReleaseData();
            }
        }

        public RedisBool ExpectOne(RedisSocketContext context, bool throwException = true)
        {
            var result = ExpectNullableInteger(context, throwException);
            if (result != null)
            {
                var value = result.Value;
                return (value.HasValue && value.Value == RedisConstants.One);
            }
            return false;
        }

        public RedisInteger ExpectInteger(RedisSocketContext context, bool throwException = true)
        {
            var result = ExpectNullableInteger(context, throwException);
            if (result == null)
                return long.MinValue;
            return result.Value;
        }

        public RedisNullableInteger ExpectNullableInteger(RedisSocketContext context, bool throwException = true)
        {
            using (var response = ExecuteInternal(context, throwException))
            {
                return ForNullableInteger(response, throwException);
            }
        }

        public RedisDouble ExpectDouble(RedisSocketContext context, bool throwException = true)
        {
            using (var response = ExecuteInternal(context, throwException))
            {
                return ForDouble(response, throwException);
            }
        }

        public RedisNullableDouble ExpectNullableDouble(RedisSocketContext context, bool throwException = true)
        {
            using (var response = ExecuteInternal(context, throwException))
            {
                return ForNullableDouble(response, throwException);
            }
        }

        public RedisRaw ExpectArray(RedisSocketContext context, bool throwException = true)
        {
            using (var response = ExecuteInternal(context, throwException))
            {
                if (response == null)
                {
                    if (throwException)
                        throw new RedisException("No data returned", RedisErrorCode.CorruptResponse);
                    return null;
                }
                return new RedisRaw(RedisRawObject.ToObject(response));
            }
        }

        public RedisMultiString ExpectMultiDataStrings(RedisSocketContext context, bool throwException = true)
        {
            using (var response = ExecuteInternal(context, throwException))
            {
                return ForMutiDataStrings(response, throwException);
            }
        }

        public RedisMultiBytes ExpectMultiDataBytes(RedisSocketContext context, bool throwException = true)
        {
            using (var response = ExecuteInternal(context, throwException))
            {
                return ForMultiDataBytes(response, throwException);
            }
        }

        public RedisRawResponse Execute(RedisSocketContext context, bool throwException = true)
        {
            ValidateNotDisposed();
            return ExecuteInternal(context, throwException);
        }

        private RedisRawResponse ExecuteInternal(RedisSocketContext context, bool throwException = true, bool sendNotReceive = false)
        {
            if (context == null)
            {
                if (throwException)
                    throw new RedisFatalException(new ArgumentNullException("context"), RedisErrorCode.MissingParameter);
                return null;
            }

            var socket = context.Socket;
            if (socket == null)
            {
                if (throwException)
                    throw new RedisFatalException(new ArgumentNullException("context.Socket"), RedisErrorCode.MissingParameter);
                return null;
            }

            RedisRawResponse response = RedisVoidResponse.Void;
            if (sendNotReceive || !m_CommandType.HasFlag(RedisCommandType.SendAndReceive))
                WriteTo(socket);
            else
            {
                WriteTo(socket);
                using (var reader = new RedisSingleResponseReader(context.Settings))
                    response = reader.Execute(socket);

                if (ReferenceEquals(response, null) && throwException)
                    throw new RedisException("Corrupted redis response data", RedisErrorCode.CorruptResponse);
                response.HandleError();
            }
            return response;
        }

        #endregion RedisSocket Execution Methods

        #region Common Execution Methods

        protected static double ForDouble(IRedisRawResponse response, bool throwException)
        {
            if (response == null)
            {
                if (throwException)
                    throw new RedisException("No data returned", RedisErrorCode.CorruptResponse);
                return double.MinValue;
            }

            if (response.Type == RedisRawObjectType.Array ||
                response.Type == RedisRawObjectType.Undefined)
            {
                if (throwException)
                    throw new RedisException("Invalid data returned", RedisErrorCode.CorruptResponse);
                return double.MinValue;
            }

            var data = response.Data;
            if (data.IsEmpty())
            {
                if (throwException)
                    throw new RedisException("No data returned", RedisErrorCode.CorruptResponse);
                return double.MinValue;
            }

            double result;
            if (double.TryParse(Encoding.UTF8.GetString(data), out result))
                return result;

            if (throwException)
                throw new RedisException("Not a double result", RedisErrorCode.CorruptResponse);

            return double.MinValue;
        }

        protected static long? ForNullableInteger(IRedisRawResponse response, bool throwException)
        {
            if (response == null)
            {
                if (throwException)
                    throw new RedisException("No data returned", RedisErrorCode.CorruptResponse);
                return null;
            }

            if (response.Type != RedisRawObjectType.Integer)
            {
                if (throwException)
                    throw new RedisException("Invalid data returned", RedisErrorCode.CorruptResponse);
                return null;
            }

            var data = response.Data;
            if (data.IsEmpty())
            {
                if (throwException)
                    throw new RedisException("No data returned", RedisErrorCode.CorruptResponse);
                return null;
            }

            if (data.EqualTo(RedisConstants.Nil))
                return null;

            long result;
            if (long.TryParse(Encoding.UTF8.GetString(data), out result))
                return result;

            if (throwException)
                throw new RedisException("Not an integer result", RedisErrorCode.CorruptResponse);

            return null;
        }


        protected static double? ForNullableDouble(IRedisRawResponse response, bool throwException)
        {
            if (response == null)
            {
                if (throwException)
                    throw new RedisException("No data returned", RedisErrorCode.CorruptResponse);
                return null;
            }

            if (response.Type == RedisRawObjectType.Array ||
                response.Type == RedisRawObjectType.Undefined)
            {
                if (throwException)
                    throw new RedisException("Invalid data returned", RedisErrorCode.CorruptResponse);
                return null;
            }

            var data = response.Data;
            if (data == null)
                return null;

            if (data.Length == 0)
            {
                if (throwException)
                    throw new RedisException("No data returned", RedisErrorCode.CorruptResponse);
                return null;
            }

            if (data.EqualTo(RedisConstants.Nil))
                return null;

            double result;
            if (double.TryParse(Encoding.UTF8.GetString(data), out result))
                return result;

            if (throwException)
                throw new RedisException("Not a double result", RedisErrorCode.CorruptResponse);

            return null;
        }

        protected static string[] ForMutiDataStrings(IRedisRawResponse response, bool throwException)
        {
            if (response == null)
            {
                if (throwException)
                    throw new RedisException("No data returned", RedisErrorCode.CorruptResponse);
                return null;
            }

            var data = response.Data;
            switch (response.Type)
            {
                case RedisRawObjectType.SimpleString:
                case RedisRawObjectType.BulkString:
                case RedisRawObjectType.Integer:
                    return data != null ? new string[] { Encoding.UTF8.GetString(data) } : null;
                case RedisRawObjectType.Error:
                    {
                        if (!throwException)
                            return data != null ? new string[] { Encoding.UTF8.GetString(data) } : null;
                        throw new RedisException(!data.IsEmpty() ? Encoding.UTF8.GetString(data) : "No data returned", RedisErrorCode.CorruptResponse);
                    }
                case RedisRawObjectType.Undefined:
                    if (throwException)
                        throw new RedisException("Undefined respone data", RedisErrorCode.CorruptResponse);
                    return null;
                case RedisRawObjectType.Array:
                    {
                        var len = response.Length;
                        if (len < 0)
                            return null;
                        if (len == 0)
                            return new string[] { };

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
                                    if (item.Type == RedisRawObjectType.Undefined)
                                    {
                                        if (throwException)
                                            throw new RedisException("Undefined respone data", RedisErrorCode.CorruptResponse);
                                        return null;
                                    }

                                    if (item.Type == RedisRawObjectType.Array)
                                    {
                                        if (throwException)
                                            throw new RedisException("Multi-array is not allowed for multi-data respone", RedisErrorCode.CorruptResponse);
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
                default:
                    break;
            }
            return null;
        }

        protected static byte[][] ForMultiDataBytes(IRedisRawResponse response, bool throwException)
        {
            if (response == null)
            {
                if (throwException)
                    throw new RedisException("No data returned", RedisErrorCode.CorruptResponse);
                return null;
            }

            var data = response.Data;
            switch (response.Type)
            {
                case RedisRawObjectType.SimpleString:
                case RedisRawObjectType.BulkString:
                case RedisRawObjectType.Integer:
                    return data != null ? new byte[][] { data } : null;
                case RedisRawObjectType.Error:
                    {
                        if (!throwException)
                            return data != null ? new byte[][] { data } : null;
                        throw new RedisException(!data.IsEmpty() ? Encoding.UTF8.GetString(data) : "No data returned", RedisErrorCode.CorruptResponse);
                    }
                case RedisRawObjectType.Undefined:
                    if (throwException)
                        throw new RedisException("Undefined respone data", RedisErrorCode.CorruptResponse);
                    return null;
                case RedisRawObjectType.Array:
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
                                    if (item.Type == RedisRawObjectType.Undefined)
                                    {
                                        if (throwException)
                                            throw new RedisException("Undefined respone data", RedisErrorCode.CorruptResponse);
                                        return null;
                                    }

                                    if (item.Type == RedisRawObjectType.Array)
                                    {
                                        if (throwException)
                                            throw new RedisException("Multi-array is not allowed for multi-data respone", RedisErrorCode.CorruptResponse);
                                        return null;
                                    }

                                    list.Add(item.Data);
                                }
                            }
                            return list.ToArray();
                        }

                        return null;
                    }
                default:
                    break;
            }
            return null;
        }

        #endregion Common Execution Methods

        #endregion Execution Methods

        #region WriteTo Methods

        public void WriteTo(Stream stream, bool flush = true)
        {
            if (stream == null)
                throw new ArgumentNullException("stream");

            if (!stream.CanWrite)
                throw new ArgumentException("Can not write to closed stream", "stream");

            using (var writer = new RedisStreamWriter(stream, flush))
            {
                WriteTo(writer);
            }
        }

        public void WriteTo(RedisSocket socket, bool flush = true)
        {
            if (socket == null)
                throw new ArgumentNullException("socket");

            var stream = socket.GetBufferedStream();
            if (!stream.CanWrite)
                throw new ArgumentException("Can not write to closed stream", "stream");

            using (var writer = new RedisStreamWriter(stream, flush))
            {
                WriteTo(writer);
            }
        }

        public Task WriteToAsync(Stream stream, bool flush = true)
        {
            if (stream == null)
                throw new ArgumentNullException("stream");

            if (!stream.CanWrite)
                throw new ArgumentException("Can not write to closed stream", "stream");

            Action action = () =>
            {
                using (var writer = new RedisStreamWriter(stream, flush))
                {
                    WriteTo(writer);
                }
            };
            return action.InvokeAsync();
        }

        public Task WriteToAsync(RedisSocket socket, bool flush = true)
        {
            if (socket == null)
                throw new ArgumentNullException("socket");

            Action<Stream> action = (stream) =>
            {
                using (var writer = new RedisStreamWriter(stream, flush))
                {
                    WriteTo(writer);
                }
            };
            return action.InvokeAsync(socket.GetBufferedStream());
        }

        public virtual void WriteTo(IRedisWriter writer)
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
