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
using System.Threading;

namespace Sweet.Redis
{
    public class RedisSentinelClient : RedisClient, IRedisSentinelClient
    {
        #region Field Members

        private RedisPoolSettings m_Settings;
        private IRedisSentinelCommands m_Commands;

        private IRedisConnectionProvider m_ConnectionProvider;

        #endregion Field Members

        #region .Ctors

        public RedisSentinelClient(RedisPoolSettings settings, bool throwOnError = true)
            : base(throwOnError)
        {
            if (settings == null)
                throw new ArgumentNullException("settings");

            m_Settings = settings;
            m_ConnectionProvider = new RedisSentinelConnectionProvider(m_Settings);
        }

        #endregion .Ctors

        #region Destructors

        protected override void OnDispose(bool disposing)
        {
            base.OnDispose(disposing);
            Interlocked.Exchange(ref m_Settings, null);
        }

        #endregion Destructors

        #region Properties

        public IRedisSentinelCommands Commands
        {
            get
            {
                ValidateNotDisposed();
                if (m_Commands == null)
                    m_Commands = new RedisSentinelCommands(this);
                return m_Commands;
            }
        }

        public override RedisRole Role
        {
            get { return RedisRole.Sentinel; }
        }

        public RedisPoolSettings Settings
        {
            get { return m_Settings; }
        }

        #endregion Properties

        #region Methods

        protected virtual IRedisConnection Connect()
        {
            ValidateNotDisposed();

            var connectionProvider = m_ConnectionProvider;
            if (connectionProvider != null)
            {
                var connection = connectionProvider.Connect(-1, RedisRole.Sentinel);

                if (connection != null && !connection.Connected)
                    connection.Connect();

                OnConnect(connection);

                return connection;
            }
            return null;
        }

        protected virtual void OnConnect(IRedisConnection connection)
        { }

        #region Execution Methods

        protected internal override T Expect<T>(RedisCommand command, RedisCommandExpect expectation, string okIf = null)
        {
            using (var connection = Connect())
            {
                switch (expectation)
                {
                    case RedisCommandExpect.Response:
                        return (T)(object)command.Execute(connection, ThrowOnError);
                    case RedisCommandExpect.Array:
                        return (T)(object)command.ExpectArray(connection, ThrowOnError);
                    case RedisCommandExpect.BulkString:
                        return (T)(object)command.ExpectBulkString(connection, ThrowOnError);
                    case RedisCommandExpect.BulkStringBytes:
                        return (T)(object)command.ExpectBulkStringBytes(connection, ThrowOnError);
                    case RedisCommandExpect.Double:
                        return (T)(object)command.ExpectDouble(connection, ThrowOnError);
                    case RedisCommandExpect.GreaterThanZero:
                        return (T)(object)command.ExpectInteger(connection, ThrowOnError);
                    case RedisCommandExpect.Integer:
                        return (T)(object)command.ExpectInteger(connection, ThrowOnError);
                    case RedisCommandExpect.MultiDataBytes:
                        return (T)(object)command.ExpectMultiDataBytes(connection, ThrowOnError);
                    case RedisCommandExpect.MultiDataStrings:
                        return (T)(object)command.ExpectMultiDataStrings(connection, ThrowOnError);
                    case RedisCommandExpect.Nothing:
                        return (T)(object)command.ExpectNothing(connection, ThrowOnError);
                    case RedisCommandExpect.NullableDouble:
                        return (T)(object)command.ExpectNullableDouble(connection, ThrowOnError);
                    case RedisCommandExpect.NullableInteger:
                        return (T)(object)command.ExpectNullableInteger(connection, ThrowOnError);
                    case RedisCommandExpect.OK:
                        return (T)(object)command.ExpectOK(connection, ThrowOnError);
                    case RedisCommandExpect.One:
                        return (T)(object)command.ExpectOne(connection, ThrowOnError);
                    case RedisCommandExpect.SimpleString:
                        if (!String.IsNullOrEmpty(okIf))
                            return (T)(object)command.ExpectSimpleString(connection, okIf, ThrowOnError);
                        return (T)(object)command.ExpectSimpleString(connection, ThrowOnError);
                    case RedisCommandExpect.SimpleStringBytes:
                        if (!String.IsNullOrEmpty(okIf))
                            return (T)(object)command.ExpectSimpleStringBytes(connection, okIf.ToBytes(), ThrowOnError);
                        return (T)(object)command.ExpectSimpleStringBytes(connection, ThrowOnError);
                    default:
                        throw new RedisException(String.Format("Undefined expectation type, {0}", expectation.ToString("F")), RedisErrorCode.NotSupported);
                }
            }
        }

        #endregion Execution Methods

        #endregion Methods
    }
}
