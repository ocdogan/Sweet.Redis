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
using System.Net.Sockets;

namespace Sweet.Redis
{
    internal class RedisDbConnection : RedisConnection, IRedisDbConnection
    {
        #region Constants

        private const int DbReceiveTimeout = 100;

        #endregion Constants

        #region Field Members

        private int m_Db;

        #endregion Field Members

        #region .Ctors

        internal RedisDbConnection(string name, Action<RedisConnection, RedisSocket> releaseAction,
            int db, RedisSocket socket = null, bool connectImmediately = false)
            : this(name, RedisSettings.Default, releaseAction, db, socket, connectImmediately)
        { }

        internal RedisDbConnection(string name, RedisSettings settings,
            Action<RedisConnection, RedisSocket> releaseAction, int db, RedisSocket socket = null,
            bool connectImmediately = false)
            : base(name, settings, releaseAction, socket, connectImmediately)
        {
            m_Db = db;
        }

        #endregion .Ctors

        #region Properties

        public int Db
        {
            get { return m_Db; }
        }

        #endregion Properties

        #region Member Methods

        protected override int GetReceiveTimeout()
        {
            return DbReceiveTimeout;
        }

        public IRedisResponse SendReceive(byte[] data)
        {
            ValidateNotDisposed();

            var socket = Connect();
            if (socket == null)
            {
                SetLastError((long)SocketError.NotConnected);
                SetState((long)RedisConnectionState.Failed);

                throw new SocketException((int)SocketError.NotConnected);
            }

            var task = socket.SendAsync(data, 0, data.Length)
                .ContinueWith<IRedisResponse>((ret) =>
                {
                    if (ret.IsCompleted && ret.Result > 0)
                        using (var reader = new RedisSingleResponseReader(Settings))
                            return reader.Execute(socket);
                    return null;
                });
            return task.Result;
        }

        public IRedisResponse SendReceive(IRedisCommand cmd)
        {
            if (cmd == null)
                throw new ArgumentNullException("cmd");

            ValidateNotDisposed();

            var socket = Connect();
            if (socket == null)
            {
                SetLastError((long)SocketError.NotConnected);
                SetState((long)RedisConnectionState.Failed);

                throw new SocketException((int)SocketError.NotConnected);
            }

            cmd.WriteTo(socket);
            using (var reader = new RedisSingleResponseReader(Settings))
                return reader.Execute(socket);
        }

        #endregion Member Methods
    }
}
