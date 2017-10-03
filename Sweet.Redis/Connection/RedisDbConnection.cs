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
using System.Threading;

namespace Sweet.Redis
{
    internal class RedisDbConnection : RedisConnection, IRedisDbConnection
    {
        #region Constants

        private const int DbReceiveTimeout = 100;

        #endregion Constants

        #region Field Members

        private int m_DbIndex;

        #endregion Field Members

        #region .Ctors

        internal RedisDbConnection(string name, Action<RedisConnection, RedisSocket> onCreateSocket,
            Action<RedisConnection, RedisSocket> onReleaseSocket, int dbIndex, RedisSocket socket = null,
            bool connectImmediately = false)
            : this(name, RedisSettings.Default, onCreateSocket, onReleaseSocket, dbIndex, socket, connectImmediately)
        { }

        internal RedisDbConnection(string name, RedisSettings settings,
            Action<RedisConnection, RedisSocket> onCreateSocket, Action<RedisConnection, RedisSocket> onReleaseSocket,
            int dbIndex, RedisSocket socket = null, bool connectImmediately = false)
            : base(name, settings, onCreateSocket, onReleaseSocket, socket, false)
        {
            m_DbIndex = Math.Min(Math.Max(dbIndex, RedisConstants.MinDbIndex), RedisConstants.MaxDbIndex);
            if (connectImmediately)
                ConnectInternal();
        }

        #endregion .Ctors

        #region Properties

        public int DbIndex
        {
            get { return m_DbIndex; }
        }

        #endregion Properties

        #region Member Methods

        protected override int GetReceiveTimeout()
        {
            return DbReceiveTimeout;
        }

        protected override void OnConnect(RedisSocket socket)
        {
            base.OnConnect(socket);
            if (m_DbIndex > RedisConstants.MinDbIndex &&
               socket.IsConnected() && SelectInternal(socket, m_DbIndex, true))
                socket.SetDb(m_DbIndex);
        }

        public void Select(int dbIndex)
        {
            dbIndex = Math.Min(Math.Max(dbIndex, RedisConstants.MinDbIndex), RedisConstants.MaxDbIndex);
            if (dbIndex != m_DbIndex)
            {
                var socket = GetSocket();
                if (socket != null)
                {
                    if (dbIndex != RedisConstants.MinDbIndex)
                        SelectInternal(null, m_DbIndex, true);
                    socket.SetDb(m_DbIndex);
                }
                Interlocked.Exchange(ref m_DbIndex, dbIndex);
            }
        }

        protected bool SelectInternal(RedisSocket socket, int db, bool throwException)
        {
            ValidateNotDisposed();
            if (db > RedisConstants.MinDbIndex && db <= RedisConstants.MaxDbIndex)
            {
                using (var cmd = new RedisCommand(db, RedisCommands.Select, RedisCommandType.SendAndReceive, db.ToBytes()))
                {
                    return cmd.ExpectSimpleString(socket, Settings, RedisConstants.OK, throwException);
                }
            }
            return true;
        }

        public override RedisRawResponse SendReceive(byte[] data)
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
                .ContinueWith<RedisRawResponse>((ret) =>
                {
                    if (ret.IsCompleted && ret.Result > 0)
                        using (var reader = new RedisSingleResponseReader(Settings))
                            return reader.Execute(socket);
                    return null;
                });
            return task.Result;
        }

        public override RedisRawResponse SendReceive(IRedisCommand cmd)
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
