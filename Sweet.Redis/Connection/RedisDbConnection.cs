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
    internal class RedisDbConnection : RedisBidirectionalConnection, IRedisDbConnection
    {
        #region Constants

        private const int DbReceiveTimeout = 100;

        #endregion Constants

        #region Field Members

        private int m_DbIndex;

        #endregion Field Members

        #region .Ctors

        internal RedisDbConnection(string name, RedisRole expectedRole, RedisConnectionSettings settings,
            Action<RedisConnection, RedisSocket> onCreateSocket, Action<RedisConnection, RedisSocket> onReleaseSocket,
            int dbIndex, RedisSocket socket = null, bool connectImmediately = false)
            : base(name, expectedRole, settings, onCreateSocket, onReleaseSocket, socket, false)
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

        protected bool SelectInternal(RedisSocket socket, int dbIndex, bool throwException)
        {
            ValidateNotDisposed();
            if (dbIndex > RedisConstants.MinDbIndex && dbIndex <= RedisConstants.MaxDbIndex)
            {
                using (var cmd = new RedisCommand(dbIndex, RedisCommandList.Select, RedisCommandType.SendAndReceive, dbIndex.ToBytes()))
                {
                    return cmd.ExpectOK(new RedisSocketContext(socket, Settings), throwException);
                }
            }
            return true;
        }

        #endregion Member Methods
    }
}
