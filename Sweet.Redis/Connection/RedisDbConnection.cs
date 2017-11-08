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
            m_DbIndex = Math.Min(Math.Max(dbIndex, RedisConstants.UninitializedDbIndex), RedisConstants.MaxDbIndex);
            try
            {
                if (connectImmediately)
                    ConnectInternal();
            }
            catch (Exception)
            { }
            SetDb(m_DbIndex);
        }

        #endregion .Ctors

        #region Properties

        public int DbIndex
        {
            get { return m_DbIndex; }
        }

        #endregion Properties

        #region Methods

        internal void SetDb(int dbIndex)
        {
            m_DbIndex = Math.Min(Math.Max(dbIndex, RedisConstants.UninitializedDbIndex), RedisConstants.MaxDbIndex);

            var socket = m_Socket;
            if (m_DbIndex > RedisConstants.UninitializedDbIndex && 
                socket.IsConnected() && socket.DbIndex != m_DbIndex)
                socket.SelectDB(Settings, m_DbIndex);
        }

        protected override int GetReceiveTimeout()
        {
            return DbReceiveTimeout;
        }

        protected override void OnConnect(RedisSocket socket)
        {
            base.OnConnect(socket);
            if (SelectDB(socket, m_DbIndex))
                m_DbIndex = socket.DbIndex;
        }

        #endregion Methods
    }
}
