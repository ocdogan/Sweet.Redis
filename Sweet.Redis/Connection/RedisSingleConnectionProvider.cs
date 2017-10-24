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
using System.Net;
using System.Threading;

namespace Sweet.Redis
{
    internal class RedisSingleConnectionProvider : RedisConnectionProvider
    {
        #region Field Members

        protected RedisSocket m_Socket;
        protected readonly object m_SocketLock = new object();

        protected IRedisConnection m_Connection;
        protected readonly object m_ConnectionLock = new object();

        #endregion Field Members

        #region .Ctors

        public RedisSingleConnectionProvider(string name, RedisConnectionSettings settings)
            : base(name, settings)
        { }

        #endregion .Ctors

        #region Destructors

        protected override void OnDispose(bool disposing)
        {
            base.OnDispose(disposing);
            DisposeConnection();
        }

        #endregion Destructors

        #region Properties

        public override RedisEndPoint EndPoint
        {
            get
            {
                var socket = m_Socket;
                if (socket.IsConnected())
                {
                    var ipEP = socket.RemoteEP;
                    if (ipEP != null)
                        return new RedisEndPoint(ipEP.Address.ToString(), ipEP.Port);

                    var ep = socket.RemoteEndPoint;
                    if (ep != null)
                    {
                        ipEP = ep as IPEndPoint;
                        if (ipEP != null)
                            return new RedisEndPoint(ipEP.Address.ToString(), ipEP.Port);

                        var dnsEP = ep as DnsEndPoint;
                        if (dnsEP != null)
                            return new RedisEndPoint(dnsEP.Host, ipEP.Port);
                    }
                }
                return (base.EndPoint ?? RedisEndPoint.Empty);
            }
        }

        public override int SpareCount
        {
            get
            {
                var connection = m_Connection;
                if (connection != null && !connection.Disposed)
                    return 1;
                return 0;
            }
        }

        #endregion Properties

        #region Methods

        internal void DisposeConnection()
        {
            var connection = Interlocked.Exchange(ref m_Connection, null);
            if (connection != null)
                connection.Dispose();

            var socket = Interlocked.Exchange(ref m_Socket, null);
            if (socket != null)
                socket.DisposeSocket();
        }

        protected override RedisConnectionLimiter NewConnectionLimiter(int maxCount)
        {
            return new RedisConnectionLimiter(1);
        }

        protected override IRedisConnection NewConnection(RedisSocket socket, int dbIndex, RedisRole expectedRole, bool connectImmediately = true)
        {
            var connection = m_Connection;
            if (connection == null || connection.Disposed)
            {
                lock (m_ConnectionLock)
                {
                    connection = m_Connection;
                    if (connection == null || connection.Disposed)
                    {
                        m_Connection = connection = OnNewConnection(socket, dbIndex, expectedRole, connectImmediately);
                    }
                }
            }
            return connection;
        }

        protected virtual IRedisConnection OnNewConnection(RedisSocket socket, int dbIndex, RedisRole role, bool connectImmediately = true)
        {
            return null;
        }

        protected override RedisSocket DequeueSocket(int dbIndex, RedisRole role)
        {
            var socket = m_Socket;
            lock (m_SocketLock)
            {
                socket = m_Socket;
                if (socket != null && !socket.IsConnected())
                {
                    Interlocked.Exchange(ref m_Socket, null);
                    socket = null;
                }
            }
            return socket;
        }

        protected override void CompleteSocketRelease(IRedisConnection connection, RedisSocket socket)
        {
            if (socket != null)
            {
                lock (m_SocketLock)
                {
                    RedisSocket currentSocket;
                    if (Disposed)
                    {
                        currentSocket = Interlocked.CompareExchange(ref m_Socket, null, socket);
                        if (currentSocket != null)
                            currentSocket.DisposeSocket();
                        else if (socket != null)
                            socket.DisposeSocket();
                        return;
                    }

                    currentSocket = m_Socket;
                    if (currentSocket != null && currentSocket.Disposed)
                    {
                        Interlocked.Exchange(ref m_Socket, null);
                        currentSocket = null;
                    }

                    if (ReferenceEquals(currentSocket, socket))
                        return;

                    if (socket != null && !socket.Disposed)
                    {
                        currentSocket = Interlocked.Exchange(ref m_Socket, socket);
                        if (currentSocket != null)
                            currentSocket.DisposeSocket();
                    }
                }
            }
        }

        #endregion Methods
    }
}
