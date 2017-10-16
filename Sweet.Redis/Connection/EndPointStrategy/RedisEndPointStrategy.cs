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
using System.Linq;
using System.Net;

namespace Sweet.Redis
{
    internal class RedisEndPointStrategy : IRedisEndPointStrategy
    {
        #region QueueItem

        private class EPSocketPair
        {
            public IPEndPoint EndPoint;
            public RedisSocket Socket;
        }

        #endregion QueueItem

        #region Field Members

        private EPSocketPair[] m_Items;
        private readonly object m_ItemsLock = new object();

        private Func<IPEndPoint, RedisSocket> m_SocketFactory;

        #endregion Field Members

        #region .Ctors

        public RedisEndPointStrategy(RedisEndPoint[] endPoints, Func<IPEndPoint, RedisSocket> socketFactory)
        {
            m_SocketFactory = socketFactory;
            Reload(endPoints);
        }

        private void Reload(RedisEndPoint[] endPoints)
        {
            if (endPoints != null)
            {
                var length = endPoints.Length;
                if (length > 0)
                {
                    var endPointList = new HashSet<IPEndPoint>();
                    foreach (var endPoint in endPoints)
                    {
                        var addresses = endPoint.ResolveHost();
                        if (addresses != null)
                        {
                            foreach (var address in addresses)
                                endPointList.Add(new IPEndPoint(address, endPoint.Port));
                        }
                    }

                    if (endPointList.Count > 0)
                    {
                        var ipEndPoints = endPointList.ToArray();
                        if (ipEndPoints != null)
                        {
                            length = ipEndPoints.Length;
                            if (length > 0)
                            {
                                var list = new List<EPSocketPair>(length);
                                foreach (var endPoint in ipEndPoints)
                                {
                                    list.Add(new EPSocketPair { EndPoint = endPoint });
                                }

                                m_Items = list.ToArray();
                            }
                        }
                    }
                }
            }
        }

        #endregion .Ctors

        #region Methods

        public RedisSocket Dequeue()
        {
            if (m_Items != null && m_Items.Length > 0)
            {
                lock (m_ItemsLock)
                {
                    foreach (var item in m_Items)
                    {
                        var socket = item.Socket;
                        if (socket != null)
                        {
                            item.Socket = null;
                            if (socket.IsConnected())
                                return socket;

                            socket.DisposeSocket();
                        }
                    }

                    foreach (var item in m_Items)
                    {
                        var socket = item.Socket;
                        if (socket == null)
                            return m_SocketFactory(item.EndPoint);
                    }
                }
            }
            throw new RedisFatalException("Cannot dequeue any socket");
        }

        public void Enqueue(RedisSocket socket)
        {
            if (socket != null && socket.IsConnected())
            {
                var endPoint = socket.RemoteEP;
                if (endPoint != null)
                {
                    lock (m_ItemsLock)
                    {
                        foreach (var item in m_Items)
                        {
                            if (item.EndPoint == endPoint)
                            {
                                var currSocket = item.Socket;
                                if (!ReferenceEquals(socket, currSocket))
                                {
                                    item.Socket = socket;
                                    currSocket.DisposeSocket();
                                }
                            }
                        }
                    }
                }
            }
            throw new RedisFatalException("Cannot enqueue socket");
        }

        #endregion Methods
    }
}
