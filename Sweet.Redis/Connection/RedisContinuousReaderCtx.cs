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
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Net.Sockets;
using System.Text;
using System.Threading;

namespace Sweet.Redis
{
    internal class RedisContinuousReaderCtx : RedisResponseReader
    {
        #region Field Members

        private RedisSocket m_Socket;
        private RedisConnection m_Connection;
        private RedisContinuousReader m_Reader;

        private Action<IRedisResponse> m_OnReceive;

        private long m_ProcessingReceivedQ;
        private ConcurrentQueue<IRedisResponse> m_ReceivedResponseQ = new ConcurrentQueue<IRedisResponse>();

        #endregion Field Members

        #region .Ctors

        public RedisContinuousReaderCtx(RedisContinuousReader reader, RedisConnection connection,
                   RedisSocket socket, Action<IRedisResponse> onReceive)
            : base(16 * 1024)
        {
            Reader = reader;
            Connection = connection;
            Socket = socket;
            m_OnReceive = onReceive;
        }

        #endregion .Ctors

        #region Destructors

        protected override void OnDispose(bool disposing)
        {
            base.OnDispose(disposing);

            EndRead();

            Interlocked.Exchange(ref m_OnReceive, null);

            Interlocked.Exchange(ref m_Reader, null);
            Interlocked.Exchange(ref m_Connection, null);

            var socket = Interlocked.Exchange(ref m_Socket, null);

            if (ReceiveStarted)
            {
                try
                {
                    if (socket != null && socket.Connected)
                        socket.Send(new byte[0]);
                }
                catch (Exception)
                { }
            }

            IRedisResponse temp;
            while (m_ReceivedResponseQ.TryDequeue(out temp)) { }
        }

        #endregion Destructors

        #region Properties

        public RedisSocket Socket
        {
            get { return m_Socket; }
            private set
            {
                Interlocked.Exchange(ref m_Socket, value);
            }
        }

        public RedisConnection Connection
        {
            get { return m_Connection; }
            private set
            {
                Interlocked.Exchange(ref m_Connection, value);
            }
        }

        public RedisContinuousReader Reader
        {
            get { return m_Reader; }
            private set
            {
                Interlocked.Exchange(ref m_Reader, value);
            }
        }

        public override bool Receiving
        {
            get
            {
                if (base.Receiving)
                {
                    var reader = Reader;
                    return (reader != null) && !reader.Disposed && reader.Receiving;
                }
                return false;
            }
        }

        #endregion Properties

        #region Methods

        public void Read()
        {
            ValidateNotDisposed();

            if (!(Socket == null || Connection == null) &&
                Interlocked.CompareExchange(ref m_ReadState, RedisConstants.One, RedisConstants.Zero) == RedisConstants.Zero)
            {
                try
                {
                    do
                    {
                        ReadResponse(Socket);
                    }
                    while (Receiving);
                }
                catch (Exception e)
                {
                    Error = e;
                }
                finally
                {
                    Interlocked.Exchange(ref m_ReadState, RedisConstants.Zero);
                }
            }
        }

        protected override void OnResponse(IRedisResponse response)
        {
            if (response != null && Receiving)
            {
                m_ReceivedResponseQ.Enqueue(response);

                if (Interlocked.CompareExchange(ref m_ProcessingReceivedQ, RedisConstants.One, RedisConstants.Zero) ==
                    RedisConstants.Zero)
                {
                    Action qProcess = () =>
                    {
                        try
                        {
                            IRedisResponse qItem;
                            while (m_ReceivedResponseQ.TryDequeue(out qItem))
                            {
                                try
                                {
                                    var onReceive = m_OnReceive;
                                    if (onReceive != null)
                                        onReceive.InvokeAsync(qItem);
                                }
                                catch (Exception)
                                { }
                            }
                        }
                        finally
                        {
                            Interlocked.Exchange(ref m_ProcessingReceivedQ, RedisConstants.Zero);
                        }
                    };

                    qProcess.InvokeAsync();
                }
            }
        }

        #endregion Methods
    }
}
