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
    internal class RedisContinuousReader : RedisDisposable, IRedisReceiver
    {
        #region Field Members

        private long m_ReceiveState;

        private RedisContinuousReaderCtx m_Context;

        private RedisSocket m_Socket;
        private RedisConnection m_Connection;

        #endregion Field Members

        #region .Ctors

        public RedisContinuousReader(RedisConnection connection)
        {
            m_Connection = connection;
        }

        #endregion .Ctors

        #region Destructors

        protected override void OnDispose(bool disposing)
        {
            base.OnDispose(disposing);

            EndReceiveInternal();

            Interlocked.Exchange(ref m_Socket, null);
            Interlocked.Exchange(ref m_Connection, null);
        }

        #endregion Destructors

        #region Properties

        public RedisConnection Connection
        {
            get { return m_Connection; }
        }

        public bool Receiving
        {
            get { return Interlocked.Read(ref m_ReceiveState) != RedisConstants.Zero; }
        }

        #endregion Properties

        #region Methods

        public void BeginReceive(Action<RedisContinuousReader> onComplete, Action<IRedisRawResponse> onReceive)
        {
            ValidateNotDisposed();

            if (Interlocked.CompareExchange(ref m_ReceiveState, RedisConstants.One, RedisConstants.Zero) ==
                RedisConstants.Zero)
            {
                m_Connection.ConnectAsync().
                    ContinueWith(t =>
                    {
                        RedisSocket socket = null;
                        if (t.IsCompleted)
                            socket = t.Result;

                        Interlocked.Exchange(ref m_Socket, socket);
                        try
                        {
                            if (socket != null)
                            {
                                try
                                {
                                    using (var ctx = new RedisContinuousReaderCtx(this, m_Connection, socket, onReceive))
                                    {
                                        Interlocked.Exchange(ref m_Context, ctx);
                                        ctx.Read();
                                    }
                                }
                                finally
                                {
                                    Interlocked.Exchange(ref m_Context, null);
                                }
                            }
                        }
                        finally
                        {
                            Interlocked.Exchange(ref m_ReceiveState, RedisConstants.Zero);
                            Interlocked.Exchange(ref m_Socket, null);

                            if (socket != null)
                            {
                                try
                                {
                                    if (socket.IsConnected())
                                        socket.Close();
                                }
                                catch (Exception)
                                { }

                                socket.DisposeSocket();
                            }
                        }
                    }).ContinueWith(t =>
                    {
                        if (onComplete != null)
                            onComplete(this);
                    });
            }
        }

        public void EndReceive()
        {
            ValidateNotDisposed();
            EndReceiveInternal();
        }

        private void EndReceiveInternal()
        {
            Interlocked.Exchange(ref m_ReceiveState, RedisConstants.Zero);

            var ctx = Interlocked.Exchange(ref m_Context, null);
            if (ctx != null)
                ctx.Dispose();
        }

        #endregion Methods
    }
}
