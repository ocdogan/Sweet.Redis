﻿#region License
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
    internal class RedisContinuousReaderConnection : RedisConnection, IRedisContinuousConnection, IRedisReceiver
    {
        #region Constants

        private const int CRReceiveTimeout = 500;

        #endregion Constants

        #region Field Members

        private long m_ReceiveState;
        private RedisContinuousReader m_Reader;

        private Action<IRedisRawResponse> m_OnReceiveResponse;

        #endregion Field Members

        #region .Ctors

        internal RedisContinuousReaderConnection(string name, RedisRole expectedRole, RedisPoolSettings settings,
            Action<IRedisRawResponse> onReceiveResponse, Action<RedisConnection, RedisSocket> onCreateSocket,
            Action<RedisConnection, RedisSocket> onReleaseSocket, RedisSocket socket = null, bool connectImmediately = true)
            : base(name, expectedRole, settings, onCreateSocket, onReleaseSocket, socket, connectImmediately)
        {
            m_OnReceiveResponse = onReceiveResponse;
        }

        #endregion .Ctors

        #region Destructor

        protected override void OnDispose(bool disposing)
        {
            base.OnDispose(disposing);

            Interlocked.Exchange(ref m_OnReceiveResponse, null);
            Interlocked.Exchange(ref m_ReceiveState, RedisConstants.Zero);

            var reader = Interlocked.Exchange(ref m_Reader, null);
            if (reader != null)
                reader.Dispose();
        }

        #endregion Destructor

        #region Properties

        public bool Receiving
        {
            get { return Interlocked.Read(ref m_ReceiveState) != RedisConstants.Zero; }
        }

        #endregion Properties

        #region Methods

        public override RedisRawResponse SendReceive(byte[] data, RedisRole commandRole)
        {
            ValidateNotDisposed();
            throw new NotImplementedException("SendAndReceive is not supported by continuous reader connections. Use Send method for sending data.");
        }

        public override RedisRawResponse SendReceive(IRedisCommand cmd)
        {
            ValidateNotDisposed();
            throw new NotImplementedException("SendAndReceive is not supported by continuous reader connections. Use Send method for sending command.");
        }

        protected override int GetReceiveTimeout()
        {
            return CRReceiveTimeout;
        }

        protected override void DoConfigure(RedisSocket socket)
        {
            base.DoConfigure(socket);
            socket.Blocking = true;
        }

        public override void ReleaseSocket()
        {
            try
            {
                var socket = m_Socket;
                if (socket != null)
                {
                    var onReleaseSocket = m_ReleaseAction;
                    if (onReleaseSocket != null)
                        onReleaseSocket(this, socket);
                }
            }
            catch (Exception)
            { }
        }

        public bool BeginReceive(Action<object> onComplete)
        {
            ValidateNotDisposed();

            var onReceiveResponse = m_OnReceiveResponse;
            if ((onReceiveResponse != null) &&
                Interlocked.CompareExchange(ref m_ReceiveState, RedisConstants.One, RedisConstants.Zero) == RedisConstants.Zero)
            {
                try
                {
                    var reader = new RedisContinuousReader(this);

                    var prevReader = Interlocked.Exchange(ref m_Reader, reader);
                    if (prevReader != null)
                        prevReader.Dispose();

                    reader.BeginReceive((sr) =>
                        {
                            Interlocked.Exchange(ref m_ReceiveState, RedisConstants.Zero);
                            if (onComplete != null)
                                onComplete(this);
                        },
                        (response) =>
                        {
                            onReceiveResponse(response);
                        });

                    return true;
                }
                catch (Exception)
                {
                    Interlocked.Exchange(ref m_ReceiveState, RedisConstants.Zero);
                    throw;
                }
            }
            return false;
        }

        public void EndReceive()
        {
            ValidateNotDisposed();

            if (Interlocked.Read(ref m_ReceiveState) != RedisConstants.Zero)
            {
                var reader = Interlocked.Exchange(ref m_Reader, null);

                if (reader != null) reader.Dispose();
                Interlocked.Exchange(ref m_ReceiveState, RedisConstants.Zero);
            }
        }

        #endregion Methods
    }
}
