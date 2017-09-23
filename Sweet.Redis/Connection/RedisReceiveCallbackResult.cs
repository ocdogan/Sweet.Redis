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
    internal class RedisReceiveCallbackResult : RedisDisposable
    {
        #region Field Members

        private long m_Ended;
        private long m_Waiting;
        private RedisSocket m_Socket;
        private ManualResetEvent m_Event;
        private IAsyncResult m_AsyncResult;

        #endregion Field Members

        #region .Ctors

        public RedisReceiveCallbackResult(RedisSocket socket, IAsyncResult asyncResult)
        {
            m_Socket = socket;
            m_AsyncResult = asyncResult;
            m_Event = new ManualResetEvent(false);
        }

        #endregion .Ctors

        #region Destructors

        protected override void OnDispose(bool disposing)
        {
            var @event = Interlocked.Exchange(ref m_Event, null);
            try
            {
                base.OnDispose(disposing);
                EndReceiveInternal();
            }
            finally
            {
                Interlocked.Exchange(ref m_Waiting, RedisConstants.Zero);
                if (@event != null)
                    using (@event)
                    {
                        @event.Set();
                    }
            }
        }

        #endregion Destructors

        #region Properties

        public IAsyncResult AsyncResult
        {
            get { return m_AsyncResult; }
        }

        public bool IsCompleted
        {
            get
            {
                var asyncResult = m_AsyncResult;
                return asyncResult != null && asyncResult.IsCompleted;
            }
        }

        public bool Waiting
        {
            get { return Interlocked.Read(ref m_Waiting) != RedisConstants.Zero; }
        }

        #endregion Properties

        #region Methods

        public int EndReceive()
        {
            ValidateNotDisposed();
            return EndReceiveInternal();
        }

        private int EndReceiveInternal()
        {
            if (Interlocked.CompareExchange(ref m_Ended, RedisConstants.One, RedisConstants.Zero) ==
                    RedisConstants.Zero)
            {
                var socket = Interlocked.Exchange(ref m_Socket, null);
                var asyncResult = Interlocked.Exchange(ref m_AsyncResult, null);

                if (asyncResult != null && (socket != null && socket.Connected))
                {
                    try
                    {
                        if (asyncResult.IsCompleted)
                            return socket.EndReceive(asyncResult);

                        var waitHandle = asyncResult.AsyncWaitHandle;
                        if (waitHandle != null)
                        {
                            var safeWaitHandle = waitHandle.SafeWaitHandle;
                            if (safeWaitHandle != null && !(safeWaitHandle.IsClosed || safeWaitHandle.IsInvalid))
                                waitHandle.Close();
                        }
                    }
                    catch (ObjectDisposedException)
                    {
                        return 0;
                    }
                }
            }
            return int.MinValue;
        }

        public bool WaitOne(int millisecondsTimeout = Timeout.Infinite)
        {
            ValidateNotDisposed();

            if (Interlocked.CompareExchange(ref m_Waiting, RedisConstants.One, RedisConstants.Zero) !=
                    RedisConstants.Zero)
                return false;

            try
            {
                if (millisecondsTimeout < 0)
                    return m_Event.WaitOne();
                return m_Event.WaitOne(millisecondsTimeout);
            }
            finally
            {
                Interlocked.Exchange(ref m_Waiting, RedisConstants.Zero);
            }
        }

        #endregion Methods
    }
}
