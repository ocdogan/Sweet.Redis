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
using System.Threading;

namespace Sweet.Redis
{
    public class RedisMonitorChannel
    {
        #region RedisMonitorSubscriptions

        private class RedisMonitorSubscriptions : RedisCallbackHub<RedisMonitorMessage>
        {
            #region Methods

            public override void Invoke(RedisMonitorMessage msg)
            {
                if (!msg.IsEmpty)
                {
                    var callbacks = CallbacksOf("*");
                    if (callbacks != null && callbacks.Count > 0)
                    {
                        foreach (var callback in callbacks)
                        {
                            try
                            {
                                callback.InvokeAsync(msg);
                            }
                            catch (Exception)
                            { }
                        }
                    }
                }
            }

            #endregion Methods
        }

        #endregion RedisMonitorSubscriptions

        #region Field Members

        private long m_Disposed;
        private RedisConnectionPool m_Pool;
        private RedisConnection m_Connection;
        private Semaphore m_ConnectionSync = new Semaphore(1, 1);

        private readonly object m_SubscriptionLock = new object();

        private RedisMonitorSubscriptions m_Subscriptions = new RedisMonitorSubscriptions();

        #endregion Field Members

        #region .Ctors

        public RedisMonitorChannel(RedisConnectionPool pool)
        {
            m_Pool = pool;
        }

        #endregion .Ctors

        #region Destructors

        ~RedisMonitorChannel()
        {
            Dispose(false);
        }

        internal void Dispose()
        {
            Dispose(true);
        }

        private void Dispose(bool disposing)
        {
            if (SetDisposed())
                return;

            if (disposing)
                GC.SuppressFinalize(this);

            m_ConnectionSync.Close();

            Interlocked.Exchange(ref m_Pool, null);

            var connection = Interlocked.Exchange(ref m_Connection, null);
            if (connection != null)
                connection.Dispose();
        }

        #endregion Destructors

        #region Properties

        internal bool Disposed
        {
            get { return Interlocked.Read(ref m_Disposed) != 0; }
        }

        #endregion Properties

        #region Methods

        private bool SetDisposed()
        {
            return Interlocked.Exchange(ref m_Disposed, RedisConstants.True) != RedisConstants.False;
        }

        private void ValidateNotDisposed()
        {
            if (Disposed)
                throw new RedisException(GetType().Name + " is disposed");
        }

        private RedisConnection Connect()
        {
            ValidateNotDisposed();

            var connection = m_Connection;
            if (connection == null)
            {
                var settings = m_Pool.Settings;

                var timeout = settings.ConnectionTimeout;
                timeout = timeout <= 0 ? RedisConstants.MaxConnectionTimeout : timeout;

                var now = DateTime.UtcNow;
                var remainingTime = timeout;

                var retryCount = 0;

                while (remainingTime > 0)
                {
                    var signaled = m_ConnectionSync.WaitOne(settings.WaitTimeout);
                    if (!signaled)
                    {
                        retryCount++;
                        if (retryCount > settings.WaitRetryCount)
                            throw new RedisException("Wait retry count exited the given maximum limit");
                    }

                    try
                    {
                        connection = new RedisContinuousReaderConnection(m_Pool.Name, m_Pool.Settings,
                            (response) =>
                            {
                                ResponseReceived(response);
                            },
                            (disposedConnection, releasedSocket) =>
                            {
                                var innerConnection = Interlocked.CompareExchange(ref m_Connection, null, disposedConnection);
                                if (innerConnection != null)
                                    innerConnection.Dispose();
                            },
                            true);

                        var prevConnection = Interlocked.Exchange(ref m_Connection, connection);
                        if (prevConnection != null && prevConnection != m_Connection)
                            prevConnection.Dispose();

                        break;
                    }
                    catch (Exception)
                    { }

                    remainingTime = timeout - (int)(DateTime.UtcNow - now).TotalMilliseconds;
                }
            }

            if (connection != null && !connection.Connected)
                connection.Connect();

            ((RedisContinuousReaderConnection)connection).BeginReceive();

            return connection;
        }

        private void ResponseReceived(IRedisResponse response)
        {
            var monitorMsg = RedisMonitorMessage.ToMonitorMessage(response);
            if (!monitorMsg.IsEmpty)
            {
                 var subscriptions = m_Subscriptions;
                 if (subscriptions != null)
                     subscriptions.Invoke(monitorMsg);
            }
        }

        private void SendAsync(byte[] cmd, params byte[][] parameters)
        {
            Action action = () =>
            {
                var connection = Connect();
                if (connection != null && connection.Connected)
                {
                    var pubSubCmd = new RedisCommand(0, cmd, parameters);
                    connection.SendAsync(pubSubCmd)
                        .ContinueWith(t => pubSubCmd.Dispose());
                }
            };
            action.InvokeAsync();
        }

        public void Subscribe(Action<RedisMonitorMessage> callback)
        {
            if (callback == null)
                throw new ArgumentNullException("callback");

            ValidateNotDisposed();

            var subscriptions = m_Subscriptions;
            if (subscriptions != null)
            {
                lock (m_SubscriptionLock)
                {
                    if (!subscriptions.Exists("*"))
                    {
                        subscriptions.Register("*", callback);
                    }
                }

                SendAsync(RedisCommands.Monitor);
            }
        }

        public void Monitor()
        {
            ValidateNotDisposed();
            SendAsync(RedisCommands.Monitor);
        }

        public void Quit()
        {
            SendAsync(RedisCommands.Quit);
        }

        public void UnregisterSubscription(Action<RedisMonitorMessage> callback)
        {
            lock (m_SubscriptionLock)
            {
                m_Subscriptions.Unregister(callback);
            }
        }

        #endregion Methods
    }
}
