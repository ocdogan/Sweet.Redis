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
    public class RedisMonitorChannel : RedisInternalDisposable
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

        private RedisConnectionPool m_Pool;
        private RedisContinuousConnectionProvider m_ConnectionProvider;

        private readonly object m_SubscriptionLock = new object();
        private RedisMonitorSubscriptions m_Subscriptions = new RedisMonitorSubscriptions();

        #endregion Field Members

        #region .Ctors

        internal RedisMonitorChannel(RedisConnectionPool pool)
        {
            m_Pool = pool;
            m_ConnectionProvider = new RedisContinuousConnectionProvider(pool.Name, ResponseReceived);
        }

        #endregion .Ctors

        #region Destructors

        protected override void OnDispose(bool disposing)
        {
            Interlocked.Exchange(ref m_Pool, null);

            var connectionProvider = Interlocked.Exchange(ref m_ConnectionProvider, null);
            if (connectionProvider != null)
                connectionProvider.Dispose();
        }

        #endregion Destructors

        #region Methods

        private IRedisConnection Connect()
        {
            ValidateNotDisposed();

            var connectionProvider = m_ConnectionProvider;
            if (connectionProvider != null)
            {
                var connection = connectionProvider.Connect(-1);

                if (connection != null && !connection.Connected)
                    connection.Connect();

                ((RedisContinuousReaderConnection)connection).BeginReceive();

                return connection;
            }
            return null;
        }

        private void ResponseReceived(IRedisRawResponse response)
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
                    var pubSubCmd = new RedisCommand(0, cmd, RedisCommandType.SendNotReceive, parameters);
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
