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
    public class RedisPubSubChannel
    {
        #region RedisPubSubSubscriptions

        private class RedisPubSubSubscriptions : RedisCallbackHub<RedisPubSubMessage>
        {
            #region Methods

            public override void Invoke(RedisPubSubMessage msg)
            {
                if (!msg.IsEmpty &&
                    (msg.Type == RedisPubSubMessageType.Message ||
                    msg.Type == RedisPubSubMessageType.PMessage))
                {
                    var channel = msg.Type == RedisPubSubMessageType.Message ? msg.Channel : msg.Pattern;
                    if (!String.IsNullOrEmpty(channel))
                    {
                        var callbacks = CallbacksOf(channel);
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
            }

            #endregion Methods
        }

        #endregion RedisPubSubSubscriptions

        #region Field Members

        private long m_Disposed;
        private RedisConnectionPool m_Pool;
        private RedisContinuousConnectionProvider m_ConnectionProvider;

        private readonly object m_SubscriptionLock = new object();
        private readonly object m_PSubscriptionLock = new object();

        private RedisPubSubSubscriptions m_Subscriptions = new RedisPubSubSubscriptions();
        private RedisPubSubSubscriptions m_PSubscriptions = new RedisPubSubSubscriptions();

        private RedisPubSubSubscriptions m_PendingSubscriptions = new RedisPubSubSubscriptions();
        private RedisPubSubSubscriptions m_PendingPSubscriptions = new RedisPubSubSubscriptions();

        #endregion Field Members

        #region .Ctors

        internal RedisPubSubChannel(RedisConnectionPool pool)
        {
            m_Pool = pool;
            m_ConnectionProvider = new RedisContinuousConnectionProvider(pool.Name, ResponseReceived);
        }

        #endregion .Ctors

        #region Destructors

        ~RedisPubSubChannel()
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

            Interlocked.Exchange(ref m_Pool, null);

            var connectionProvider = Interlocked.Exchange(ref m_ConnectionProvider, null);
            if (connectionProvider != null)
                connectionProvider.Dispose();
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

        private static RedisPubSubMessage ToPubSubMessage(RedisPubSubResponse response)
        {
            if (response.Type == RedisPubSubResponseType.PMessage)
                return new RedisPubSubMessage(RedisPubSubMessageType.PMessage, response.Channel, response.Pattern, response.Data as byte[]);

            if (response.Type == RedisPubSubResponseType.Message)
                return new RedisPubSubMessage(RedisPubSubMessageType.Message, response.Channel, response.Pattern, response.Data as byte[]);

            return RedisPubSubMessage.Empty;
        }

        private void ResponseReceived(IRedisResponse response)
        {
            var pubSubResp = RedisPubSubResponse.ToPubSubResponse(response);
            if (!pubSubResp.IsEmpty)
            {
                switch (pubSubResp.Type)
                {
                    case RedisPubSubResponseType.Message:
                        {
                            var subscriptions = m_Subscriptions;
                            if (subscriptions != null &&
                                subscriptions.HasCallbacks(pubSubResp.Channel))
                                subscriptions.Invoke(ToPubSubMessage(pubSubResp));
                        }
                        break;
                    case RedisPubSubResponseType.PMessage:
                        {
                            var subscriptions = m_PSubscriptions;
                            if (subscriptions != null &&
                                subscriptions.HasCallbacks(pubSubResp.Pattern))
                                subscriptions.Invoke(ToPubSubMessage(pubSubResp));
                        }
                        break;
                    case RedisPubSubResponseType.Subscribe:
                        {
                            lock (m_SubscriptionLock)
                            {
                                var bag = m_PendingSubscriptions.Drop(pubSubResp.Channel);
                                m_Subscriptions.Register(pubSubResp.Channel, bag);
                            }
                        }
                        break;
                    case RedisPubSubResponseType.PSubscribe:
                        {
                            lock (m_PSubscriptionLock)
                            {
                                var bag = m_PendingPSubscriptions.Drop(pubSubResp.Pattern);
                                m_PSubscriptions.Register(pubSubResp.Pattern, bag);
                            }
                        }
                        break;
                    case RedisPubSubResponseType.Unsubscribe:
                        {
                            lock (m_SubscriptionLock)
                            {
                                if (String.IsNullOrEmpty(pubSubResp.Channel))
                                {
                                    m_PendingSubscriptions.UnregisterAll();
                                    m_Subscriptions.UnregisterAll();
                                }
                                else
                                {

                                    if (!m_Subscriptions.Unregister(pubSubResp.Channel))
                                        m_PendingSubscriptions.Unregister(pubSubResp.Channel);
                                }
                            }
                        }
                        break;
                    case RedisPubSubResponseType.PUnsubscribe:
                        {
                            lock (m_PSubscriptionLock)
                            {
                                if (String.IsNullOrEmpty(pubSubResp.Pattern))
                                {
                                    m_PendingPSubscriptions.UnregisterAll();
                                    m_PSubscriptions.UnregisterAll();
                                }
                                else
                                {
                                    if (!m_PSubscriptions.Unregister(pubSubResp.Pattern))
                                        m_PendingPSubscriptions.Unregister(pubSubResp.Pattern);
                                }
                            }
                        }
                        break;
                }
            }
        }

        public void PSubscribe(Action<RedisPubSubMessage> callback, string pattern, params string[] patterns)
        {
            if (callback == null)
                throw new ArgumentNullException("callback");

            if (pattern == null)
                throw new ArgumentNullException("pattern");

            ValidateNotDisposed();

            var subscriptions = m_PSubscriptions;
            if (subscriptions != null)
            {
                var pendingSubscriptions = m_PendingPSubscriptions;
                if (pendingSubscriptions != null)
                {
                    var newItems = new List<byte[]>();

                    lock (m_PSubscriptionLock)
                    {
                        if (!subscriptions.Exists(pattern))
                        {
                            newItems.Add(pattern.ToBytes());
                            pendingSubscriptions.Register(pattern, callback);
                        }
                    }

                    foreach (var ptrn in patterns)
                    {
                        lock (m_PSubscriptionLock)
                        {
                            if (!subscriptions.Exists(ptrn, callback))
                            {
                                newItems.Add(ptrn.ToBytes());
                                pendingSubscriptions.Register(ptrn, callback);
                            }
                        }
                    }

                    if (newItems.Count > 0)
                        SendAsync(RedisCommands.PSubscribe, newItems.ToArray());
                }
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

        public void PUnsubscribe(params string[] patterns)
        {
            if (patterns.Length == 0)
                SendAsync(RedisCommands.PUnsubscribe);
            else
                SendAsync(RedisCommands.PUnsubscribe, patterns.ToBytesArray());
        }

        public void Subscribe(Action<RedisPubSubMessage> callback, string channel, params string[] channels)
        {
            if (callback == null)
                throw new ArgumentNullException("callback");

            if (channel == null)
                throw new ArgumentNullException("pattern");

            ValidateNotDisposed();

            var subscriptions = m_Subscriptions;
            if (subscriptions != null)
            {
                var pendingSubscriptions = m_PendingSubscriptions;
                if (pendingSubscriptions != null)
                {
                    var newItems = new List<byte[]>();

                    lock (m_SubscriptionLock)
                    {
                        if (!subscriptions.Exists(channel))
                        {
                            newItems.Add(channel.ToBytes());
                            pendingSubscriptions.Register(channel, callback);
                        }
                    }

                    foreach (var chnl in channels)
                    {
                        lock (m_SubscriptionLock)
                        {

                            if (!subscriptions.Exists(chnl, callback))
                            {
                                newItems.Add(chnl.ToBytes());
                                pendingSubscriptions.Register(chnl, callback);
                            }
                        }
                    }

                    if (newItems.Count > 0)
                        SendAsync(RedisCommands.Subscribe, newItems.ToArray());
                }
            }
        }

        public void Unsubscribe(params string[] channels)
        {
            if (channels.Length == 0)
                SendAsync(RedisCommands.Unsubscribe);
            else
                SendAsync(RedisCommands.Unsubscribe, channels.ToBytesArray());
        }

        public void UnregisterPSubscription(Action<RedisPubSubMessage> callback)
        {
            lock (m_PSubscriptionLock)
            {
                m_PendingPSubscriptions.Unregister(callback);
                m_PSubscriptions.Unregister(callback);
            }
        }

        public void UnregisterPSubscription(string channel, Action<RedisPubSubMessage> callback)
        {
            lock (m_PSubscriptionLock)
            {
                m_PendingPSubscriptions.Unregister(channel, callback);
                m_PSubscriptions.Unregister(channel, callback);
            }
        }

        public void UnregisterSubscription(Action<RedisPubSubMessage> callback)
        {
            lock (m_SubscriptionLock)
            {
                m_PendingSubscriptions.Unregister(callback);
                m_Subscriptions.Unregister(callback);
            }
        }

        public void UnregisterSubscription(string channel, Action<RedisPubSubMessage> callback)
        {
            lock (m_SubscriptionLock)
            {
                m_PendingSubscriptions.Unregister(channel, callback);
                m_Subscriptions.Unregister(channel, callback);
            }
        }

        #endregion Methods
    }
}
