using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Sweet.Redis
{
    public class RedisPubSubChannel
    {
        #region ActionBag

        private class ActionBag : List<Action<RedisPubSubMessage>>
        {
            #region .Ctors

            public ActionBag()
            { }

            public ActionBag(IList<Action<RedisPubSubMessage>> items)
                : base(items)
            { }

            #endregion .Ctors
        }

        #endregion ActionBag

        #region SubscriptionList

        private class SubscriptionList
        {
            #region Field Members

            private readonly object m_SyncObj = new object();
            private Dictionary<string, ActionBag> m_Subscriptions = new Dictionary<string, ActionBag>();

            #endregion Field Members

            #region Methods

            public ActionBag CallbacksOf(string channel)
            {
                if (!String.IsNullOrEmpty(channel))
                {
                    lock (m_SyncObj)
                    {
                        ActionBag callbacks;
                        if (m_Subscriptions.TryGetValue(channel, out callbacks) &&
                            callbacks != null && callbacks.Count > 0)
                            return new ActionBag(callbacks);
                    }
                }
                return null;
            }

            public IDictionary<string, ActionBag> Subscriptions()
            {
                lock (m_SyncObj)
                {
                    if (m_Subscriptions.Count > 0)
                    {
                        var result = new Dictionary<string, ActionBag>();
                        foreach (var kvp in m_Subscriptions)
                            result[kvp.Key] = new ActionBag(kvp.Value);

                        return result;
                    }
                }
                return null;
            }

            public bool Exists(string channel, Action<RedisPubSubMessage> callback)
            {
                if (!String.IsNullOrEmpty(channel) && callback != null)
                {
                    ActionBag callbacks;
                    lock (m_SyncObj)
                    {
                        if (m_Subscriptions.TryGetValue(channel, out callbacks) &&
                            callbacks != null && callbacks.Count > 0)
                        {
                            var minfo = callback.Method;
                            return callbacks.FindIndex(c => c.Method == minfo) > -1;
                        }
                    }
                }
                return false;
            }

            public bool Exists(string channel)
            {
                if (!String.IsNullOrEmpty(channel))
                {
                    lock (m_SyncObj)
                    {
                        return m_Subscriptions.ContainsKey(channel);
                    }
                }
                return false;
            }

            public bool HasCallbacks(string channel)
            {
                if (!String.IsNullOrEmpty(channel))
                {
                    ActionBag callbacks;
                    lock (m_SyncObj)
                    {
                        if (m_Subscriptions.TryGetValue(channel, out callbacks))
                            return (callbacks != null && callbacks.Count > 0);
                    }
                }
                return false;
            }

            public bool IsEmpty()
            {
                lock (m_SyncObj)
                {
                    foreach (var kvp in m_Subscriptions)
                    {
                        var callbacks = kvp.Value;
                        if (callbacks != null && callbacks.Count > 0)
                            return false;
                    }
                }
                return true;
            }

            public bool Register(string channel, Action<RedisPubSubMessage> callback)
            {
                if (String.IsNullOrEmpty(channel))
                    return false;

                var result = false;

                ActionBag bag;
                if (!m_Subscriptions.TryGetValue(channel, out bag))
                {
                    lock (m_SyncObj)
                    {
                        if (!m_Subscriptions.TryGetValue(channel, out bag))
                        {
                            bag = new ActionBag();
                            bag.Add(callback);
                            m_Subscriptions[channel] = bag;
                            result = true;
                        }
                    }
                }

                if (!result)
                {
                    lock (m_SyncObj)
                    {
                        var minfo = callback.Method;

                        var index = bag.FindIndex(c => c.Method == minfo);
                        if (index == -1)
                        {
                            bag.Add(callback);
                            result = true;
                        }
                    }
                }

                return result;
            }

            public bool Register(string channel, ActionBag callbacks)
            {
                if (String.IsNullOrEmpty(channel) || callbacks == null ||
                    callbacks.Count == 0)
                    return false;

                var result = false;

                ActionBag bag;
                var processed = false;
                if (!m_Subscriptions.TryGetValue(channel, out bag))
                {
                    lock (m_SyncObj)
                    {
                        if (!m_Subscriptions.TryGetValue(channel, out bag))
                        {
                            processed = true;
                            bag = new ActionBag();

                            foreach (var callback in callbacks)
                            {
                                if (callback != null)
                                {
                                    var minfo = callback.Method;

                                    var index = bag.FindIndex(c => c.Method == minfo);
                                    if (index == -1)
                                    {
                                        bag.Add(callback);
                                        result = true;
                                    }
                                }
                            }

                            if (result)
                                m_Subscriptions[channel] = bag;
                        }
                    }
                }

                if (!processed)
                {
                    lock (m_SyncObj)
                    {
                        foreach (var callback in callbacks)
                        {
                            if (callback != null)
                            {
                                var minfo = callback.Method;

                                var index = bag.FindIndex(c => c.Method == minfo);
                                if (index == -1)
                                {
                                    bag.Add(callback);
                                    result = true;
                                }
                            }
                        }
                    }
                }

                return result;
            }

            public ActionBag Drop(string channel)
            {
                if (!String.IsNullOrEmpty(channel))
                {
                    lock (m_SyncObj)
                    {
                        ActionBag callbacks;
                        if (m_Subscriptions.TryGetValue(channel, out callbacks))
                            m_Subscriptions.Remove(channel);
                        return callbacks;
                    }
                }
                return null;
            }

            public bool Unregister(string channel)
            {
                if (!String.IsNullOrEmpty(channel))
                {
                    lock (m_SyncObj)
                    {
                        ActionBag callbacks;
                        if (m_Subscriptions.TryGetValue(channel, out callbacks))
                        {
                            m_Subscriptions.Remove(channel);
                            if (callbacks != null && callbacks.Count > 0)
                                callbacks.Clear();
                            return true;
                        }
                    }
                }
                return false;
            }

            public bool Unregister(string channel, Action<RedisPubSubMessage> callback)
            {
                if (callback != null && !String.IsNullOrEmpty(channel))
                {
                    lock (m_SyncObj)
                    {
                        ActionBag callbacks;
                        if (m_Subscriptions.TryGetValue(channel, out callbacks) &&
                            callbacks != null && callbacks.Count > 0)
                        {
                            var minfo = callback.Method;

                            var index = callbacks.FindIndex(c => c.Method == minfo);
                            if (index > -1)
                            {
                                callbacks.RemoveAt(index);
                                return true;
                            }
                        }
                    }
                }
                return false;
            }

            public bool Unregister(Action<RedisPubSubMessage> callback)
            {
                if (callback == null)
                    return false;

                var result = false;
                var minfo = callback.Method;

                lock (m_SyncObj)
                {
                    foreach (var kvp in m_Subscriptions)
                    {
                        var callbacks = kvp.Value;
                        var count = callbacks.Count;

                        for (var i = count - 1; i > -1; i--)
                        {
                            if (callbacks[i].Method == minfo)
                            {
                                callbacks.RemoveAt(i);
                                result = true;
                            }
                        }
                    }
                }
                return result;
            }

            public void UnregisterAll()
            {
                lock (m_SyncObj)
                {
                    foreach (var kvp in m_Subscriptions)
                        if (kvp.Value != null)
                            kvp.Value.Clear();

                    m_Subscriptions.Clear();
                }
            }

            public void Invoke(RedisPubSubMessage msg)
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

        #endregion SubscriptionList

        #region Field Members

        private long m_Disposed;
        private RedisConnectionPool m_Pool;
        private RedisConnection m_Connection;
        private Semaphore m_ConnectionSync = new Semaphore(1, 1);

        private readonly object m_SubscriptionLock = new object();
        private readonly object m_PSubscriptionLock = new object();

        private SubscriptionList m_Subscriptions = new SubscriptionList();
        private SubscriptionList m_PSubscriptions = new SubscriptionList();

        private SubscriptionList m_PendingSubscriptions = new SubscriptionList();
        private SubscriptionList m_PendingPSubscriptions = new SubscriptionList();

        #endregion Field Members

        #region .Ctors

        public RedisPubSubChannel(RedisConnectionPool pool)
        {
            m_Pool = pool;
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
                        connection = new RedisPubSubConnection(m_Pool.Name, m_Pool.Settings,
                            (receivedConnection, response) =>
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

            ((RedisPubSubConnection)connection).BeginReceive();

            return connection;
        }

        private static RedisPubSubMessage ToPubSubMessage(RedisPubSubResponse response)
        {
            if (response.Type == RedisPubSubResponseType.PMessage)
                return new RedisPubSubMessage(RedisPubSubMessageType.PMessage, response.Channel, response.Pattern, response.Data as byte[]);

            if (response.Type == RedisPubSubResponseType.Message)
                return new RedisPubSubMessage(RedisPubSubMessageType.Message, response.Channel, response.Pattern, response.Data as byte[]);

            return RedisPubSubMessage.Empty;
        }

        private void ResponseReceived(RedisResponse response)
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
