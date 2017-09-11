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

            public bool Exists(string channel)
            {
                if (!String.IsNullOrEmpty(channel))
                    return m_Subscriptions.ContainsKey(channel);
                return false;
            }

            public List<Action<RedisPubSubMessage>> CallbacksOf(string channel)
            {
                if (!String.IsNullOrEmpty(channel))
                {
                    lock (m_SyncObj)
                    {
                        ActionBag callbacks;
                        if (!m_Subscriptions.TryGetValue(channel, out callbacks) &&
                            callbacks != null && callbacks.Count == 0)
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

            public bool Register(string channel, Action<RedisPubSubMessage> callback)
            {
                if (String.IsNullOrEmpty(channel))
                    return false;

                var result = false;

                ActionBag callbacks;
                if (!m_Subscriptions.TryGetValue(channel, out callbacks))
                {
                    lock (m_SyncObj)
                    {
                        if (!m_Subscriptions.TryGetValue(channel, out callbacks))
                        {
                            callbacks = new ActionBag();
                            callbacks.Add(callback);
                            result = true;
                        }
                    }
                }

                if (!result)
                {
                    lock (m_SyncObj)
                    {
                        var minfo = callback.Method;

                        var index = callbacks.FindIndex(c => c.Method == minfo);
                        if (index == -1)
                        {
                            callbacks.Add(callback);
                            result = true;
                        }
                    }
                }

                return result;
            }

            public bool Unregister(string channel, Action<RedisPubSubMessage> callback)
            {
                if (String.IsNullOrEmpty(channel))
                    return false;

                var result = false;

                ActionBag callbacks;
                lock (m_SyncObj)
                {
                    if (!m_Subscriptions.TryGetValue(channel, out callbacks))
                    {
                        callbacks = new ActionBag();
                        callbacks.Add(callback);
                        result = true;
                    }
                }

                if (!result)
                {
                    var minfo = callback.Method;

                    lock (m_SyncObj)
                    {
                        var index = callbacks.FindIndex(c => c.Method == minfo);
                        if (index > -1)
                        {
                            callbacks.RemoveAt(index);
                            result = true;
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
                var channel = msg.Channel;
                if (!String.IsNullOrEmpty(channel))
                {
                    var callbacks = CallbacksOf(channel);
                    if (callbacks != null)
                    {
                        Task.Factory.StartNew(() =>
                        {
                            callbacks.ForEach(callback =>
                            {
                                try
                                {
                                    callback(msg);
                                }
                                catch (Exception)
                                { }
                            });
                        });
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
            return Interlocked.Exchange(ref m_Disposed, 1L) != 0L;
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
                        connection = new RedisConnection(m_Pool.Name, m_Pool.Settings, (c, s) =>
                        {
                            var conn2 = Interlocked.CompareExchange(ref m_Connection, null, c);
                            if (conn2 != null)
                                conn2.Dispose();
                        }, null, true);

                        break;
                    }
                    catch (Exception)
                    { }

                    remainingTime = timeout - (int)(DateTime.UtcNow - now).TotalMilliseconds;
                }
            }

            if (connection != null && !connection.Connected)
                connection.Connect();

            return connection;
        }

        public void PSubscribe(Action<RedisPubSubMessage> callback, string pattern, params string[] patterns)
        {
            if (callback == null)
                throw new ArgumentNullException("callback");

            if (pattern == null)
                throw new ArgumentNullException("pattern");

            ValidateNotDisposed();

            var newPatterns = new List<string>();

            if (!m_PSubscriptions.Exists(pattern))
            {
                newPatterns.Add(pattern);
                m_PendingPSubscriptions.Register(pattern, callback);
            }

            foreach (var ptrn in patterns)
                if (!m_PSubscriptions.Exists(ptrn, callback))
                {
                    newPatterns.Add(ptrn);
                    m_PendingPSubscriptions.Register(ptrn, callback);
                }

            if (newPatterns.Count > 0)
            {
                var connection = Connect();
                if (connection != null)
                {
                    Send(connection, RedisCommands.PSubscribe,
                         newPatterns.ToArray().ToBytesArray());
                }
            }
        }

        private long[] ToSubscriptionResult(RedisObject response)
        {
            if (response != null && response.Type == RedisObjectType.Array)
            {
                var items = response.Items;
                if (items != null && items.Count == 4)
                {
                    var last = items[3];
                    /* if (last != null && last.Type == RedisObjectType.Integer)
                        return (long)last.Data; */
                }
            }
            return new long[0];
        }

        private static void Send(IRedisConnection connection, byte[] cmd, params byte[][] parameters)
        {
            using (var rcmd = new RedisCommand(0, cmd, parameters))
            {
                connection.Send(cmd);
            }
        }

        public void PUnscribe(params string[] patterns)
        {
            throw new NotImplementedException();
        }

        public void Subscribe(Action<RedisPubSubMessage> callback, string channel, params string[] channels)
        {
            if (callback == null)
                throw new ArgumentNullException("callback");

            if (channel == null)
                throw new ArgumentNullException("pattern");

            ValidateNotDisposed();

            var newChannels = new List<string>();

            if (!m_Subscriptions.Exists(channel))
            {
                newChannels.Add(channel);
                m_PendingSubscriptions.Register(channel, callback);
            }

            foreach (var ptrn in channels)
                if (!m_Subscriptions.Exists(ptrn, callback))
                {
                    newChannels.Add(ptrn);
                    m_PendingSubscriptions.Register(ptrn, callback);
                }

            if (newChannels.Count > 0)
            {
                var connection = Connect();
                if (connection != null)
                {
                    Send(connection, RedisCommands.Subscribe,
                         newChannels.ToArray().ToBytesArray());
                }
            }
        }


        public void Unsubscribe(string channel, params string[] channels)
        {

        }

        public void UnregisterFromPSubscribe(Action<RedisPubSubMessage> callback)
        {
        }

        #endregion Methods
    }
}
