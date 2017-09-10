using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;

namespace Sweet.Redis
{
    public class RedisPubSubChannel
    {
        #region Field Members

        private long m_Disposed;
        private RedisConnectionPool m_Pool;
        private RedisConnection m_Connection;
        private Semaphore m_ConnectionSync = new Semaphore(1, 1);

        private readonly object m_SubscriptionsLock = new object();
        private readonly object m_PSubscriptionsLock = new object();

        private ConcurrentDictionary<string, List<Func<RedisPubSubMessage>>> m_Subscriptions =
            new ConcurrentDictionary<string, List<Func<RedisPubSubMessage>>>();

        private ConcurrentDictionary<string, List<Func<RedisPubSubMessage>>> m_PSubscriptions =
            new ConcurrentDictionary<string, List<Func<RedisPubSubMessage>>>();

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
                        }, 0, null, true);

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

        public long[] PSubscribe(Func<RedisPubSubMessage> callback, string pattern, params string[] patterns)
        {
            if (callback == null)
                throw new ArgumentNullException("callback");

            if (pattern == null)
                throw new ArgumentNullException("pattern");

            ValidateNotDisposed();

            var newPatterns = new List<string>();

            if (RegisterPSubsctiption(pattern, callback))
                newPatterns.Add(pattern);

            foreach (var ptrn in patterns)
                if (RegisterPSubsctiption(ptrn, callback))
                    newPatterns.Add(ptrn);

            var count = newPatterns.Count;
            if (count > 0)
            {
                var result = new long[count];
                for (var i = 0; i < count; i++)
                    result[i] = PSubscribe(newPatterns[i]);

                return result;
            }

            return new long[0];
        }

        private long PSubscribe(params string[] patterns)
        {
            if (patterns.Length > 0)
            {
                var connection = Connect();
                if (connection != null)
                {
                    var response = ExpectArray(connection, RedisCommands.PSubscribe, patterns.ToBytesArray());
                    return SubscriptionResult(response);
                }
            }
            return 0L;
        }

        private long SubscriptionResult(RedisObject response)
        {
            if (response != null && response.Type == RedisObjectType.Array)
            {
                var items = response.Items;
                if (items != null && items.Count == 4)
                {
                    var last = items[3];
                    if (last != null && last.Type == RedisObjectType.Integer)
                        return (long)last.Data;
                }
            }
            return 0L;
        }

        private static RedisObject ExpectArray(IRedisConnection connection, byte[] cmd, params byte[][] parameters)
        {
            using (var rcmd = new RedisCommand(0, cmd, parameters))
            {
                return rcmd.ExpectArray(connection, true);
            }
        }

        private bool RegisterPSubsctiption(string pattern, Func<RedisPubSubMessage> callback)
        {
            if (String.IsNullOrEmpty(pattern))
                return false;

            var result = false;

            List<Func<RedisPubSubMessage>> callbacks;
            if (!m_PSubscriptions.TryGetValue(pattern, out callbacks))
            {
                lock (m_PSubscriptionsLock)
                {
                    if (!m_PSubscriptions.TryGetValue(pattern, out callbacks))
                    {
                        callbacks = new List<Func<RedisPubSubMessage>>();
                        callbacks.Add(callback);
                        result = true;
                    }
                }
            }

            if (!result)
            {
                lock (m_PSubscriptionsLock)
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

        public long PUnscribe(params string[] patterns)
        {
            throw new NotImplementedException();
        }

        public long[] Subscribe(Func<RedisPubSubMessage> callback, string channel, params string[] channels)
        {
            if (callback == null)
                throw new ArgumentNullException("callback");

            if (channel == null)
                throw new ArgumentNullException("channel");

            ValidateNotDisposed();

            var newChannels = new List<string>();

            if (RegisterSubsctiption(channel, callback))
                newChannels.Add(channel);

            foreach (var chnl in channels)
                if (RegisterPSubsctiption(chnl, callback))
                    newChannels.Add(chnl);

            var count = newChannels.Count;
            if (count > 0)
            {
                var result = new long[count];
                for (var i = 0; i < count; i++)
                    result[i] = Subscribe(newChannels[i]);

                return result;
            }

            return new long[0];
        }

        private long Subscribe(params string[] patterns)
        {
            if (patterns.Length > 0)
            {
                var connection = Connect();
                if (connection != null)
                {
                    var response = ExpectArray(connection, RedisCommands.PSubscribe, patterns.ToBytesArray());
                    return SubscriptionResult(response);
                }
            }
            return 0L;
        }

        private bool RegisterSubsctiption(string channel, Func<RedisPubSubMessage> callback)
        {
            if (String.IsNullOrEmpty(channel))
                return false;

            var result = false;

            List<Func<RedisPubSubMessage>> callbacks;
            if (!m_Subscriptions.TryGetValue(channel, out callbacks))
            {
                lock (m_SubscriptionsLock)
                {
                    if (!m_Subscriptions.TryGetValue(channel, out callbacks))
                    {
                        callbacks = new List<Func<RedisPubSubMessage>>();
                        callbacks.Add(callback);
                        result = true;
                    }
                }
            }

            if (!result)
            {
                lock (m_SubscriptionsLock)
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
        public long Unsubscribe(string channel, params string[] channels)
        {
            throw new NotImplementedException();
        }

        public void UnregisterFromPSubscribe(Func<RedisPubSubMessage> callback)
        {
        }

        #endregion Methods
    }
}
