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
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Sweet.Redis
{
    public class RedisConnectionPool : RedisDisposable
    {
        #region RedisConnectionPoolMember

        private class RedisConnectionPoolMember : RedisDisposable
        {
            #region Field Members

            private RedisSocket m_Socket;

            #endregion Field Members

            #region .Ctors

            public RedisConnectionPoolMember(RedisSocket socket, int db)
            {
                Db = db;
                Socket = socket;
                PooledTime = DateTime.UtcNow;
            }

            #endregion .Ctors

            #region Properties

            public int Db { get; private set; }

            public DateTime PooledTime { get; private set; }

            public RedisSocket Socket
            {
                get { return m_Socket; }
                set
                {
                    Interlocked.Exchange(ref m_Socket, value);
                }
            }

            #endregion Properties

            #region Methods

            public RedisSocket ReleaseSocket()
            {
                ValidateNotDisposed();
                return ReleaseSocketInternal();
            }

            private RedisSocket ReleaseSocketInternal()
            {
                var socket = Interlocked.Exchange(ref m_Socket, null);
                GC.SuppressFinalize(this);
                return socket;
            }

            protected override void OnDispose(bool disposing)
            {
                ReleaseSocketInternal().DisposeSocket();
            }

            #endregion Methods
        }

        #endregion RedisConnectionPoolMember

        #region Static Members

        #region Static Readonly Members

        private readonly static object s_PoolLock = new object();
        private readonly static List<RedisConnectionPool> s_Pools = new List<RedisConnectionPool>();

        #endregion Static Readonly Members

        private static Timer s_PurgeTimer;

        #endregion Static Members

        #region Field Members

        #region Field Readonly Members

        private readonly Semaphore m_MaxCountSync;
        private readonly RedisSettings m_Settings;
        private readonly object m_MemberStoreLock = new object();
        private readonly object m_PubSubChannelLock = new object();
        private LinkedList<RedisConnectionPoolMember> m_MemberStore = new LinkedList<RedisConnectionPoolMember>();

        #endregion Field Readonly Members

        private long m_InitCount;
        private string m_Name;
        private int m_PurgingIdles;
        private long m_WaitRetryCount;
        private RedisPubSubChannel m_PubSubChannel;

        #endregion Field Members

        #region .Ctors

        public RedisConnectionPool(string name)
            : this(name, new RedisSettings())
        { }

        public RedisConnectionPool(string name, RedisSettings settings)
        {
            if (settings == null)
                throw new ArgumentNullException("settings");

            m_Settings = settings;

            name = (name ?? String.Empty).Trim();
            m_Name = !String.IsNullOrEmpty(name) ? name : Guid.NewGuid().ToString("N").ToUpper();

            m_MaxCountSync = new Semaphore(settings.MaxCount, settings.MaxCount);

            Register(this);
        }

        #endregion .Ctors

        #region Destructors

        protected override void OnDispose(bool disposing)
        {
            RedisConnectionPool.Unregister(this);
            CloseStore();

            if (!disposing)
                m_MaxCountSync.Close();

            var channel = Interlocked.Exchange(ref m_PubSubChannel, null);
            if (channel != null)
                channel.Dispose();
        }

        #endregion Destructors

        #region Properties

        public long Count
        {
            get { return Interlocked.Read(ref m_InitCount); }
        }

        public string Name
        {
            get { return m_Name; }
        }

        public RedisPubSubChannel PubSubChannel
        {
            get
            {
                ValidateNotDisposed();

                var channel = m_PubSubChannel;
                if (channel == null)
                {
                    lock (m_PubSubChannelLock)
                    {
                        channel = m_PubSubChannel;
                        if (channel == null)
                        {
                            channel = new RedisPubSubChannel(this);
                            Interlocked.Exchange(ref m_PubSubChannel, channel);
                        }
                    }
                }
                return channel;
            }
        }

        public RedisSettings Settings
        {
            get { return m_Settings; }
        }

        #endregion Properties

        #region Member Methods

        public override void ValidateNotDisposed()
        {
            if (Disposed)
            {
                if (!String.IsNullOrEmpty(Name))
                    throw new ObjectDisposedException(Name);
                base.ValidateNotDisposed();
            }
        }

        private void CloseStore()
        {
            LinkedList<RedisConnectionPoolMember> store;
            lock (m_MemberStoreLock)
            {
                store = Interlocked.Exchange(ref m_MemberStore, null);
            }

            if (store != null)
            {
                RedisConnectionPoolMember[] members;
                lock (m_MemberStoreLock)
                {
                    members = store.ToArray();
                    store.Clear();
                }

                if (members.Length > 0)
                {
                    if (members != null && members.Length > 0)
                    {
                        members.AsParallel().ForAll(m => m.Dispose());
                    }
                }
            }
        }

        private RedisSocket Enqueue(int db)
        {
            var store = m_MemberStore;
            if (store != null)
            {
                lock (m_MemberStoreLock)
                {
                    RedisSocket socket = null;
                    var node = store.First;

                    while (node != null)
                    {
                        try
                        {
                            if (node.Value.Db == db)
                            {
                                socket = node.Value.ReleaseSocket();

                                store.Remove(node);
                                if (socket.IsConnected())
                                    return socket;

                                socket.DisposeSocket();
                            }
                        }
                        catch (Exception)
                        { }
                        finally
                        {
                            node = node.Next;
                        }
                    }
                }
            }
            return null;
        }

        public IRedisDb GetDb(int db = 0)
        {
            return new RedisDb(this, db);
        }

        internal IRedisDbConnection Connect(int db)
        {
            ValidateNotDisposed();

            var timeout = m_Settings.ConnectionTimeout;
            timeout = timeout <= 0 ? RedisConstants.MaxConnectionTimeout : timeout;

            var now = DateTime.UtcNow;
            var remainingTime = timeout;

            while (remainingTime > 0)
            {
                var signaled = m_MaxCountSync.WaitOne(m_Settings.WaitTimeout);
                if (!signaled)
                {
                    var retryCount = Interlocked.Increment(ref m_WaitRetryCount);
                    if (retryCount > m_Settings.WaitRetryCount)
                        throw new RedisException("Wait retry count exited the given maximum limit");
                }

                var socket = Enqueue(db);

                if ((socket != null) ||
                    (Interlocked.Read(ref m_InitCount) < m_Settings.MaxCount))
                {
                    Interlocked.Exchange(ref m_WaitRetryCount, 0);
                    return NewConnection(socket, db, true);
                }

                remainingTime = timeout - (int)(DateTime.UtcNow - now).TotalMilliseconds;
            }
            throw new RedisException("Connection timeout occured while trying to connect");
        }

        private IRedisDbConnection NewConnection(RedisSocket socket, int db, bool connectImmediately = true)
        {
            var conn = new RedisDbConnection(this.Name, m_Settings, this.Release, db,
                                           socket.IsConnected() ? socket : null, connectImmediately);

            Interlocked.Increment(ref m_InitCount);
            return conn;
        }

        private void DecCount()
        {
            var count = Interlocked.Decrement(ref m_InitCount);
            if (count < 0)
            {
                Interlocked.Increment(ref m_InitCount);
            }
        }

        private void Release(RedisConnection conn, RedisSocket socket)
        {
            ValidateNotDisposed();

            if (conn != null)
            {
                m_MaxCountSync.Release();

                if (conn.Disposed)
                {
                    try
                    {
                        if (!socket.IsConnected())
                            socket.DisposeSocket();
                        else
                        {
                            var store = m_MemberStore;
                            if (store != null)
                            {
                                lock (m_MemberStoreLock)
                                {
                                    var db = 0;
                                    if (conn is IRedisDbConnection)
                                        db = ((IRedisDbConnection)conn).Db;

                                    store.AddLast(new RedisConnectionPoolMember(socket, db));
                                }
                            }
                        }
                    }
                    finally
                    {
                        DecCount();
                    }
                }
            }
        }

        private void PurgeIdles()
        {
            var wasPurging = Interlocked.CompareExchange(ref m_PurgingIdles, 1, 0);
            if (wasPurging != 0)
                return;

            try
            {
                var now = DateTime.UtcNow;
                var timeout = m_Settings.IdleTimeout;

                var store = m_MemberStore;
                if (store != null)
                {
                    lock (m_MemberStoreLock)
                    {
                        var node = store.First;
                        while (node != null)
                        {
                            try
                            {
                                var m = node.Value;
                                if ((m == null) || !m.Socket.IsConnected(100) ||
                                    ((timeout > 0) && (now - m.PooledTime).TotalSeconds >= timeout))
                                {
                                    store.Remove(node);
                                    m.Dispose();
                                }
                            }
                            catch (Exception)
                            { }
                            finally
                            {
                                node = node.Next;
                            }
                        }
                    }
                }
            }
            finally
            {
                Interlocked.Exchange(ref m_PurgingIdles, 0);
            }
        }

        #endregion Member Methods

        #region Static Methods

        private static void Register(RedisConnectionPool pool)
        {
            lock (s_PoolLock)
            {
                s_Pools.Add(pool);
                if (s_PurgeTimer == null)
                {
                    s_PurgeTimer = new Timer((state) =>
                    {
                        var pools = GetPoolList();
                        if (pools != null && pools.Length > 0)
                        {
                            pools.AsParallel().ForAll(p => p.PurgeIdles());
                        }
                    }, null,
                        RedisConstants.ConnectionPurgePeriod,
                                             RedisConstants.ConnectionPurgePeriod);
                }
            }
        }

        private static void Unregister(RedisConnectionPool pool)
        {
            lock (s_PoolLock)
            {
                s_Pools.Remove(pool);
                if (s_Pools.Count == 0)
                {
                    var timer = Interlocked.Exchange(ref s_PurgeTimer, null);
                    if (timer != null)
                    {
                        timer.Dispose();
                    }
                }
            }
        }

        private static RedisConnectionPool[] GetPoolList()
        {
            lock (s_PoolLock)
            {
                if (s_Pools.Count > 0)
                {
                    return s_Pools.ToArray();
                }
            }
            return null;
        }

        #endregion Static Methods
    }
}
