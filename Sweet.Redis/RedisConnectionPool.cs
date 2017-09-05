using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;

namespace Sweet.Redis
{
    public class RedisConnectionPool : RedisDisposable
    {
        #region RedisConnectionPoolMember

        private class RedisConnectionPoolMember : RedisDisposable
        {
            private Socket m_Socket;

            public DateTime PooledTime { get; private set; }
            public Socket Socket
            {
                get { return m_Socket; }
                set
                {
                    Interlocked.Exchange(ref m_Socket, value);
                }
            }

            public RedisConnectionPoolMember(Socket socket)
            {
                Socket = socket;
                PooledTime = DateTime.UtcNow;
            }

            public Socket ReleaseSocket()
            {
                ValidateNotDisposed();
                return ReleaseSocketInternal();
            }

            private Socket ReleaseSocketInternal()
            {
                var socket = Interlocked.Exchange(ref m_Socket, null);
                GC.SuppressFinalize(this);
                return socket;
            }

            protected override void OnDispose(bool disposing)
            {
                var socket = ReleaseSocketInternal();
                if (socket != null)
                {
                    Task.Factory.StartNew(obj =>
                    {
                        var sock = obj as Socket;
                        if (sock != null)
                        {
                            try
                            {
                                socket.Dispose();
                            }
                            catch (Exception)
                            { }
                        }
                    }, socket);
                }
            }
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
        private LinkedList<RedisConnectionPoolMember> m_MemberStore = new LinkedList<RedisConnectionPoolMember>();

        #endregion Field Readonly Members

        private long m_InitCount;
        private string m_Name;
        private int m_PurgingIdles;
        private long m_WaitRetryCount;

        #endregion Field Members

        #region Properties

        public long Count
        {
            get { return Interlocked.Read(ref m_InitCount); }
        }

        public string Name
        {
            get { return m_Name; }
        }

        public RedisSettings Settings
        {
            get { return m_Settings; }
        }

        #endregion Properties

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
        }

        #endregion Destructors

        #region Member Methods

        protected override void ValidateNotDisposed()
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
                var count = store.Count;
                if (count > 0)
                {
                    RedisConnectionPoolMember[] members;
                    lock (m_MemberStoreLock)
                    {
                        members = store.ToArray();
                        store.Clear();
                    }

                    if (members != null && members.Length > 0)
                    {
                        members.AsParallel().ForAll(m => m.Dispose());
                    }
                }
            }
        }

        private Socket Enqueue()
        {
            lock (m_MemberStoreLock)
            {
                var store = m_MemberStore;
                if (store != null)
                {
                    Socket socket = null;
                    do
                    {
                        var node = store.First;
                        if (node == null)
                            return null;

                        store.RemoveFirst();

                        socket = node.Value.ReleaseSocket();
                        if (IsConnected(socket, 100))
                            return socket;
                    }
                    while (socket == null);
                }
            }
            return null;
        }

        public IRedisDb GetDb(int db = 0)
        {
            return new RedisDb(this, db);
        }

        internal IRedisConnection Connect()
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

                var socket = Enqueue();

                if ((socket != null) ||
                    (Interlocked.Read(ref m_InitCount) < m_Settings.MaxCount))
                {
                    Interlocked.Exchange(ref m_WaitRetryCount, 0);
                    return NewConnection(socket, true);
                }

                remainingTime = timeout - (int)(DateTime.UtcNow - now).TotalMilliseconds;
            }
            throw new RedisException("Connection timeout occured while trying to connect");
        }

        private bool IsConnected(Socket socket, int poll = -1)
        {
            if (socket == null || !socket.Connected)
                return false;
            return !((poll > -1) && socket.Poll(poll, SelectMode.SelectRead) && (socket.Available == 0));
        }

        private IRedisConnection NewConnection(Socket socket, bool connectImmediately = true)
        {
            socket = IsConnected(socket) ? socket : null;

            var conn = new RedisConnection(this, m_Settings, Release, socket, connectImmediately);

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

        private void Release(RedisConnection conn, Socket socket)
        {
            ValidateNotDisposed();

            if (conn != null)
            {
                m_MaxCountSync.Release();

                if (conn.Disposed)
                {
                    if (IsConnected(socket))
                    {
                        var store = m_MemberStore;
                        if (store != null)
                        {
                            lock (m_MemberStoreLock)
                            {
                                store.AddLast(new RedisConnectionPoolMember(socket));
                            }
                        }
                    }

                    DecCount();
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

                lock (m_MemberStoreLock)
                {
                    var store = m_MemberStore;
                    if (store != null)
                    {
                        var node = store.First;
                        while (node != null)
                        {
                            try
                            {
                                var m = node.Value;
                                if ((m == null) || !IsConnected(m.Socket) ||
                                    ((timeout > 0) && (now - m.PooledTime).TotalSeconds >= timeout))
                                {
                                    store.Remove(node);
                                    m.Dispose();
                                }
                            }
                            catch (Exception)
                            { }
                            node = node.Next;
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
                    s_PurgeTimer = new Timer(RedisConnectionPool.PurgeIdles, null,
                        RedisConstants.IdleTimerPeriod, RedisConstants.IdleTimerPeriod);
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

        private static void PurgeIdles(object state)
        {
            var pools = GetPoolList();
            if (pools != null)
            {
                pools.AsParallel().ForAll(p => p.PurgeIdles());
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
