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
    public class RedisConnectionPool : RedisConnectionProvider
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

        private RedisConnectionPoolMember m_MemberStoreTail;
        private readonly object m_MemberStoreLock = new object();
        private LinkedList<RedisConnectionPoolMember> m_MemberStore = new LinkedList<RedisConnectionPoolMember>();

        #endregion Field Readonly Members

        private int m_PurgingIdles;

        private readonly object m_PubSubChannelLock = new object();
        private RedisPubSubChannel m_PubSubChannel;

        private readonly object m_MonitorChannelLock = new object();
        private RedisMonitorChannel m_MonitorChannel;

        #endregion Field Members

        #region .Ctors

        public RedisConnectionPool(string name)
            : this(name, RedisSettings.Default)
        { }

        public RedisConnectionPool(string name, RedisSettings settings)
            : base(name, settings)
        {
            Register(this);
        }

        #endregion .Ctors

        #region Destructors

        protected override void OnDispose(bool disposing)
        {
            RedisConnectionPool.Unregister(this);
            CloseStore();

            base.OnDispose(disposing);

            var monitorChannel = Interlocked.Exchange(ref m_MonitorChannel, null);
            if (monitorChannel != null)
                monitorChannel.Dispose();

            var pubSubChannel = Interlocked.Exchange(ref m_PubSubChannel, null);
            if (pubSubChannel != null)
                pubSubChannel.Dispose();
        }

        #endregion Destructors

        #region Properties

        public RedisMonitorChannel MonitorChannel
        {
            get
            {
                ValidateNotDisposed();

                var channel = m_MonitorChannel;
                if (channel == null)
                {
                    lock (m_MonitorChannelLock)
                    {
                        channel = m_MonitorChannel;
                        if (channel == null)
                        {
                            channel = new RedisMonitorChannel(this);
                            Interlocked.Exchange(ref m_MonitorChannel, channel);
                        }
                    }
                }
                return channel;
            }
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
            get { return GetSettings(); }
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
            RedisConnectionPoolMember tail;
            lock (m_MemberStoreLock)
            {
                tail = Interlocked.Exchange(ref m_MemberStoreTail, null);
            }

            if (tail != null)
            {
                tail.Dispose();
            }

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

        protected override RedisSocket Enqueue(int db)
        {
            var store = m_MemberStore;
            if (store != null)
            {
                lock (m_MemberStoreLock)
                {
                    RedisSocket socket = null;

                    var member = m_MemberStoreTail;
                    if (member != null)
                    {
                        try
                        {
                            if (member.Db == db)
                            {
                                socket = member.ReleaseSocket();

                                m_MemberStoreTail = null;
                                if (socket.IsConnected())
                                    return socket;

                                socket.DisposeSocket();
                            }
                        }
                        catch (Exception)
                        { }
                    }

                    var node = store.First;
                    while (node != null)
                    {
                        try
                        {
                            member = node.Value;
                            if (member.Db == db)
                            {
                                socket = member.ReleaseSocket();

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

        protected override IRedisConnection NewConnection(RedisSocket socket, int db, bool connectImmediately = true)
        {
            var settings = GetSettings() ?? RedisSettings.Default;
            return new RedisDbConnection(this.Name, settings, this.OnRelease, db,
                                           socket.IsConnected() ? socket : null, connectImmediately);
        }

        protected override void CompleteRelease(IRedisConnection conn, RedisSocket socket)
        {
            var db = 0;
            if (conn is IRedisDbConnection)
                db = ((IRedisDbConnection)conn).Db;

            var member = new RedisConnectionPoolMember(socket, db);
            lock (m_MemberStoreLock)
            {
                var prevTail = Interlocked.Exchange(ref m_MemberStoreTail, member);
                if (prevTail != null)
                {
                    var store = m_MemberStore;
                    if (store != null)
                    {
                        store.AddLast(prevTail);
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
                var timeout = (GetSettings() ?? RedisSettings.Default).IdleTimeout;

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
