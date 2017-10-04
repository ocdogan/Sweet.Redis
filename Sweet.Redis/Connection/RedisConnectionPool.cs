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

namespace Sweet.Redis
{
    public class RedisConnectionPool : RedisConnectionProvider, IRedisConnectionPool
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

        private RedisAsyncRequestQProcessor m_Processor;

        private RedisConnectionPoolMember m_MemberStoreTail;
        private readonly object m_MemberStoreLock = new object();
        private LinkedList<RedisConnectionPoolMember> m_MemberStore = new LinkedList<RedisConnectionPoolMember>();

        #endregion Field Readonly Members

        private int m_PurgingIdles;

        private readonly object m_PubSubChannelLock = new object();
        private RedisPubSubChannel m_PubSubChannel;

        private readonly object m_MonitorChannelLock = new object();
        private RedisMonitorChannel m_MonitorChannel;

        private RedisAsyncRequestQ m_AsycRequestQ;

        #endregion Field Members

        #region .Ctors

        public RedisConnectionPool(string name, RedisSettings settings = null)
            : base(name, settings)
        {
            Register(this);

            var thisSettings = Settings;

            m_AsycRequestQ = new RedisAsyncRequestQ(thisSettings.SendTimeout);
            m_Processor = new RedisAsyncRequestQProcessor(m_AsycRequestQ, thisSettings);
        }

        #endregion .Ctors

        #region Destructors

        protected override void OnDispose(bool disposing)
        {
            RedisConnectionPool.Unregister(this);
            CloseMemberStore();

            base.OnDispose(disposing);

            StopToProcessQ();

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

        public bool ProcessingQ
        {
            get
            {
                var processor = m_Processor;
                return processor != null && processor.Processing;
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

        public IRedisTransaction BeginTransaction(int db = 0)
        {
            return new RedisTransaction(this, db);
        }

        public IRedisDb GetDb(int db = 0)
        {
            return new RedisDb(this, db);
        }

        #region Processor Methods

        private void StartToProcessQ()
        {
            var processor = m_Processor;
            if (processor != null)
                processor.Start();
        }

        private void StopToProcessQ()
        {
            try
            {
                var processor = Interlocked.Exchange(ref m_Processor, null);
                if (processor != null)
                    processor.Stop();
            }
            finally
            {
                var queue = Interlocked.Exchange(ref m_AsycRequestQ, null);
                if (queue != null)
                    queue.Dispose();
            }
        }

        #endregion Processor Methods

        #region Connection Methods

        protected override IRedisConnection NewConnection(RedisSocket socket, int db, bool connectImmediately = true)
        {
            var settings = GetSettings() ?? RedisSettings.Default;
            return new RedisDbConnection(Name, settings, null, OnReleaseSocket, db,
                                           socket.IsConnected() ? socket : null, connectImmediately);
        }

        protected override void CompleteSocketRelease(IRedisConnection conn, RedisSocket socket)
        {
            EnqueueSocket(socket);
        }

        protected override void OnConnectionRetry(RedisConnectionRetryEventArgs e)
        {
            var settings = GetSettings() ?? RedisSettings.Default;
            if (settings.UseAsyncCompleter)
            {
                e.ThrowError = true;
                e.ContinueToSpin = true;
            }
            else if (e.CurrentRetryCount > 0)
            {
                e.ThrowError = false;
                e.ContinueToSpin = false;
            }
        }

        #endregion Connection Methods

        #region Member Store Methods

        protected void EnqueueSocket(RedisSocket socket)
        {
            if (socket != null)
            {
                var member = new RedisConnectionPoolMember(socket, socket.DbIndex);
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
        }

        protected override RedisSocket DequeueSocket(int db)
        {
            lock (m_MemberStoreLock)
            {
                var member = m_MemberStoreTail;
                if (member != null)
                {
                    try
                    {
                        if (member.Db == db)
                        {
                            var socket = member.ReleaseSocket();

                            m_MemberStoreTail = null;
                            if (socket.IsConnected())
                                return socket;

                            socket.DisposeSocket();
                        }
                    }
                    catch (Exception)
                    { }
                }
            }

            var store = m_MemberStore;
            if (store != null)
            {
                lock (m_MemberStoreLock)
                {
                    if (store.Count > 0)
                    {
                        RedisSocket socket = null;
                        RedisConnectionPoolMember member;

                        var node = store.First;
                        while (node != null)
                        {
                            var nextNode = node.Next;
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
                                node = nextNode;
                            }
                        }
                    }
                }
            }
            return null;
        }

        private void CloseMemberStore()
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

        private void PurgeIdles()
        {
            var wasPurging = Interlocked.CompareExchange(ref m_PurgingIdles, 1, 0);
            if (wasPurging != 0)
                return;

            try
            {
                var now = DateTime.UtcNow;
                var idleTimeout = (GetSettings() ?? RedisSettings.Default).IdleTimeout;

                lock (m_MemberStoreLock)
                {
                    var member = m_MemberStoreTail;
                    if ((member != null) && (!member.Socket.IsConnected(100) ||
                        ((idleTimeout > 0) && (now - member.PooledTime).TotalMilliseconds >= idleTimeout)))
                    {
                        Interlocked.Exchange(ref m_MemberStoreTail, null);
                        member.Dispose();
                    }
                }

                var store = m_MemberStore;
                if (store != null)
                {
                    lock (m_MemberStoreLock)
                    {
                        var node = store.First;
                        while (node != null)
                        {
                            var nextNode = node.Next;
                            try
                            {
                                var member = node.Value;
                                if ((member == null) || !member.Socket.IsConnected(100) ||
                                    ((idleTimeout > 0) && (now - member.PooledTime).TotalSeconds >= idleTimeout))
                                {
                                    store.Remove(node);
                                    if (member != null)
                                        member.Dispose();
                                }
                            }
                            catch (Exception)
                            { }
                            finally
                            {
                                node = nextNode;
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

        #endregion Member Store Methods

        #region Command Execution

        internal RedisResponse Execute(RedisCommand command, bool throwException = true)
        {
            if (command == null)
                throw new ArgumentNullException("command");

            ValidateNotDisposed();

            IRedisConnection connection = null;
            if ((GetSettings() ?? RedisSettings.Default).UseAsyncCompleter)
            {
                connection = Connect(command.DbIndex);
                if (connection == null)
                {
                    var asyncRequest = m_AsycRequestQ.Enqueue<RedisResponse>(command, RedisCommandExpect.Response, null);
                    StartToProcessQ();

                    return asyncRequest.Task.Result;
                }
            }

            using (connection = (connection ?? Connect(command.DbIndex)))
            {
                return command.Execute(connection, throwException);
            }
        }

        internal RedisRaw ExpectArray(RedisCommand command, bool throwException = true)
        {
            if (command == null)
                throw new ArgumentNullException("command");

            ValidateNotDisposed();

            IRedisConnection connection = null;
            if ((GetSettings() ?? RedisSettings.Default).UseAsyncCompleter)
            {
                connection = Connect(command.DbIndex);
                if (connection == null)
                {
                    var asyncRequest = m_AsycRequestQ.Enqueue<RedisRaw>(command, RedisCommandExpect.Array, null);
                    StartToProcessQ();

                    return asyncRequest.Task.Result;
                }
            }

            using (connection = (connection ?? Connect(command.DbIndex)))
            {
                return command.ExpectArray(connection, throwException);
            }
        }

        internal RedisString ExpectBulkString(RedisCommand command, bool throwException = true)
        {
            ValidateNotDisposed();

            IRedisConnection connection = null;
            if ((GetSettings() ?? RedisSettings.Default).UseAsyncCompleter)
            {
                connection = Connect(command.DbIndex);
                if (connection == null)
                {
                    var asyncRequest = m_AsycRequestQ.Enqueue<RedisString>(command, RedisCommandExpect.BulkString, null);
                    StartToProcessQ();

                    return asyncRequest.Task.Result;
                }
            }

            using (connection = (connection ?? Connect(command.DbIndex)))
            {
                return command.ExpectBulkString(connection, throwException);
            }
        }

        internal RedisBytes ExpectBulkStringBytes(RedisCommand command, bool throwException = true)
        {
            ValidateNotDisposed();

            IRedisConnection connection = null;
            if ((GetSettings() ?? RedisSettings.Default).UseAsyncCompleter)
            {
                connection = Connect(command.DbIndex);
                if (connection == null)
                {
                    var asyncRequest = m_AsycRequestQ.Enqueue<RedisBytes>(command, RedisCommandExpect.BulkStringBytes, null);
                    StartToProcessQ();

                    return asyncRequest.Task.Result;
                }
            }

            using (connection = (connection ?? Connect(command.DbIndex)))
            {
                return command.ExpectBulkStringBytes(connection, throwException);
            }
        }

        internal RedisDouble ExpectDouble(RedisCommand command, bool throwException = true)
        {
            ValidateNotDisposed();

            IRedisConnection connection = null;
            if ((GetSettings() ?? RedisSettings.Default).UseAsyncCompleter)
            {
                connection = Connect(command.DbIndex);
                if (connection == null)
                {
                    var asyncRequest = m_AsycRequestQ.Enqueue<RedisDouble>(command, RedisCommandExpect.Double, null);
                    StartToProcessQ();

                    return asyncRequest.Task.Result;
                }
            }

            using (connection = (connection ?? Connect(command.DbIndex)))
            {
                return command.ExpectDouble(connection, throwException);
            }
        }

        internal RedisBool ExpectGreaterThanZero(RedisCommand command, bool throwException = true)
        {
            ValidateNotDisposed();

            IRedisConnection connection = null;
            if ((GetSettings() ?? RedisSettings.Default).UseAsyncCompleter)
            {
                connection = Connect(command.DbIndex);
                if (connection == null)
                {
                    var asyncRequest = m_AsycRequestQ.Enqueue<RedisBool>(command, RedisCommandExpect.GreaterThanZero, null);
                    StartToProcessQ();

                    return asyncRequest.Task.Result;
                }
            }

            using (connection = (connection ?? Connect(command.DbIndex)))
            {
                return command.ExpectInteger(connection, throwException) > RedisConstants.Zero;
            }
        }

        internal RedisInteger ExpectInteger(RedisCommand command, bool throwException = true)
        {
            ValidateNotDisposed();

            IRedisConnection connection = null;
            if ((GetSettings() ?? RedisSettings.Default).UseAsyncCompleter)
            {
                connection = Connect(command.DbIndex);
                if (connection == null)
                {
                    var asyncRequest = m_AsycRequestQ.Enqueue<RedisInteger>(command, RedisCommandExpect.Integer, null);
                    StartToProcessQ();

                    return asyncRequest.Task.Result;
                }
            }

            using (connection = (connection ?? Connect(command.DbIndex)))
            {
                return command.ExpectInteger(connection, throwException);
            }
        }

        internal RedisMultiBytes ExpectMultiDataBytes(RedisCommand command, bool throwException = true)
        {
            ValidateNotDisposed();

            IRedisConnection connection = null;
            if ((GetSettings() ?? RedisSettings.Default).UseAsyncCompleter)
            {
                connection = Connect(command.DbIndex);
                if (connection == null)
                {
                    var asyncRequest = m_AsycRequestQ.Enqueue<RedisMultiBytes>(command, RedisCommandExpect.MultiDataBytes, null);
                    StartToProcessQ();

                    return asyncRequest.Task.Result;
                }
            }

            using (connection = (connection ?? Connect(command.DbIndex)))
            {
                return command.ExpectMultiDataBytes(connection, throwException);
            }
        }

        internal RedisMultiString ExpectMultiDataStrings(RedisCommand command, bool throwException = true)
        {
            ValidateNotDisposed();

            IRedisConnection connection = null;
            if ((GetSettings() ?? RedisSettings.Default).UseAsyncCompleter)
            {
                connection = Connect(command.DbIndex);
                if (connection == null)
                {
                    var asyncRequest = m_AsycRequestQ.Enqueue<RedisMultiString>(command, RedisCommandExpect.MultiDataStrings, null);
                    StartToProcessQ();

                    return asyncRequest.Task.Result;
                }
            }

            using (connection = (connection ?? Connect(command.DbIndex)))
            {
                return command.ExpectMultiDataStrings(connection, throwException);
            }
        }

        internal RedisVoid ExpectNothing(RedisCommand command, bool throwException = true)
        {
            ValidateNotDisposed();

            IRedisConnection connection = null;
            if ((GetSettings() ?? RedisSettings.Default).UseAsyncCompleter)
            {
                connection = Connect(command.DbIndex);
                if (connection == null)
                {
                    var asyncRequest = m_AsycRequestQ.Enqueue<RedisVoid>(command, RedisCommandExpect.NullableDouble, null);
                    StartToProcessQ();

                    return asyncRequest.Task.Result;
                }
            }

            using (connection = (connection ?? Connect(command.DbIndex)))
            {
                return command.ExpectNothing(connection, throwException);
            }
        }

        internal RedisNullableDouble ExpectNullableDouble(RedisCommand command, bool throwException = true)
        {
            ValidateNotDisposed();

            IRedisConnection connection = null;
            if ((GetSettings() ?? RedisSettings.Default).UseAsyncCompleter)
            {
                connection = Connect(command.DbIndex);
                if (connection == null)
                {
                    var asyncRequest = m_AsycRequestQ.Enqueue<RedisNullableDouble>(command, RedisCommandExpect.NullableDouble, null);
                    StartToProcessQ();

                    return asyncRequest.Task.Result;
                }
            }

            using (connection = (connection ?? Connect(command.DbIndex)))
            {
                return command.ExpectNullableDouble(connection, throwException);
            }
        }

        internal RedisNullableInteger ExpectNullableInteger(RedisCommand command, bool throwException = true)
        {
            ValidateNotDisposed();

            IRedisConnection connection = null;
            if ((GetSettings() ?? RedisSettings.Default).UseAsyncCompleter)
            {
                connection = Connect(command.DbIndex);
                if (connection == null)
                {
                    var asyncRequest = m_AsycRequestQ.Enqueue<RedisNullableInteger>(command, RedisCommandExpect.NullableInteger, null);
                    StartToProcessQ();

                    return asyncRequest.Task.Result;
                }
            }

            using (connection = (connection ?? Connect(command.DbIndex)))
            {
                return command.ExpectNullableInteger(connection, throwException);
            }
        }

        internal RedisBool ExpectOK(RedisCommand command, bool throwException = true)
        {
            ValidateNotDisposed();

            IRedisConnection connection = null;
            if ((GetSettings() ?? RedisSettings.Default).UseAsyncCompleter)
            {
                connection = Connect(command.DbIndex);
                if (connection == null)
                {
                    var asyncRequest = m_AsycRequestQ.Enqueue<RedisBool>(command, RedisCommandExpect.OK, "OK");
                    StartToProcessQ();

                    return asyncRequest.Task.Result;
                }
            }

            using (connection = (connection ?? Connect(command.DbIndex)))
            {
                return command.ExpectSimpleString(connection, "OK", throwException);
            }
        }

        internal RedisBool ExpectOne(RedisCommand command, bool throwException = true)
        {
            ValidateNotDisposed();

            IRedisConnection connection = null;
            if ((GetSettings() ?? RedisSettings.Default).UseAsyncCompleter)
            {
                connection = Connect(command.DbIndex);
                if (connection == null)
                {
                    var asyncRequest = m_AsycRequestQ.Enqueue<RedisBool>(command, RedisCommandExpect.One, null);
                    StartToProcessQ();

                    return asyncRequest.Task.Result;
                }
            }

            using (connection = (connection ?? Connect(command.DbIndex)))
            {
                return command.ExpectInteger(connection, throwException) == RedisConstants.One;
            }
        }

        internal RedisBool ExpectSimpleString(RedisCommand command, string expectedResult, bool throwException = true)
        {
            ValidateNotDisposed();

            IRedisConnection connection = null;
            if ((GetSettings() ?? RedisSettings.Default).UseAsyncCompleter)
            {
                connection = Connect(command.DbIndex);
                if (connection == null)
                {
                    var asyncRequest = m_AsycRequestQ.Enqueue<RedisBool>(command, RedisCommandExpect.OK, expectedResult);
                    StartToProcessQ();

                    return asyncRequest.Task.Result;
                }
            }

            using (connection = (connection ?? Connect(command.DbIndex)))
            {
                return command.ExpectSimpleString(connection, expectedResult, throwException);
            }
        }

        internal RedisString ExpectSimpleString(RedisCommand command, bool throwException = true)
        {
            ValidateNotDisposed();

            var connection = Connect(command.DbIndex);
            if (connection == null)
            {
                var asyncRequest = m_AsycRequestQ.Enqueue<RedisString>(command, RedisCommandExpect.SimpleString, null);
                StartToProcessQ();

                return asyncRequest.Task.Result;
            }

            using (connection = (connection ?? Connect(command.DbIndex)))
            {
                return command.ExpectSimpleString(connection, throwException);
            }
        }

        internal RedisBool ExpectSimpleStringBytes(RedisCommand command, byte[] expectedResult, bool throwException = true)
        {
            ValidateNotDisposed();

            IRedisConnection connection = null;
            if ((GetSettings() ?? RedisSettings.Default).UseAsyncCompleter)
            {
                connection = Connect(command.DbIndex);
                if (connection == null)
                {
                    var asyncRequest = m_AsycRequestQ.Enqueue<RedisBytes>(command, RedisCommandExpect.SimpleStringBytes, null);
                    StartToProcessQ();

                    return (asyncRequest.Task.Result == expectedResult);
                }
            }

            using (connection = (connection ?? Connect(command.DbIndex)))
            {
                return command.ExpectSimpleStringBytes(connection, expectedResult, throwException);
            }
        }

        internal RedisBytes ExpectSimpleStringBytes(RedisCommand command, bool throwException = true)
        {
            ValidateNotDisposed();

            IRedisConnection connection = null;
            if ((GetSettings() ?? RedisSettings.Default).UseAsyncCompleter)
            {
                connection = Connect(command.DbIndex);
                if (connection == null)
                {
                    var asyncRequest = m_AsycRequestQ.Enqueue<RedisBytes>(command, RedisCommandExpect.SimpleStringBytes, null);
                    StartToProcessQ();

                    return asyncRequest.Task.Result;
                }
            }

            using (connection = (connection ?? Connect(command.DbIndex)))
            {
                return command.ExpectSimpleStringBytes(connection, throwException);
            }
        }

        #endregion Command Execution

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
