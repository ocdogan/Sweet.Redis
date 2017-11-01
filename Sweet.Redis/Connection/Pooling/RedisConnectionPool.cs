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
    public class RedisConnectionPool : RedisConnectionProvider, IRedisConnectionPool, IRedisHeartBeatProbe
    {
        #region RedisConnectionPoolMember

        private class RedisConnectionPoolMember : RedisDisposable, IRedisHeartBeatProbe
        {
            #region Field Members

            private long m_PulseState;
            private long m_PulseFailCount;

            private RedisSocket m_Socket;
            private RedisConnectionSettings m_Settings;

            private readonly object m_SyncRoot = new object();

            #endregion Field Members

            #region .Ctors

            public RedisConnectionPoolMember(RedisSocket socket, int dbIndex, RedisConnectionSettings settings)
            {
                DbIndex = dbIndex;
                Socket = socket;
                m_Settings = settings;
                PooledTime = DateTime.UtcNow;
            }

            #endregion .Ctors

            #region Destructors

            protected override void OnDispose(bool disposing)
            {
                Interlocked.Exchange(ref m_Settings, null);
                base.OnDispose(disposing);

                ReleaseSocketInternal().DisposeSocket();
            }

            #endregion Destructors

            #region Properties

            public int DbIndex { get; private set; }

            public DateTime PooledTime { get; private set; }

            public long PulseFailCount
            {
                get { return Interlocked.Read(ref m_PulseFailCount); }
            }

            public bool Pulsing
            {
                get { return Interlocked.Read(ref m_PulseState) != RedisConstants.Zero; }
            }

            public RedisRole Role
            {
                get { return m_Socket != null ? m_Socket.Role : RedisRole.Undefined; }
            }

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
                lock (m_SyncRoot)
                {
                    var socket = Interlocked.Exchange(ref m_Socket, null);
                    GC.SuppressFinalize(this);
                    return socket;
                }
            }

            public bool Pulse()
            {
                if (Interlocked.CompareExchange(ref m_PulseState, RedisConstants.One, RedisConstants.Zero) ==
                    RedisConstants.Zero)
                {
                    try
                    {
                        if (!Disposed)
                        {
                            lock (m_SyncRoot)
                            {
                                var socket = m_Socket;
                                if (socket.IsConnected(10))
                                {
                                    var settings = m_Settings;
                                    if (settings != null)
                                    {
                                        using (var cmd = new RedisCommand(-1, RedisCommandList.Ping))
                                        {
                                            cmd.ExpectSimpleString(new RedisSocketContext(socket, settings), RedisConstants.PONG);
                                        }
                                    }
                                }
                            }

                            Interlocked.Add(ref m_PulseFailCount, RedisConstants.Zero);
                            return true;
                        }
                    }
                    catch (Exception)
                    {
                        if (Interlocked.Read(ref m_PulseFailCount) < long.MaxValue)
                            Interlocked.Add(ref m_PulseFailCount, RedisConstants.One);
                    }
                    finally
                    {
                        Interlocked.Exchange(ref m_PulseState, RedisConstants.Zero);
                    }
                }
                return false;
            }

            public void ResetPulseFailCounter()
            {
                Interlocked.Add(ref m_PulseFailCount, RedisConstants.Zero);
            }

            public void PulseStateChanged(RedisCardioPulseStatus status)
            {
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

        private long m_PulseState;
        private bool m_ProbeAttached;
        private long m_PulseFailCount;

        private RedisAsyncRequestQProcessor m_Processor;

        private RedisConnectionPoolMember m_MemberStoreTail;
        private readonly object m_MemberStoreLock = new object();
        private LinkedList<RedisConnectionPoolMember> m_MemberStore = new LinkedList<RedisConnectionPoolMember>();

        private int m_PurgingIdles;

        private readonly object m_PubSubChannelLock = new object();
        private RedisPubSubChannel m_PubSubChannel;

        private readonly object m_MonitorChannelLock = new object();
        private RedisMonitorChannel m_MonitorChannel;

        private RedisAsyncRequestQ m_AsycRequestQ;

        #endregion Field Members

        #region Events

        public event EventHandler MonitorCompleted;
        public event EventHandler PubSubCompleted;
        public event Action<object, RedisCardioPulseStatus> PoolPulseStateChanged;
        public event Action<object, RedisCardioPulseStatus> PubSubPulseStateChanged;

        #endregion Events

        #region .Ctors

        public RedisConnectionPool(string name, RedisPoolSettings settings)
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
            Interlocked.Exchange(ref MonitorCompleted, null);
            Interlocked.Exchange(ref PubSubCompleted, null);
            Interlocked.Exchange(ref PubSubPulseStateChanged, null);
            Interlocked.Exchange(ref PoolPulseStateChanged, null);

            if (m_ProbeAttached)
                RedisCardio.Default.Detach(this);

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

        public int IdleCount
        {
            get
            {
                if (Disposed)
                    return 0;

                lock (m_MemberStoreLock)
                {
                    var result = 0;
                    if (m_MemberStoreTail != null)
                        result++;

                    if (m_MemberStore != null)
                        result += m_MemberStore.Count;

                    return result;
                }
            }
        }

        public IRedisMonitorChannel MonitorChannel
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
                            channel = new RedisMonitorChannel(Settings, (obj) =>
                            {
                                var onComplete = MonitorCompleted;
                                if (onComplete != null)
                                    onComplete(this, EventArgs.Empty);
                            });
                            Interlocked.Exchange(ref m_MonitorChannel, channel);
                        }
                    }
                }
                return channel;
            }
        }

        public IRedisPubSubChannel PubSubChannel
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
                            channel = new RedisPubSubChannel(this, Settings, (obj) =>
                            {
                                var onComplete = PubSubCompleted;
                                if (onComplete != null)
                                    onComplete(this, EventArgs.Empty);
                            },
                            OnPubSubPulseStateChanged);
                            Interlocked.Exchange(ref m_PubSubChannel, channel);
                        }
                    }
                }
                return channel;
            }
        }

        long IRedisHeartBeatProbe.PulseFailCount
        {
            get { return Interlocked.Read(ref m_PulseFailCount); }
        }

        bool IRedisHeartBeatProbe.Pulsing
        {
            get { return Interlocked.Read(ref m_PulseState) != RedisConstants.Zero; }
        }

        public bool ProcessingQ
        {
            get
            {
                var processor = m_Processor;
                return processor != null && processor.Processing;
            }
        }

        public new RedisPoolSettings Settings
        {
            get { return base.Settings as RedisPoolSettings; }
        }

        #endregion Properties

        #region Methods

        public override void ValidateNotDisposed()
        {
            if (Disposed)
            {
                if (!String.IsNullOrEmpty(Name))
                    throw new RedisFatalException(new ObjectDisposedException(Name), RedisErrorCode.ObjectDisposed);
                base.ValidateNotDisposed();
            }
        }

        protected virtual void OnPubSubPulseStateChanged(object sender, RedisCardioPulseStatus status)
        {
            var onPulseStateChange = PubSubPulseStateChanged;
            if (onPulseStateChange != null)
                onPulseStateChange(sender, status);
        }

        protected override void ApplyRole(RedisRole role)
        {
        }

        protected override int GetMaxConnectionCount()
        {
            var settings = Settings as RedisPoolSettings;
            if (settings != null)
                return settings.MaxConnectionCount;
            return RedisConstants.DefaultMaxConnectionCount;
        }

        public IRedisTransaction BeginTransaction(int dbIndex = 0)
        {
            ValidateNotDisposed();
            return new RedisTransaction(this, dbIndex);
        }

        public IRedisPipeline CreatePipeline(int dbIndex = 0)
        {
            ValidateNotDisposed();
            return new RedisPipeline(this, dbIndex);
        }

        public IRedisAdmin GetAdmin()
        {
            ValidateNotDisposed();
            return new RedisAdmin(this);
        }

        public IRedisDb GetDb(int dbIndex = 0)
        {
            ValidateNotDisposed();
            return new RedisDb(this, dbIndex);
        }

        #region Pulse

        public void AttachToCardio()
        {
            if (!Disposed && !m_ProbeAttached)
            {
                var settings = Settings;
                if (settings != null && settings.HeartBeatEnabled)
                {
                    m_ProbeAttached = true;
                    RedisCardio.Default.Attach(this, settings.HearBeatIntervalInSecs);
                }
            }
        }

        public void DetachFromCardio()
        {
            if (m_ProbeAttached && !Disposed)
                RedisCardio.Default.Detach(this);
        }

        bool IRedisHeartBeatProbe.Pulse()
        {
            if (Interlocked.CompareExchange(ref m_PulseState, RedisConstants.One, RedisConstants.Zero) ==
                RedisConstants.Zero)
            {
                try
                {
                    if (!Disposed)
                    {
                        var result = DoPulse();
                        if (result)
                            Interlocked.Add(ref m_PulseFailCount, RedisConstants.Zero);
                        return result;
                    }

                    return false;
                }
                catch (Exception)
                {
                    if (Interlocked.Read(ref m_PulseFailCount) < long.MaxValue)
                        Interlocked.Add(ref m_PulseFailCount, RedisConstants.One);
                }
                finally
                {
                    Interlocked.Exchange(ref m_PulseState, RedisConstants.Zero);
                }
            }
            return false;
        }

        protected virtual bool DoPulse()
        {
            var result = false;
            try
            {
                result = Ping(IsDown);
            }
            catch (Exception)
            { }

            try
            {
                DoPulseMemberStore();
            }
            catch (Exception)
            { }
            return result;
        }

        protected virtual void DoPulseMemberStore()
        {
            if (!Disposed)
            {
                lock (m_MemberStoreLock)
                {
                    if (m_MemberStore != null)
                    {
                        var members = m_MemberStore.ToArray();
                        if (members.Length > 0)
                        {
                            Action<RedisConnectionPoolMember> pulseAction =
                                (member) =>
                                {
                                    if (!Disposed && member.IsAlive() && !member.Pulsing)
                                    {
                                        var result = false;
                                        try
                                        {
                                            result = member.Pulse();
                                        }
                                        catch (Exception)
                                        { }
                                        finally
                                        {
                                            if (!result)
                                            {
                                                var memberList = m_MemberStore;
                                                if (memberList != null)
                                                {
                                                    memberList.Remove(member);
                                                    member.Dispose();
                                                }
                                            }
                                        }
                                    }
                                };

                            if (members.Length == 1)
                                pulseAction(members[0]);
                            else
                                Parallel.ForEach(members, (member) => { pulseAction(member); });
                        }
                    }
                }
            }
        }

        void IRedisHeartBeatProbe.ResetPulseFailCounter()
        {
            Interlocked.Add(ref m_PulseFailCount, RedisConstants.Zero);
        }

        void IRedisHeartBeatProbe.PulseStateChanged(RedisCardioPulseStatus status)
        {
            OnPoolPulseStateChange(status);
        }

        protected virtual void OnPoolPulseStateChange(RedisCardioPulseStatus status)
        {
            var onPulseStateChange = PoolPulseStateChanged;
            if (onPulseStateChange != null)
            {
                Action failAction = () =>
                {
                    onPulseStateChange(this, status);
                };
                failAction.InvokeAsync();
            }
        }

        protected internal override bool Ping(bool forceNewConnection = false)
        {
            if (!Disposed)
            {
                try
                {
                    if (forceNewConnection || IsDown)
                        return base.Ping(true);

                    using (var db = GetDb(-1))
                        db.Connection.Ping();

                    return true;
                }
                catch (Exception)
                { }
            }
            return false;
        }

        #endregion Pulse

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

        protected override IRedisConnection NewConnection(RedisConnectionSettings settings)
        {
            return new RedisDbConnection(Name, RedisRole.Any, settings,
                         null,
                         (connection, socket) =>
                         {
                             socket.DisposeSocket();
                         },
                         RedisConstants.MinDbIndex, null, true);
        }

        protected override IRedisConnection NewConnection(RedisSocket socket, int dbIndex, RedisRole expectedRole, bool connectImmediately = true)
        {
            var settings = (Settings as RedisPoolSettings) ?? RedisPoolSettings.Default;
            return new RedisDbConnection(Name, expectedRole, settings, null, OnReleaseSocket, dbIndex,
                                           socket.IsConnected() ? socket : null, connectImmediately);
        }

        protected override void CompleteSocketRelease(IRedisConnection connection, RedisSocket socket)
        {
            EnqueueSocket(socket);
        }

        protected override void OnConnectionRetry(RedisConnectionRetryEventArgs e)
        {
            var settings = (Settings as RedisPoolSettings) ?? RedisPoolSettings.Default;
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
            if (socket.IsAlive())
            {
                var member = new RedisConnectionPoolMember(socket, socket.DbIndex, Settings);
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

        protected override RedisSocket DequeueSocket(int dbIndex, RedisRole expectedRole)
        {
            var storeHasMember = false;
            lock (m_MemberStoreLock)
            {
                var member = m_MemberStoreTail;
                if (member != null)
                {
                    try
                    {
                        if (member.DbIndex == dbIndex)
                        {
                            storeHasMember = true;
                            if (expectedRole == RedisRole.Any ||
                                expectedRole == RedisRole.Undefined ||
                                member.Role == expectedRole)
                            {
                                var socket = member.ReleaseSocket();

                                m_MemberStoreTail = null;
                                if (socket.IsConnected())
                                    return socket;

                                socket.DisposeSocket();
                            }
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
                                if (member.DbIndex == dbIndex)
                                {
                                    storeHasMember = true;
                                    if (expectedRole == RedisRole.Any ||
                                        expectedRole == RedisRole.Undefined ||
                                        member.Role == expectedRole)
                                    {
                                        socket = member.ReleaseSocket();

                                        store.Remove(node);
                                        if (socket.IsConnected())
                                            return socket;

                                        socket.DisposeSocket();
                                    }
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

            if (storeHasMember && expectedRole == RedisRole.Slave)
                return DequeueSocket(dbIndex, RedisRole.Master);
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

                if (!members.IsEmpty())
                    members.AsParallel().ForAll(m => m.Dispose());
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
                var idleTimeout = (Settings as RedisPoolSettings ?? RedisPoolSettings.Default).ConnectionIdleTimeout;

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

                                var removeMember = (member == null) || !(member.Socket.IsConnected(100) && member.Pulse()) ||
                                    ((idleTimeout > 0) && (now - member.PooledTime).TotalSeconds >= idleTimeout);

                                if (removeMember)
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

        #region IRedisCommandExecuter Methods

        protected internal override RedisResponse Execute(RedisCommand command, bool throwException = true)
        {
            if (command == null)
                throw new ArgumentNullException("command");

            ValidateNotDisposed();

            IRedisConnection connection = null;
            if ((Settings as RedisPoolSettings ?? RedisPoolSettings.Default).UseAsyncCompleter)
            {
                connection = Connect(command.DbIndex, command.Role);
                if (connection == null)
                {
                    var asyncRequest = m_AsycRequestQ.Enqueue<RedisResponse>(command, RedisCommandExpect.Response, null);
                    StartToProcessQ();

                    return asyncRequest.Task.Result;
                }
            }

            using (connection = (connection ?? Connect(command.DbIndex, command.Role)))
            {
                return command.Execute(connection, throwException);
            }
        }

        protected internal override RedisRaw ExpectArray(RedisCommand command, bool throwException = true)
        {
            if (command == null)
                throw new ArgumentNullException("command");

            ValidateNotDisposed();

            IRedisConnection connection = null;
            if ((Settings as RedisPoolSettings ?? RedisPoolSettings.Default).UseAsyncCompleter)
            {
                connection = Connect(command.DbIndex, command.Role);
                if (connection == null)
                {
                    var asyncRequest = m_AsycRequestQ.Enqueue<RedisRaw>(command, RedisCommandExpect.Array, null);
                    StartToProcessQ();

                    return asyncRequest.Task.Result;
                }
            }

            using (connection = (connection ?? Connect(command.DbIndex, command.Role)))
            {
                return command.ExpectArray(connection, throwException);
            }
        }

        protected internal override RedisString ExpectBulkString(RedisCommand command, bool throwException = true)
        {
            if (command == null)
                throw new ArgumentNullException("command");

            ValidateNotDisposed();

            IRedisConnection connection = null;
            if ((Settings as RedisPoolSettings ?? RedisPoolSettings.Default).UseAsyncCompleter)
            {
                connection = Connect(command.DbIndex, command.Role);
                if (connection == null)
                {
                    var asyncRequest = m_AsycRequestQ.Enqueue<RedisString>(command, RedisCommandExpect.BulkString, null);
                    StartToProcessQ();

                    return asyncRequest.Task.Result;
                }
            }

            using (connection = (connection ?? Connect(command.DbIndex, command.Role)))
            {
                return command.ExpectBulkString(connection, throwException);
            }
        }

        protected internal override RedisBytes ExpectBulkStringBytes(RedisCommand command, bool throwException = true)
        {
            if (command == null)
                throw new ArgumentNullException("command");

            ValidateNotDisposed();

            IRedisConnection connection = null;
            if ((Settings as RedisPoolSettings ?? RedisPoolSettings.Default).UseAsyncCompleter)
            {
                connection = Connect(command.DbIndex, command.Role);
                if (connection == null)
                {
                    var asyncRequest = m_AsycRequestQ.Enqueue<RedisBytes>(command, RedisCommandExpect.BulkStringBytes, null);
                    StartToProcessQ();

                    return asyncRequest.Task.Result;
                }
            }

            using (connection = (connection ?? Connect(command.DbIndex, command.Role)))
            {
                return command.ExpectBulkStringBytes(connection, throwException);
            }
        }

        protected internal override RedisDouble ExpectDouble(RedisCommand command, bool throwException = true)
        {
            if (command == null)
                throw new ArgumentNullException("command");

            ValidateNotDisposed();

            IRedisConnection connection = null;
            if ((Settings as RedisPoolSettings ?? RedisPoolSettings.Default).UseAsyncCompleter)
            {
                connection = Connect(command.DbIndex, command.Role);
                if (connection == null)
                {
                    var asyncRequest = m_AsycRequestQ.Enqueue<RedisDouble>(command, RedisCommandExpect.Double, null);
                    StartToProcessQ();

                    return asyncRequest.Task.Result;
                }
            }

            using (connection = (connection ?? Connect(command.DbIndex, command.Role)))
            {
                return command.ExpectDouble(connection, throwException);
            }
        }

        protected internal override RedisBool ExpectGreaterThanZero(RedisCommand command, bool throwException = true)
        {
            if (command == null)
                throw new ArgumentNullException("command");

            ValidateNotDisposed();

            IRedisConnection connection = null;
            if ((Settings as RedisPoolSettings ?? RedisPoolSettings.Default).UseAsyncCompleter)
            {
                connection = Connect(command.DbIndex, command.Role);
                if (connection == null)
                {
                    var asyncRequest = m_AsycRequestQ.Enqueue<RedisBool>(command, RedisCommandExpect.GreaterThanZero, null);
                    StartToProcessQ();

                    return asyncRequest.Task.Result;
                }
            }

            using (connection = (connection ?? Connect(command.DbIndex, command.Role)))
            {
                return command.ExpectInteger(connection, throwException) > RedisConstants.Zero;
            }
        }

        protected internal override RedisInteger ExpectInteger(RedisCommand command, bool throwException = true)
        {
            if (command == null)
                throw new ArgumentNullException("command");

            ValidateNotDisposed();

            IRedisConnection connection = null;
            if ((Settings as RedisPoolSettings ?? RedisPoolSettings.Default).UseAsyncCompleter)
            {
                connection = Connect(command.DbIndex, command.Role);
                if (connection == null)
                {
                    var asyncRequest = m_AsycRequestQ.Enqueue<RedisInteger>(command, RedisCommandExpect.Integer, null);
                    StartToProcessQ();

                    return asyncRequest.Task.Result;
                }
            }

            using (connection = (connection ?? Connect(command.DbIndex, command.Role)))
            {
                return command.ExpectInteger(connection, throwException);
            }
        }

        protected internal override RedisMultiBytes ExpectMultiDataBytes(RedisCommand command, bool throwException = true)
        {
            if (command == null)
                throw new ArgumentNullException("command");

            ValidateNotDisposed();

            IRedisConnection connection = null;
            if ((Settings as RedisPoolSettings ?? RedisPoolSettings.Default).UseAsyncCompleter)
            {
                connection = Connect(command.DbIndex, command.Role);
                if (connection == null)
                {
                    var asyncRequest = m_AsycRequestQ.Enqueue<RedisMultiBytes>(command, RedisCommandExpect.MultiDataBytes, null);
                    StartToProcessQ();

                    return asyncRequest.Task.Result;
                }
            }

            using (connection = (connection ?? Connect(command.DbIndex, command.Role)))
            {
                return command.ExpectMultiDataBytes(connection, throwException);
            }
        }

        protected internal override RedisMultiString ExpectMultiDataStrings(RedisCommand command, bool throwException = true)
        {
            if (command == null)
                throw new ArgumentNullException("command");

            ValidateNotDisposed();

            IRedisConnection connection = null;
            if ((Settings as RedisPoolSettings ?? RedisPoolSettings.Default).UseAsyncCompleter)
            {
                connection = Connect(command.DbIndex, command.Role);
                if (connection == null)
                {
                    var asyncRequest = m_AsycRequestQ.Enqueue<RedisMultiString>(command, RedisCommandExpect.MultiDataStrings, null);
                    StartToProcessQ();

                    return asyncRequest.Task.Result;
                }
            }

            using (connection = (connection ?? Connect(command.DbIndex, command.Role)))
            {
                return command.ExpectMultiDataStrings(connection, throwException);
            }
        }

        protected internal override RedisVoid ExpectNothing(RedisCommand command, bool throwException = true)
        {
            if (command == null)
                throw new ArgumentNullException("command");

            ValidateNotDisposed();

            IRedisConnection connection = null;
            if ((Settings as RedisPoolSettings ?? RedisPoolSettings.Default).UseAsyncCompleter)
            {
                connection = Connect(command.DbIndex, command.Role);
                if (connection == null)
                {
                    var asyncRequest = m_AsycRequestQ.Enqueue<RedisVoid>(command, RedisCommandExpect.NullableDouble, null);
                    StartToProcessQ();

                    return asyncRequest.Task.Result;
                }
            }

            using (connection = (connection ?? Connect(command.DbIndex, command.Role)))
            {
                return command.ExpectNothing(connection, throwException);
            }
        }

        protected internal override RedisNullableDouble ExpectNullableDouble(RedisCommand command, bool throwException = true)
        {
            if (command == null)
                throw new ArgumentNullException("command");

            ValidateNotDisposed();

            IRedisConnection connection = null;
            if ((Settings as RedisPoolSettings ?? RedisPoolSettings.Default).UseAsyncCompleter)
            {
                connection = Connect(command.DbIndex, command.Role);
                if (connection == null)
                {
                    var asyncRequest = m_AsycRequestQ.Enqueue<RedisNullableDouble>(command, RedisCommandExpect.NullableDouble, null);
                    StartToProcessQ();

                    return asyncRequest.Task.Result;
                }
            }

            using (connection = (connection ?? Connect(command.DbIndex, command.Role)))
            {
                return command.ExpectNullableDouble(connection, throwException);
            }
        }

        protected internal override RedisNullableInteger ExpectNullableInteger(RedisCommand command, bool throwException = true)
        {
            if (command == null)
                throw new ArgumentNullException("command");

            ValidateNotDisposed();

            IRedisConnection connection = null;
            if ((Settings as RedisPoolSettings ?? RedisPoolSettings.Default).UseAsyncCompleter)
            {
                connection = Connect(command.DbIndex, command.Role);
                if (connection == null)
                {
                    var asyncRequest = m_AsycRequestQ.Enqueue<RedisNullableInteger>(command, RedisCommandExpect.NullableInteger, null);
                    StartToProcessQ();

                    return asyncRequest.Task.Result;
                }
            }

            using (connection = (connection ?? Connect(command.DbIndex, command.Role)))
            {
                return command.ExpectNullableInteger(connection, throwException);
            }
        }

        protected internal override RedisBool ExpectOK(RedisCommand command, bool throwException = true)
        {
            if (command == null)
                throw new ArgumentNullException("command");

            ValidateNotDisposed();

            IRedisConnection connection = null;
            if ((Settings as RedisPoolSettings ?? RedisPoolSettings.Default).UseAsyncCompleter)
            {
                connection = Connect(command.DbIndex, command.Role);
                if (connection == null)
                {
                    var asyncRequest = m_AsycRequestQ.Enqueue<RedisBool>(command, RedisCommandExpect.OK, RedisConstants.OK);
                    StartToProcessQ();

                    return asyncRequest.Task.Result;
                }
            }

            using (connection = (connection ?? Connect(command.DbIndex, command.Role)))
            {
                return command.ExpectSimpleString(connection, RedisConstants.OK, throwException);
            }
        }

        protected internal override RedisBool ExpectOne(RedisCommand command, bool throwException = true)
        {
            if (command == null)
                throw new ArgumentNullException("command");

            ValidateNotDisposed();

            IRedisConnection connection = null;
            if ((Settings as RedisPoolSettings ?? RedisPoolSettings.Default).UseAsyncCompleter)
            {
                connection = Connect(command.DbIndex, command.Role);
                if (connection == null)
                {
                    var asyncRequest = m_AsycRequestQ.Enqueue<RedisBool>(command, RedisCommandExpect.One, null);
                    StartToProcessQ();

                    return asyncRequest.Task.Result;
                }
            }

            using (connection = (connection ?? Connect(command.DbIndex, command.Role)))
            {
                return command.ExpectOne(connection, throwException);
            }
        }

        protected internal override RedisBool ExpectSimpleString(RedisCommand command, string expectedResult, bool throwException = true)
        {
            if (command == null)
                throw new ArgumentNullException("command");

            ValidateNotDisposed();

            IRedisConnection connection = null;
            if ((Settings as RedisPoolSettings ?? RedisPoolSettings.Default).UseAsyncCompleter)
            {
                connection = Connect(command.DbIndex, command.Role);
                if (connection == null)
                {
                    var asyncRequest = m_AsycRequestQ.Enqueue<RedisBool>(command, RedisCommandExpect.OK, expectedResult);
                    StartToProcessQ();

                    return asyncRequest.Task.Result;
                }
            }

            using (connection = (connection ?? Connect(command.DbIndex, command.Role)))
            {
                return command.ExpectSimpleString(connection, expectedResult, throwException);
            }
        }

        protected internal override RedisString ExpectSimpleString(RedisCommand command, bool throwException = true)
        {
            if (command == null)
                throw new ArgumentNullException("command");

            ValidateNotDisposed();

            var connection = Connect(command.DbIndex, command.Role);
            if (connection == null)
            {
                var asyncRequest = m_AsycRequestQ.Enqueue<RedisString>(command, RedisCommandExpect.SimpleString, null);
                StartToProcessQ();

                return asyncRequest.Task.Result;
            }

            using (connection = (connection ?? Connect(command.DbIndex, command.Role)))
            {
                return command.ExpectSimpleString(connection, throwException);
            }
        }

        protected internal override RedisBool ExpectSimpleStringBytes(RedisCommand command, byte[] expectedResult, bool throwException = true)
        {
            if (command == null)
                throw new ArgumentNullException("command");

            ValidateNotDisposed();

            IRedisConnection connection = null;
            if ((Settings as RedisPoolSettings ?? RedisPoolSettings.Default).UseAsyncCompleter)
            {
                connection = Connect(command.DbIndex, command.Role);
                if (connection == null)
                {
                    var asyncRequest = m_AsycRequestQ.Enqueue<RedisBytes>(command, RedisCommandExpect.SimpleStringBytes, null);
                    StartToProcessQ();

                    return (asyncRequest.Task.Result == expectedResult);
                }
            }

            using (connection = (connection ?? Connect(command.DbIndex, command.Role)))
            {
                return command.ExpectSimpleStringBytes(connection, expectedResult, throwException);
            }
        }

        protected internal override RedisBytes ExpectSimpleStringBytes(RedisCommand command, bool throwException = true)
        {
            if (command == null)
                throw new ArgumentNullException("command");

            ValidateNotDisposed();

            IRedisConnection connection = null;
            if ((Settings as RedisPoolSettings ?? RedisPoolSettings.Default).UseAsyncCompleter)
            {
                connection = Connect(command.DbIndex, command.Role);
                if (connection == null)
                {
                    var asyncRequest = m_AsycRequestQ.Enqueue<RedisBytes>(command, RedisCommandExpect.SimpleStringBytes, null);
                    StartToProcessQ();

                    return asyncRequest.Task.Result;
                }
            }

            using (connection = (connection ?? Connect(command.DbIndex, command.Role)))
            {
                return command.ExpectSimpleStringBytes(connection, throwException);
            }
        }

        #endregion IRedisCommandExecuter Methods

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
                        if (!pools.IsEmpty())
                            pools.AsParallel().ForAll(p => p.PurgeIdles());
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

        #endregion Methods
    }
}
