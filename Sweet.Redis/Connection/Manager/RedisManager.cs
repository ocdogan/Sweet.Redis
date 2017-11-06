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
using System.Text;
using System.Threading;

namespace Sweet.Redis
{
    public class RedisManager : RedisDisposable, IRedisManager, IRedisNamedObject, IRedisIdentifiedObject
    {
        #region InitializationState

        private enum InitializationState : long
        {
            Undefined = 0,
            Initializing = 1,
            Initialized = 2
        }

        #endregion InitializationState

        #region Field Members

        private long m_Id = RedisIDGenerator<RedisManager>.NextId();
        private string m_Name;
        private string m_MasterName;

        private RedisManagerType m_ManagerType;
        private RedisManagerSettings m_Settings;
        private RedisManagedEndPointResolver m_EndPointResolver;

        private RedisManagedMSGroup m_MSGroup;
        private RedisManagedSentinelGroup m_Sentinels;

        private long m_RefreshState;
        private long m_InitializationState;
        private readonly object m_SyncRoot = new object();

        private long m_SentinelHelloChannelState;
        private RedisPubSubChannel m_SentinelHelloChannel;

        private RedisEventQueue m_EventQ;

        #endregion Field Members

        #region .Ctors

        public RedisManager(string name, RedisManagerSettings settings)
        {
            if (settings == null)
                throw new RedisFatalException(new ArgumentNullException("settings"), RedisErrorCode.MissingParameter);

            m_Settings = settings;
            m_Name = !name.IsEmpty() ? name : (GetType().Name + ", " + m_Id.ToString());
            m_MasterName = (settings.MasterName ?? String.Empty).Trim();
            m_ManagerType = settings.ManagerType;

            m_MSGroup = new RedisManagedMSGroup(settings, null, null, OnProbeStateChange);
            m_Sentinels = new RedisManagedSentinelGroup(settings, m_MasterName, null, OnProbeStateChange);

            m_EventQ = new RedisEventQueue();
            m_EndPointResolver = new RedisManagedEndPointResolver(m_Name, settings);
        }

        #endregion .Ctors

        #region Destructors

        protected override void OnDispose(bool disposing)
        {
            base.OnDispose(disposing);

            var helloChannel = Interlocked.Exchange(ref m_SentinelHelloChannel, null);
            if (helloChannel != null)
                helloChannel.Dispose();

            var eventQ = Interlocked.Exchange(ref m_EventQ, null);
            if (eventQ != null)
                eventQ.Dispose();

            var endPointResolver = Interlocked.Exchange(ref m_EndPointResolver, null);
            if (endPointResolver != null)
                endPointResolver.Dispose();

            Interlocked.Exchange(ref m_InitializationState, (long)InitializationState.Undefined);

            var msGroup = Interlocked.Exchange(ref m_MSGroup, null);
            if (msGroup != null)
                msGroup.Dispose();
        }

        #endregion Destructors

        #region Properties

        public long Id
        {
            get { return m_Id; }
        }

        public bool Initialized
        {
            get
            {
                return Interlocked.Read(ref m_InitializationState) ==
                  (long)InitializationState.Initialized;
            }
        }

        public bool Initializing
        {
            get
            {
                return Interlocked.Read(ref m_InitializationState) ==
                  (long)InitializationState.Initializing;
            }
        }

        public RedisManagerType ManagerType
        {
            get { return m_ManagerType; }
        }

        public string Name
        {
            get { return m_Name; }
        }

        public bool Refreshing
        {
            get { return Interlocked.Read(ref m_RefreshState) != RedisConstants.Zero; }
        }

        public RedisManagerSettings Settings
        {
            get { return m_Settings; }
        }

        #endregion Properties

        #region Methods

        #region Public Methods

        public IRedisTransaction BeginTransaction(bool readOnly = false, int dbIndex = 0)
        {
            ValidateNotDisposed();
            return NextPool(readOnly).BeginTransaction(dbIndex);
        }

        public IRedisTransaction BeginTransaction(Func<RedisNodeInfo, bool> nodeSelector, int dbIndex = 0)
        {
            ValidateNotDisposed();
            var pool = SelectMasterOrSlavePool(nodeSelector);
            if (pool.IsAlive())
                return pool.BeginTransaction(dbIndex);
            return null;
        }

        public IRedisPipeline CreatePipeline(bool readOnly = false, int dbIndex = 0)
        {
            ValidateNotDisposed();
            return NextPool(readOnly).CreatePipeline(dbIndex);
        }

        public IRedisPipeline CreatePipeline(Func<RedisNodeInfo, bool> nodeSelector, int dbIndex = 0)
        {
            ValidateNotDisposed();
            var pool = SelectMasterOrSlavePool(nodeSelector);
            if (pool.IsAlive())
                return pool.CreatePipeline(dbIndex);
            return null;
        }

        public IRedisAdmin GetAdmin(Func<RedisNodeInfo, bool> nodeSelector)
        {
            ValidateNotDisposed();
            var pool = SelectMasterOrSlavePool(nodeSelector);
            if (pool.IsAlive())
                return pool.GetAdmin();
            return null;
        }

        public IRedisDb GetDb(bool readOnly = false, int dbIndex = 0)
        {
            ValidateNotDisposed();
            return NextPool(readOnly).GetDb(dbIndex);
        }

        public IRedisDb GetDb(Func<RedisNodeInfo, bool> nodeSelector, int dbIndex = 0)
        {
            ValidateNotDisposed();
            var pool = SelectMasterOrSlavePool(nodeSelector);
            if (pool.IsAlive())
                return pool.GetDb(dbIndex);
            return null;
        }

        public IRedisMonitorChannel GetMonitorChannel(Func<RedisNodeInfo, bool> nodeSelector)
        {
            ValidateNotDisposed();
            var pool = SelectMasterOrSlavePool(nodeSelector);
            if (pool.IsAlive())
                return pool.MonitorChannel;
            return null;
        }

        public IRedisPubSubChannel GetPubSubChannel(Func<RedisNodeInfo, bool> nodeSelector)
        {
            ValidateNotDisposed();
            var pool = SelectMasterOrSlavePool(nodeSelector);
            if (pool.IsAlive())
                return pool.PubSubChannel;
            return null;
        }

        public void Refresh()
        {
            lock (m_SyncRoot)
            {
                RefreshAllNodes(true);
            }
        }

        #endregion Public Methods

        #region Initialization

        private void InitializeNodes()
        {
            if (Interlocked.CompareExchange(ref m_InitializationState, (long)InitializationState.Initializing, (long)InitializationState.Undefined) !=
                (long)InitializationState.Initialized)
            {
                if (!Initialized)
                {
                    try
                    {
                        RefreshAllNodes(false);

                        if (m_ManagerType != RedisManagerType.Sentinel &&
                            m_Sentinels.IsAlive() && !m_Sentinels.Nodes.IsEmpty())
                            m_ManagerType = RedisManagerType.Sentinel;

                        Interlocked.Exchange(ref m_InitializationState, (long)InitializationState.Initialized);
                    }
                    catch (Exception)
                    {
                        Interlocked.Exchange(ref m_InitializationState, (long)InitializationState.Undefined);
                        throw;
                    }
                }
            }
        }

        #endregion Initialization

        #region Attached Events

        private void OnProbeStateChange(object sender, RedisCardioPulseStatus status)
        {
            if (!Disposed)
            {
                PushActionToEventQ(() =>
                {
                    var node = status.Probe as RedisManagedNode;
                    if (node.IsAlive())
                    {
                        var endPoint = node.EndPoint;
                        if (!endPoint.IsEmpty())
                        {
                            var role = node.Role;
                            var messageType = (status.NewStatus == RedisCardioProbeStatus.OK) ?
                                                    RedisSentinelMessageType.SubjectivelyUp :
                                                    RedisSentinelMessageType.SubjectivelyDown;

                            var message = new RedisSentinelMessage(messageType, m_MasterName,
                                                                   new RedisNodeInfo(endPoint, role),
                                                                   RedisNodeInfo.Empty);

                            ProcessSentinelMessage(message);
                        }
                    }
                });
            }
        }

        private void OnSentinelConnectionDrop(object sender)
        {
            if (!Disposed && !ReferenceEquals(sender, null) && !m_MasterName.IsEmpty())
            {
                var node = sender as RedisManagedNode;
                if (node.IsAlive())
                {
                    PushActionToEventQ(() =>
                    {
                        if (node.IsAlive())
                            TestNode(node);
                        RefreshSentinels();
                    });
                }
            }
        }

        /* 
        127.0.0.1,26380,07fabf3cbac43bcc955588b1023f95498b58f8f2,16,mymaster,127.0.0.1,6381,16
        */
        private void OnSentinelHelloMessage(RedisPubSubMessage msg)
        {
            if (!msg.IsEmpty &&
                msg.Channel == RedisConstants.SentinelHelloChannel &&
                !m_MasterName.IsEmpty())
            {
                PushActionToEventQ(() =>
                    {
                        var sentinels = m_Sentinels;
                        if (!sentinels.IsAlive())
                            return;

                        if (msg.Data.IsEmpty())
                            return;

                        var msgText = Encoding.UTF8.GetString(msg.Data);
                        if (msgText.IsEmpty())
                            return;

                        var helloMsg = RedisSentinelHelloMessage.Parse(msgText);

                        if (helloMsg.MasterName == m_MasterName &&
                            helloMsg.SentinelPort.HasValue &&
                            !helloMsg.SentinelIPAddress.IsEmpty())
                        {
                            var endPoints = sentinels.GetEndPoints();
                            var sentinelEP = new RedisEndPoint(helloMsg.SentinelIPAddress, helloMsg.SentinelPort.Value);

                            if (endPoints.IsEmpty() ||
                                endPoints.Any(ep => !ep.IsEmpty() && ep == sentinelEP))
                            {
                                var instanceName = helloMsg.SentinelIPAddress + ":" + helloMsg.SentinelPort.Value;

                                var sentinelMsg = new RedisSentinelMessage(RedisSentinelMessageType.ObjectivelyUp,
                                        helloMsg.MasterName,
                                        new RedisNodeInfo(sentinelEP, RedisRole.Sentinel, instanceName),
                                        new RedisNodeInfo(new RedisEndPoint(helloMsg.MasterIPAddress, helloMsg.MasterPort.Value), RedisRole.Master));

                                OnSentinelMessage(sentinelMsg);
                            }
                        }
                    });
            }
        }

        /*
        Master down:
        ------------
        +sdown master mymaster 127.0.0.1 6379
        +switch-master mymaster 127.0.0.1 6379 127.0.0.1 6380
        +sdown slave 127.0.0.1:6380 127.0.0.1 6380 @ mymaster 127.0.0.1 6379

        Slave up:
        ------------
        -sdown slave 127.0.0.1:6380 127.0.0.1 6380 @ mymaster 127.0.0.1 6379

        Slave down:
        ------------
        +sdown slave 127.0.0.1:6380 127.0.0.1 6380 @ mymaster 127.0.0.1 6379

        -odown master mymaster 127.0.0.1 6379
        +odown master mymaster 127.0.0.1 6379 #quorum 2/2

        +switch-master mymaster 127.0.0.1 6380 127.0.0.1 6379

        New Sentinel:
        -------------
        +sdown sentinel 127.0.0.1:26381 127.0.0.1 26381 @ mymaster 127.0.0.1 6379
        -dup-sentinel master mymaster 127.0.0.1 6381 #duplicate of 127.0.0.1:26381 or cab1c287ec59309126ad1a63b354ba132bb4e55b
        +sentinel sentinel 127.0.0.1:26381 127.0.0.1 26379 @ mymaster 127.0.0.1 6379
         */
        private void OnSentinelMessage(RedisSentinelMessage message)
        {
            if (message != null && !m_MasterName.IsEmpty() &&
                message.MasterName == m_MasterName)
            {
                PushActionToEventQ(() =>
                {
                    ProcessSentinelMessage(message);
                });
            }
        }

        private void PushActionToEventQ(Action action)
        {
            if (!Disposed && (action != null))
            {
                var eventQ = m_EventQ;
                if (eventQ.IsAlive())
                {
                    eventQ.Enqueu((state) =>
                    {
                        if (!Disposed)
                        {
                            try
                            {
                                lock (m_SyncRoot)
                                {
                                    if (!Disposed)
                                        action();
                                }
                            }
                            catch (Exception)
                            { }
                        }
                    });
                }
            }
        }

        #endregion Attached Events

        #region Node Selection

        private RedisManagedPool SelectMasterOrSlavePool(Func<RedisNodeInfo, bool> nodeSelector)
        {
            if (nodeSelector != null)
            {
                InitializeNodes();

                var msGroup = m_MSGroup;
                if (msGroup.IsAlive())
                {
                    var pool = SelectPool(msGroup.Masters, nodeSelector);
                    if (pool == null)
                        pool = SelectPool(msGroup.Slaves, nodeSelector);

                    return pool;
                }
            }
            return null;
        }

        private RedisManagedPool SelectPool(RedisManagedNodesGroup nodesGroup, Func<RedisNodeInfo, bool> nodeSelector)
        {
            if (nodesGroup.IsAlive())
            {
                RedisManagedNode[] nodesCopy = null;
                lock (m_SyncRoot)
                {
                    var nodes = nodesGroup.Nodes;
                    if (nodes != null)
                        nodesCopy = (RedisManagedNode[])nodes.Clone();
                }

                if (nodesCopy != null)
                {
                    foreach (var node in nodesCopy)
                    {
                        if (node.IsAlive())
                        {
                            try
                            {
                                var pool = node.Seed as RedisManagedPool;
                                if (pool.IsAlive() &&
                                    nodeSelector(node.GetNodeInfo()))
                                    return pool;
                            }
                            catch (Exception)
                            { }
                        }
                    }
                }
            }
            return null;
        }

        private RedisManagedPool NextPool(bool readOnly)
        {
            InitializeNodes();

            lock (m_SyncRoot)
            {
                var msGroup = m_MSGroup;
                if (!msGroup.IsAlive())
                    throw new RedisFatalException("Can not discover masters and slaves", RedisErrorCode.ConnectionError);

                var grp = msGroup.SelectGroup(readOnly);
                if (!grp.IsAlive())
                    throw new RedisFatalException(String.Format("No {0} group found", readOnly ? "slave" : "master"), RedisErrorCode.ConnectionError);

                var pool = grp.Next();
                if (!pool.IsAlive())
                    throw new RedisFatalException(String.Format("No {0} node found", readOnly ? "slave" : "master"), RedisErrorCode.ConnectionError);

                return pool;
            }
        }

        private static void DisposeObjects(IList<IDisposable> disposeList)
        {
            if (disposeList != null)
            {
                var count = disposeList.Count;
                if (count > 0)
                {
                    for (var i = 0; i < count; i++)
                    {
                        try
                        {
                            var obj = disposeList[i];
                            if (!ReferenceEquals(obj, null))
                                obj.Dispose();
                        }
                        catch (Exception)
                        { }
                    }
                }
            }
        }

        #endregion Node Selection

        #region Node State Management

        #region Refresh

        private void RefreshAllNodes(bool careValidNodes = true)
        {
            if (Interlocked.CompareExchange(ref m_RefreshState, RedisConstants.One, RedisConstants.Zero) ==
                RedisConstants.Zero)
            {
                try
                {
                    var endPointResolver = m_EndPointResolver;
                    if (!endPointResolver.IsAlive())
                        return;

                    var tuple = endPointResolver.CreateGroups();
                    if (tuple != null)
                    {
                        var msGroup = tuple.Item1;
                        var sentinels = tuple.Item2;

                        var objectsToDispose = new List<IDisposable>();
                        try
                        {
                            if (!careValidNodes)
                            {
                                try
                                {
                                    var oldMSGroup = Interlocked.Exchange(ref m_MSGroup, msGroup);
                                    if (oldMSGroup.IsAlive())
                                    {
                                        oldMSGroup.SetOnPulseStateChange(null);
                                        objectsToDispose.Add(oldMSGroup);

                                        var nodesGroup = oldMSGroup.Masters;
                                        if (nodesGroup.IsAlive())
                                            nodesGroup.DetachFromCardio();

                                        nodesGroup = oldMSGroup.Slaves;
                                        if (nodesGroup.IsAlive())
                                            nodesGroup.DetachFromCardio();
                                    }

                                    if (msGroup.IsAlive())
                                    {
                                        msGroup.SetOnPulseStateChange(OnProbeStateChange);

                                        var nodesGroup = msGroup.Masters;
                                        if (nodesGroup.IsAlive())
                                            nodesGroup.AttachToCardio();

                                        nodesGroup = msGroup.Slaves;
                                        if (nodesGroup.IsAlive())
                                            nodesGroup.AttachToCardio();
                                    }

                                    var oldSentinels = Interlocked.Exchange(ref m_Sentinels, sentinels);
                                    if (oldSentinels.IsAlive())
                                    {
                                        oldSentinels.SetOnPulseStateChange(null);
                                        objectsToDispose.Add(oldSentinels);

                                        oldSentinels.DetachFromCardio();
                                    }

                                    if (sentinels.IsAlive())
                                    {
                                        sentinels.SetOnPulseStateChange(OnProbeStateChange);
                                        sentinels.AttachToCardio();
                                    }
                                }
                                catch (Exception)
                                {
                                    if (msGroup != null)
                                    {
                                        var grp = msGroup;
                                        msGroup = null;

                                        grp.Dispose();
                                    }
                                    if (sentinels != null)
                                    {
                                        var grp = sentinels;
                                        sentinels = null;

                                        grp.Dispose();
                                    }
                                    throw;
                                }
                                return;
                            }

                            var currMSGroup = m_MSGroup;
                            if (msGroup == null || currMSGroup == null)
                            {
                                var oldMSGroup = Interlocked.Exchange(ref m_MSGroup, msGroup);
                                if (oldMSGroup != null)
                                {
                                    objectsToDispose.Add(oldMSGroup);

                                    var nodesGroup = oldMSGroup.Masters;
                                    if (nodesGroup.IsAlive())
                                        nodesGroup.DetachFromCardio();

                                    nodesGroup = oldMSGroup.Slaves;
                                    if (nodesGroup.IsAlive())
                                        nodesGroup.DetachFromCardio();
                                }
                            }
                            else
                            {
                                objectsToDispose.Add(msGroup);

                                // Masters
                                try
                                {
                                    RearrangeGroup(msGroup.Masters, currMSGroup.Masters,
                                                   (newMasters) =>
                                                   {
                                                       var oldMasters = m_MSGroup.ExchangeMasters((RedisManagedPoolGroup)newMasters);
                                                       if (oldMasters != null)
                                                       {
                                                           oldMasters.SetOnPulseStateChange(null);
                                                           oldMasters.DetachFromCardio();
                                                       }
                                                       if (newMasters != null)
                                                       {
                                                           newMasters.SetOnPulseStateChange(OnProbeStateChange);
                                                           newMasters.AttachToCardio();
                                                       }
                                                       return oldMasters;
                                                   },
                                                   objectsToDispose);
                                }
                                finally
                                {
                                    msGroup.ExchangeMasters(null);
                                }

                                // Slaves
                                try
                                {
                                    RearrangeGroup(msGroup.Slaves, currMSGroup.Slaves,
                                                   (newSlaves) =>
                                                   {
                                                       var oldSlaves = m_MSGroup.ExchangeSlaves((RedisManagedPoolGroup)newSlaves);
                                                       if (oldSlaves != null)
                                                       {
                                                           oldSlaves.SetOnPulseStateChange(null);
                                                           oldSlaves.DetachFromCardio();
                                                       }
                                                       if (newSlaves != null)
                                                       {
                                                           newSlaves.SetOnPulseStateChange(OnProbeStateChange);
                                                           newSlaves.AttachToCardio();
                                                       }
                                                       return oldSlaves;
                                                   },
                                                   objectsToDispose);
                                }
                                finally
                                {
                                    msGroup.ExchangeSlaves(null);
                                }
                            }

                            // Sentinels
                            RearrangeGroup(sentinels, m_Sentinels,
                                           (newSentinels) =>
                                           {
                                               var oldSentinels = Interlocked.Exchange(ref m_Sentinels, (RedisManagedSentinelGroup)newSentinels);
                                               if (oldSentinels != null)
                                               {
                                                   oldSentinels.SetOnPulseStateChange(null);
                                                   oldSentinels.DetachFromCardio();
                                               }
                                               if (newSentinels != null)
                                               {
                                                   newSentinels.SetOnPulseStateChange(OnProbeStateChange);
                                                   newSentinels.AttachToCardio();
                                               }
                                               return oldSentinels;
                                           },
                                           objectsToDispose);

                            if ((!sentinels.IsAlive() || sentinels.Nodes.IsEmpty()) &&
                                Settings.ManagerType == RedisManagerType.Sentinel)
                                RefreshSentinels();
                        }
                        finally
                        {
                            AttachToSentinels();
                            DisposeObjects(objectsToDispose);
                        }
                    }
                }
                finally
                {
                    Interlocked.Exchange(ref m_RefreshState, RedisConstants.Zero);
                }
            }
        }

        private void AttachToSentinels()
        {
            PushActionToEventQ(() =>
                {
                    var sentinels = m_Sentinels;
                    if (sentinels.IsAlive())
                    {
                        sentinels.RegisterToMessageEvents(OnSentinelMessage);
                        sentinels.Monitor(OnSentinelConnectionDrop);
                    }
                });
        }

        private RedisRole TestNode(RedisManagedNode node)
        {
            try
            {
                var nodesGroup = FindNodesGroupOf(node);
                if (nodesGroup.IsAlive())
                {
                    if (!(node.IsAlive()))
                        nodesGroup.RemoveNode(node);
                    else
                    {
                        if (!node.IsSeedAlive)
                        {
                            if (nodesGroup.RemoveNode(node))
                                node.Dispose();

                            return RedisRole.Undefined;
                        }

                        var wasDown = node.IsClosed;
                        var isDown = !node.Ping();

                        if (node.IsClosed != isDown)
                            node.IsClosed = isDown;

                        if (wasDown && !node.IsClosed)
                        {
                            var endPointResolver = m_EndPointResolver;
                            if (endPointResolver != null)
                            {
                                var tuple = endPointResolver.DiscoverNode(node.EndPoint);
                                if (tuple != null)
                                {
                                    var currRole = tuple.Item1;
                                    try
                                    {
                                        var prevRole = node.Role;
                                        if (prevRole != currRole)
                                        {
                                            node.Role = currRole;
                                            if (currRole == RedisRole.Master || currRole == RedisRole.Slave)
                                            {
                                                var msGroup = m_MSGroup;
                                                if (msGroup.IsAlive())
                                                    msGroup.ChangeGroup(node);
                                            }
                                        }
                                    }
                                    finally
                                    {
                                        var socket = tuple.Item3;
                                        if (socket.IsAlive())
                                            socket.DisposeSocket();
                                    }
                                    return currRole;
                                }
                            }
                        }
                    }
                }
            }
            catch (Exception)
            { }
            return RedisRole.Undefined;
        }

        private RedisManagedNodesGroup FindGroupOf(RedisEndPoint endPoint)
        {
            if (!endPoint.IsEmpty())
            {
                var msGroup = m_MSGroup;
                if (msGroup.IsAlive())
                {
                    // Masters
                    var masters = msGroup.Masters;
                    if (masters.IsAlive())
                    {
                        var node = masters.FindNodeOf(endPoint);
                        if (node != null)
                            return masters;
                    }

                    // Slaves
                    var slaves = msGroup.Slaves;
                    if (slaves.IsAlive())
                    {
                        var node = slaves.FindNodeOf(endPoint);
                        if (node != null)
                            return slaves;
                    }
                }

                // Sentinels
                var sentinels = m_Sentinels;
                if (sentinels.IsAlive())
                {
                    var node = sentinels.FindNodeOf(endPoint);
                    if (node != null)
                        return sentinels;
                }
            }
            return null;
        }

        private RedisManagedNode FindNodeOf(RedisEndPoint endPoint)
        {
            if (!endPoint.IsEmpty())
            {
                var msGroup = m_MSGroup;
                if (msGroup.IsAlive())
                {
                    // Masters
                    var masters = msGroup.Masters;
                    if (masters.IsAlive())
                    {
                        var node = masters.FindNodeOf(endPoint);
                        if (node != null)
                            return node;
                    }

                    // Slaves
                    var slaves = msGroup.Slaves;
                    if (slaves.IsAlive())
                    {
                        var node = slaves.FindNodeOf(endPoint);
                        if (node != null)
                            return node;
                    }
                }

                // Sentinels
                var sentinels = m_Sentinels;
                if (sentinels.IsAlive())
                {
                    var node = sentinels.FindNodeOf(endPoint);
                    if (node != null)
                        return node;
                }
            }
            return null;
        }

        private RedisManagedNodesGroup FindNodesGroupOf(RedisManagedNode node)
        {
            if (node.IsAlive())
            {
                if (node.Role == RedisRole.Sentinel)
                {
                    var nodesGroup = m_Sentinels;
                    if (nodesGroup.IsAlive())
                    {
                        var nodes = nodesGroup.Nodes;
                        if (nodes != null && nodes.Any(n => ReferenceEquals(node, n)))
                            return nodesGroup;
                    }
                    return null;
                }

                var msGroup = m_MSGroup;
                if (msGroup.IsAlive())
                {
                    var nodesGroup = msGroup.Masters;
                    if (nodesGroup.IsAlive())
                    {
                        var nodes = nodesGroup.Nodes;
                        if (nodes != null && nodes.Any(n => ReferenceEquals(node, n)))
                            return nodesGroup;
                    }

                    nodesGroup = msGroup.Slaves;
                    if (nodesGroup.IsAlive())
                    {
                        var nodes = nodesGroup.Nodes;
                        if (nodes != null && nodes.Any(n => ReferenceEquals(node, n)))
                            return nodesGroup;
                    }
                }
            }
            return null;
        }

        private void RearrangeGroup(RedisManagedNodesGroup newGroup, RedisManagedNodesGroup currGroup,
                                    Func<RedisManagedNodesGroup, RedisManagedNodesGroup> exchangeGroupFunction,
                                    IList<IDisposable> objectsToDispose)
        {
            var newNodes = (newGroup != null) ? newGroup.Nodes : null;
            var currNodes = (currGroup != null) ? currGroup.Nodes : null;

            var newLength = (newNodes != null) ? newNodes.Length : 0;

            if (newNodes == null || newLength == 0 || currNodes.IsEmpty())
            {
                var oldGroup = exchangeGroupFunction(newGroup);
                if (oldGroup != null)
                    objectsToDispose.Add(oldGroup);
            }
            else
            {
                var currNodesList = currNodes.ToDictionary(n => n.EndPoint);
                var nodesToKeep = new Dictionary<RedisEndPoint, RedisManagedNode>();

                for (var i = 0; i < newLength; i++)
                {
                    var newNode = newNodes[i];

                    RedisManagedNode currNode;
                    if (currNodesList.TryGetValue(newNode.EndPoint, out currNode))
                    {
                        nodesToKeep[currNode.EndPoint] = currNode;

                        var oldSeed = newNode.ExchangeSeed(currNode.Seed);
                        if (!ReferenceEquals(oldSeed, null))
                        {
                            var disposable = oldSeed as IDisposable;
                            if (!ReferenceEquals(disposable, null))
                                objectsToDispose.Add(disposable);
                        }
                    }
                }

                var oldGroup = exchangeGroupFunction(newGroup);

                if (oldGroup != null)
                {
                    var oldNodes = oldGroup.ExchangeNodes(null);
                    if (oldNodes != null)
                    {
                        var oldLength = oldNodes.Length;

                        for (var j = 0; j < oldLength; j++)
                        {
                            var oldNode = oldNodes[j];
                            oldNodes[j] = null;

                            if (oldNode != null)
                            {
                                var oldSeed = oldNode.ExchangeSeed(null);
                                if (!nodesToKeep.ContainsKey(oldNode.EndPoint))
                                {
                                    if (!ReferenceEquals(oldSeed, null))
                                    {
                                        var disposable = oldSeed as IDisposable;
                                        if (!ReferenceEquals(disposable, null))
                                            objectsToDispose.Add(disposable);
                                    }
                                    objectsToDispose.Add(oldNode);
                                }
                            }
                        }
                    }
                }
            }
        }

        #endregion Refresh

        private void ProcessSentinelMessage(RedisSentinelMessage message)
        {
            if (message.MessageType == RedisSentinelMessageType.MasterChanged)
            {
                var msGroup = m_MSGroup;
                if (msGroup.IsAlive())
                    msGroup.PromoteToMaster(message.PrimaryNode.EndPoint, message.SecondaryNode.EndPoint);
                return;
            }

            var primaryNode = message.PrimaryNode;
            if (!primaryNode.IsEmpty)
            {
                var primaryEndPoint = primaryNode.EndPoint;
                if (!primaryEndPoint.IsEmpty())
                {
                    if (!message.MasterName.IsEmpty() && message.MasterName == m_MasterName)
                    {
                        FixGroupOf(primaryNode.Role);
                        ApplyStateChange(primaryEndPoint, primaryNode.Role, message.MessageType);
                    }
                }
            }
        }

        private void FixGroupOf(RedisRole role)
        {
            var msGroup = m_MSGroup;
            switch (role)
            {
                case RedisRole.Master:
                case RedisRole.Slave:
                    {
                        var isMaster = role == RedisRole.Master;
                        var nodesGroup = msGroup.IsAlive() ?
                                            (isMaster ? msGroup.Masters : msGroup.Slaves) : null;

                        if (!nodesGroup.IsAlive())
                        {
                            var newGroup = new RedisManagedPoolGroup(Settings, role, null, null);
                            if (msGroup.IsAlive())
                            {
                                var oldGroup = isMaster ?
                                    msGroup.ExchangeMasters(newGroup) :
                                           msGroup.ExchangeSlaves(newGroup);

                                if (oldGroup != null)
                                {
                                    oldGroup.DetachFromCardio();
                                    oldGroup.Dispose();
                                }

                                if (newGroup.IsAlive())
                                    newGroup.AttachToCardio();
                            }
                            else
                            {
                                var siblingGroup = new RedisManagedPoolGroup(Settings, role, null, null);

                                msGroup = isMaster ?
                                    new RedisManagedMSGroup(Settings, newGroup, siblingGroup, OnProbeStateChange) :
                                    new RedisManagedMSGroup(Settings, siblingGroup, newGroup, OnProbeStateChange);

                                var oldMSGroup = Interlocked.Exchange(ref m_MSGroup, msGroup);

                                if (oldMSGroup != null)
                                {
                                    var oldMasters = oldMSGroup.Masters;
                                    if (oldMasters != null)
                                        oldMasters.DetachFromCardio();

                                    var oldSlaves = oldMSGroup.Slaves;
                                    if (oldSlaves != null)
                                        oldSlaves.DetachFromCardio();
                                }

                                var newMasters = msGroup.Masters;
                                if (newMasters.IsAlive())
                                    newMasters.AttachToCardio();

                                var newSlaves = msGroup.Slaves;
                                if (newSlaves.IsAlive())
                                    newSlaves.AttachToCardio();
                            }
                        }
                    }
                    break;
                case RedisRole.Sentinel:
                    {
                        var nodesGroup = m_Sentinels;

                        if (!nodesGroup.IsAlive())
                        {
                            nodesGroup = new RedisManagedSentinelGroup(Settings, m_MasterName, null, OnProbeStateChange);
                            var oldSentinels = Interlocked.Exchange(ref m_Sentinels, (RedisManagedSentinelGroup)nodesGroup);

                            if (oldSentinels.IsAlive())
                                oldSentinels.DetachFromCardio();

                            if (nodesGroup.IsAlive())
                                nodesGroup.AttachToCardio();
                        }
                    }
                    break;
                default:
                    break;
            }
        }

        private void ApplyStateChange(RedisEndPoint endPoint, RedisRole role, RedisSentinelMessageType messageType)
        {
            if (Disposed)
                return;

            var nodesGroup = (RedisManagedNodesGroup)null;
            switch (role)
            {
                case RedisRole.Master:
                case RedisRole.Slave:
                    {
                        var msGroup = m_MSGroup;
                        if (msGroup.IsAlive())
                        {
                            nodesGroup = role == RedisRole.Master ?
                                msGroup.Masters : msGroup.Slaves;
                        }
                    }
                    break;
                case RedisRole.Sentinel:
                    nodesGroup = m_Sentinels;
                    break;
                default:
                    break;
            }

            if (nodesGroup.IsAlive())
            {
                var node = nodesGroup.FindNodeOf(endPoint);

                // Found but disposed
                if (!ReferenceEquals(node, null) && node.Disposed)
                {
                    nodesGroup.RemoveNode(node);
                    node = null;
                }

                if (node != null)
                {
                    switch (messageType)
                    {
                        case RedisSentinelMessageType.SubjectivelyDown:
                            node.IsHalfClosed = true;
                            break;
                        case RedisSentinelMessageType.SubjectivelyUp:
                            node.IsHalfClosed = false;
                            if (role != RedisRole.Master)
                                node.IsClosed = false;
                            break;
                        case RedisSentinelMessageType.ObjectivelyDown:
                            node.IsClosed = true;
                            break;
                        case RedisSentinelMessageType.ObjectivelyUp:
                            node.IsClosed = false;
                            break;
                        case RedisSentinelMessageType.SentinelDiscovered:
                            node.IsHalfClosed = false;
                            node.IsClosed = false;
                            break;
                        default:
                            break;
                    }
                    return;
                }

                // New node discovered
                if (messageType == RedisSentinelMessageType.SubjectivelyUp ||
                    messageType == RedisSentinelMessageType.ObjectivelyUp ||
                    messageType == RedisSentinelMessageType.SentinelDiscovered)
                {
                    var endPointResolver = m_EndPointResolver;
                    if (endPointResolver.IsAlive())
                    {
                        var nodeInfo = endPointResolver.DiscoverNode(endPoint);
                        if (nodeInfo != null)
                        {
                            var discoveredRole = nodeInfo.Item1;
                            var socket = nodeInfo.Item3;

                            if (discoveredRole == RedisRole.Undefined)
                                socket.DisposeSocket();
                            else
                            {
                                var settings = (RedisManagerSettings)m_Settings.Clone(endPoint.Host, endPoint.Port);

                                // Master or Slave
                                if (discoveredRole != RedisRole.Sentinel)
                                {
                                    var newPool = new RedisManagedPool(discoveredRole, Name, settings);
                                    node = new RedisManagedPoolNode(settings, discoveredRole, newPool, null);

                                    nodesGroup.AppendNode(node);

                                    var wasDown = newPool.IsDown;
                                    var isDown = !node.Ping();

                                    if (newPool.IsDown != isDown)
                                        newPool.IsDown = isDown;
                                }
                                else // Sentinel
                                {
                                    var newListener = new RedisManagedSentinelListener(settings, null, null);
                                    newListener.ReuseSocket(socket);

                                    node = new RedisManagedSentinelNode(settings, newListener, null);
                                    nodesGroup.AppendNode(node);

                                    var wasDown = newListener.IsDown;
                                    var isDown = !node.Ping();

                                    if (newListener.IsDown != isDown)
                                        newListener.IsDown = isDown;

                                    RefreshSentinels();
                                }
                            }
                        }
                    }
                }
            }
        }

        #region Sentinels

        private void RefreshSentinels()
        {
            if (m_ManagerType == RedisManagerType.Sentinel)
            {
                PushActionToEventQ(() =>
                    {
                        var sentinels = m_Sentinels;
                        if (sentinels.IsAlive())
                        {
                            HealthCheckGroup(sentinels);

                            var nodes = sentinels.Nodes;

                            if (sentinels == null || nodes.IsEmpty() ||
                                nodes.All(n => ReferenceEquals(n, null) || n.IsClosed))
                            {
                                AttachToSentinelHelloChannel();
                                return;
                            }

                            AttachToSentinels();
                        }
                    });
            }
        }

        private void HealthCheckGroup(RedisManagedNodesGroup nodesGroup)
        {
            if (nodesGroup.IsAlive())
            {
                var nodes = nodesGroup.Nodes;
                if (nodes != null)
                {
                    var length = nodes.Length;
                    if (length > 0)
                    {
                        for (var i = 0; i < length; i++)
                            TestNode(nodes[i]);
                    }
                }
            }
        }

        private void AttachToSentinelHelloChannel()
        {
            if (!Disposed &&
                Interlocked.CompareExchange(ref m_SentinelHelloChannelState, RedisConstants.One, RedisConstants.Zero) ==
                RedisConstants.Zero)
            {
                var attached = false;
                try
                {
                    var helloChannel = m_SentinelHelloChannel;
                    if (helloChannel.IsAlive())
                        return;

                    var settings = Settings;

                    var masterName = settings.MasterName;
                    if (settings.MasterName.IsEmpty())
                        return;

                    var msGroup = m_MSGroup;
                    if (msGroup == null)
                        return;

                    var endPoints = msGroup.GetEndPoints();
                    if (endPoints.IsEmpty())
                        return;

                    foreach (var endPoint in endPoints)
                    {
                        try
                        {
                            if (!endPoint.IsEmpty())
                            {
                                var epSettings = (RedisPoolSettings)settings.Clone(endPoint.Host, endPoint.Port);

                                helloChannel = new RedisPubSubChannel(this, epSettings,
                                    (sender) =>
                                    {
                                        var senderChannel = sender as RedisPubSubChannel;

                                        Interlocked.Exchange(ref m_SentinelHelloChannelState, RedisConstants.Zero);

                                        var oldChannel = Interlocked.CompareExchange(ref m_SentinelHelloChannel, null, senderChannel);
                                        if (oldChannel.IsAlive())
                                            oldChannel.Dispose();
                                    },
                                    null);

                                try
                                {
                                    var mre = new ManualResetEvent(false);

                                    helloChannel.Subscribe((msg) =>
                                    {
                                        mre.Set();
                                        OnSentinelHelloMessage(msg);
                                    },
                                        RedisConstants.SentinelHelloChannel);

                                    mre.WaitOne(5000);

                                    attached = true;
                                    var oldChannel = Interlocked.Exchange(ref m_SentinelHelloChannel, helloChannel);
                                    if (oldChannel.IsAlive())
                                        oldChannel.Dispose();

                                    break;
                                }
                                catch (Exception)
                                {
                                    helloChannel.Dispose();
                                }
                            }
                        }
                        catch (Exception)
                        { }
                    }
                }
                finally
                {
                    if (!attached)
                        Interlocked.Exchange(ref m_SentinelHelloChannelState, RedisConstants.Zero);
                }
            }
        }

        private void DetachFromSentinelHelloChannel()
        {
            if (Interlocked.CompareExchange(ref m_SentinelHelloChannelState, RedisConstants.Zero, RedisConstants.One) ==
                RedisConstants.One)
            {
                var helloChannel = Interlocked.Exchange(ref m_SentinelHelloChannel, null);
                if (helloChannel.IsAlive())
                    helloChannel.Dispose();
            }
        }

        #endregion Sentinels

        #endregion Node State Management

        #endregion Methods
    }
}
