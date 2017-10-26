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

        private long m_Id;
        private string m_Name;
        private string m_MasterName;

        private RedisManagerSettings m_Settings;
        private RedisManagedEndPointResolver m_EndPointResolver;

        private RedisManagedMSGroup m_MSGroup;
        private RedisManagedSentinelGroup m_Sentinels;

        private long m_RefreshState;
        private long m_InitializationState;
        private readonly object m_SyncRoot = new object();

        #endregion Field Members

        #region .Ctors

        public RedisManager(string name, RedisManagerSettings settings = null)
        {
            if (settings == null)
                throw new RedisFatalException(new ArgumentNullException("settings"), RedisErrorCode.MissingParameter);

            m_Id = RedisIDGenerator<RedisManager>.NextId();
            m_Settings = settings;
            m_Name = !String.IsNullOrEmpty(name) ? name : (GetType().Name + ", " + m_Id.ToString());
            m_MasterName = (settings.MasterName ?? String.Empty).Trim();

            m_EndPointResolver = new RedisManagedEndPointResolver(m_Name, settings);
        }

        #endregion .Ctors

        #region Destructors

        protected override void OnDispose(bool disposing)
        {
            base.OnDispose(disposing);

            var endPointResolver = Interlocked.Exchange(ref m_EndPointResolver, null);
            if (endPointResolver != null)
                endPointResolver.Dispose();

            Interlocked.Exchange(ref m_InitializationState, (long)InitializationState.Undefined);

            var cluster = Interlocked.Exchange(ref m_MSGroup, null);
            if (cluster != null)
                cluster.Dispose();
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
                return Interlocked.Read(ref m_InitializationState) !=
                  (long)InitializationState.Undefined;
            }
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

        public IRedisTransaction BeginTransaction(bool readOnly = false, int dbIndex = 0)
        {
            ValidateNotDisposed();
            return NextPool(readOnly).BeginTransaction(dbIndex);
        }

        public IRedisTransaction BeginTransaction(Func<RedisManagedNodeInfo, bool> nodeSelector, int dbIndex = 0)
        {
            ValidateNotDisposed();
            var pool = SelectMasterOrSlavePool(nodeSelector);
            if (pool != null && !pool.Disposed)
                return pool.BeginTransaction(dbIndex);
            return null;
        }

        public IRedisPipeline CreatePipeline(bool readOnly = false, int dbIndex = 0)
        {
            ValidateNotDisposed();
            return NextPool(readOnly).CreatePipeline(dbIndex);
        }

        public IRedisPipeline CreatePipeline(Func<RedisManagedNodeInfo, bool> nodeSelector, int dbIndex = 0)
        {
            ValidateNotDisposed();
            var pool = SelectMasterOrSlavePool(nodeSelector);
            if (pool != null && !pool.Disposed)
                return pool.CreatePipeline(dbIndex);
            return null;
        }

        public IRedisAdmin GetAdmin(Func<RedisManagedNodeInfo, bool> nodeSelector)
        {
            ValidateNotDisposed();
            var pool = SelectMasterOrSlavePool(nodeSelector);
            if (pool != null && !pool.Disposed)
                return pool.GetAdmin();
            return null;
        }

        public IRedisDb GetDb(bool readOnly = false, int dbIndex = 0)
        {
            ValidateNotDisposed();
            return NextPool(readOnly).GetDb(dbIndex);
        }

        public IRedisDb GetDb(Func<RedisManagedNodeInfo, bool> nodeSelector, int dbIndex = 0)
        {
            ValidateNotDisposed();
            var pool = SelectMasterOrSlavePool(nodeSelector);
            if (pool != null && !pool.Disposed)
                return pool.GetDb(dbIndex);
            return null;
        }

        public IRedisMonitorChannel GetMonitorChannel(Func<RedisManagedNodeInfo, bool> nodeSelector)
        {
            ValidateNotDisposed();
            var pool = SelectMasterOrSlavePool(nodeSelector);
            if (pool != null && !pool.Disposed)
                return pool.MonitorChannel;
            return null;
        }

        public IRedisPubSubChannel GetPubSubChannel(Func<RedisManagedNodeInfo, bool> nodeSelector)
        {
            ValidateNotDisposed();
            var pool = SelectMasterOrSlavePool(nodeSelector);
            if (pool != null && !pool.Disposed)
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

        private RedisManagedConnectionPool SelectMasterOrSlavePool(Func<RedisManagedNodeInfo, bool> nodeSelector)
        {
            if (nodeSelector != null)
            {
                var msGroup = m_MSGroup;
                if (msGroup != null && !msGroup.Disposed)
                {
                    var pool = SelectPool(msGroup.Slaves, nodeSelector);
                    if (pool == null)
                        pool = SelectPool(msGroup.Masters, nodeSelector);

                    return pool;
                }
            }
            return null;
        }

        private RedisManagedConnectionPool SelectPool(RedisManagedNodesGroup nodesGroup, Func<RedisManagedNodeInfo, bool> nodeSelector)
        {
            if (nodesGroup != null)
            {
                var nodes = nodesGroup.Nodes;
                if (nodes != null)
                {
                    foreach (var node in nodes)
                    {
                        if (node != null && !node.Disposed)
                        {
                            try
                            {
                                var pool = node.Pool;
                                if (pool != null && !pool.Disposed &&
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

        private RedisManagedConnectionPool NextPool(bool readOnly)
        {
            InitializeNodes();

            lock (m_SyncRoot)
            {
                var msGroup = m_MSGroup;
                if (msGroup == null)
                    throw new RedisFatalException("Can not discover masters and slaves", RedisErrorCode.ConnectionError);

                var group = readOnly ? (msGroup.Slaves ?? msGroup.Masters) : msGroup.Masters;
                if (group == null)
                    throw new RedisFatalException(String.Format("No {0} group found", readOnly ? "slave" : "master"), RedisErrorCode.ConnectionError);

                var pool = group.Next();
                if (pool == null)
                    throw new RedisFatalException(String.Format("No {0} node found", readOnly ? "slave" : "master"), RedisErrorCode.ConnectionError);

                return pool;
            }
        }

        private static void DisposeGroups(RedisManagedNodesGroup[] nodeGroups)
        {
            if (nodeGroups != null)
            {
                foreach (var nodeGroup in nodeGroups)
                {
                    try
                    {
                        if (nodeGroup != null)
                            nodeGroup.Dispose();
                    }
                    catch (Exception)
                    { }
                }
            }
        }

        private static void DisposeObjects(IList<IRedisDisposable> disposeList)
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

        #region Initialization

        private void InitializeNodes()
        {
            if (Interlocked.CompareExchange(ref m_InitializationState, (long)InitializationState.Initializing, (long)InitializationState.Undefined) !=
                (long)InitializationState.Initialized)
            {
                lock (m_SyncRoot)
                {
                    if (!Initialized)
                    {
                        try
                        {
                            RefreshAllNodes(false);
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
        }

        #endregion Initialization

        #region Refresh

        private void RefreshAllNodes(bool careValidNodes = true)
        {
            if (Interlocked.CompareExchange(ref m_RefreshState, RedisConstants.One, RedisConstants.Zero) ==
                RedisConstants.Zero)
            {
                try
                {
                    var endPointResolver = m_EndPointResolver;
                    if (endPointResolver == null || endPointResolver.Disposed)
                        return;

                    var tuple = endPointResolver.CreateGroups();
                    if (tuple != null)
                    {
                        var msGroup = tuple.Item1;
                        var sentinels = tuple.Item2;

                        var objectsToDispose = new List<IRedisDisposable>();
                        try
                        {
                            if (!careValidNodes)
                            {
                                try
                                {
                                    var oldMSGroup = Interlocked.Exchange(ref m_MSGroup, msGroup);
                                    if (oldMSGroup != null)
                                        objectsToDispose.Add(oldMSGroup);

                                    var oldSentinels = Interlocked.Exchange(ref m_Sentinels, sentinels);
                                    if (oldSentinels != null)
                                        objectsToDispose.Add(oldSentinels);
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
                                    objectsToDispose.Add(oldMSGroup);
                            }
                            else
                            {
                                objectsToDispose.Add(msGroup);

                                // Masters
                                try
                                {
                                    RearrangeGroup(msGroup.Masters, currMSGroup.Masters,
                                                   (newMasters) => m_MSGroup.ExchangeMasters(newMasters),
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
                                                   (newSlaves) => m_MSGroup.ExchangeSlaves(newSlaves),
                                                   objectsToDispose);
                                }
                                finally
                                {
                                    msGroup.ExchangeSlaves(null);
                                }
                            }

                            // Sentinels
                            RearrangeGroup(sentinels, m_Sentinels,
                                            (newSentinels) => Interlocked.Exchange(ref m_Sentinels, (RedisManagedSentinelGroup)newSentinels),
                                            objectsToDispose);
                        }
                        finally
                        {
                            AttachToSentinel();
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

        private void AttachToSentinel()
        {
            try
            {
                var sentinels = m_Sentinels;
                if (sentinels != null)
                {
                    sentinels.RegisterMessageEvents(MasterSwitched, InstanceStateChanged);
                    sentinels.Monitor(PubSubConnectionDropped);
                }
            }
            catch (Exception)
            { }
        }

        private static void TestNode(RedisManagedNode node, RedisManagedNodesGroup parent)
        {
            try
            {
                if (node != null &&
                    parent != null && !parent.Disposed)
                {
                    if (node.Disposed)
                        parent.RemoveNode(node);
                    else
                    {
                        var pool = node.Pool;
                        if (pool == null || pool.Disposed)
                            parent.RemoveNode(node);
                        else
                            PingPool(pool);
                    }
                }
            }
            catch (Exception)
            { }
        }

        private static bool PingPool(RedisManagedConnectionPool pool)
        {
            if (pool != null && !pool.Disposed)
            {
                try
                {
                    using (var db = pool.GetDb(-1))
                        db.Connection.Ping();

                    pool.SDown = false;
                    pool.ODown = false;

                    return true;
                }
                catch (Exception)
                {
                    pool.SDown = true;
                    pool.ODown = true;
                }
            }
            return false;
        }

        private void RearrangeGroup(RedisManagedNodesGroup newGroup, RedisManagedNodesGroup currGroup,
                                    Func<RedisManagedNodesGroup, RedisManagedNodesGroup> exchangeGroupFunction,
                                    IList<IRedisDisposable> objectsToDispose)
        {
            var newNodes = (newGroup != null) ? newGroup.Nodes : null;
            var currNodes = (currGroup != null) ? currGroup.Nodes : null;

            var newLength = (newNodes != null) ? newNodes.Length : 0;

            if (newNodes == null || newLength == 0 ||
                currNodes == null || currNodes.Length == 0)
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

                        var pool = newNode.ExchangePool(currNode.Pool);
                        if (pool != null)
                            objectsToDispose.Add(pool);
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
                                var oldPool = oldNode.ExchangePool(null);
                                if (!nodesToKeep.ContainsKey(oldNode.EndPoint))
                                {
                                    objectsToDispose.Add(oldPool);
                                    objectsToDispose.Add(oldNode);
                                }
                            }
                        }
                    }
                }
            }
        }

        #endregion Refresh

        #region Instance State Changed

        private void InstanceStateChanged(RedisNodeStateChangedMessage message)
        {
            if (!Disposed && message != null)
            {
                var task = new Task(() =>
                {
                    var instanceEndPoint = message.InstanceEndPoint;
                    if (instanceEndPoint != null && !instanceEndPoint.IsEmpty)
                    {
                        lock (m_SyncRoot)
                        {
                            var instanceRole = ToRedisRole(message.InstanceType);
                            var nodesGroup = GetNodesGroup(instanceRole, message.MasterName);

                            if (nodesGroup != null && !nodesGroup.Disposed)
                                ApplyStateChange(message.Channel, instanceRole, instanceEndPoint, nodesGroup);
                        }
                    }
                });
                task.Start();
            }
        }

        private static RedisRole ToRedisRole(string instanceType)
        {
            var instanceRole = RedisRole.Undefined;
            switch (instanceType)
            {
                case "master":
                    instanceRole = RedisRole.Master;
                    break;
                case "slave":
                    instanceRole = RedisRole.Slave;
                    break;
                case "sentinel":
                    instanceRole = RedisRole.Sentinel;
                    break;
            }
            return instanceRole;
        }

        private RedisManagedNodesGroup GetNodesGroup(RedisRole instanceRole, string masterName)
        {
            var msGroup = m_MSGroup;

            var nodesGroup = (RedisManagedNodesGroup)null;
            switch (instanceRole)
            {
                case RedisRole.Master:
                    nodesGroup =
                        (masterName == m_MasterName) &&
                            (msGroup != null && !msGroup.Disposed) ? msGroup.Masters : null;

                    if (nodesGroup == null || nodesGroup.Disposed)
                    {
                        var masters = new RedisManagedNodesGroup(instanceRole, null);
                        if (msGroup == null || msGroup.Disposed)
                        {
                            msGroup = new RedisManagedMSGroup(masters, new RedisManagedNodesGroup(instanceRole, null));
                            Interlocked.Exchange(ref m_MSGroup, msGroup);
                        }
                        else
                        {
                            var oldMasters = msGroup.ExchangeMasters(masters);
                            if (oldMasters != null)
                                oldMasters.Dispose();
                        }
                    }
                    break;
                case RedisRole.Slave:
                    nodesGroup =
                        (masterName == m_MasterName) &&
                            (msGroup != null && !msGroup.Disposed) ? msGroup.Slaves : null;

                    if (nodesGroup == null || nodesGroup.Disposed)
                    {
                        var slaves = new RedisManagedNodesGroup(instanceRole, null);
                        if (msGroup == null || msGroup.Disposed)
                        {
                            msGroup = new RedisManagedMSGroup(new RedisManagedNodesGroup(instanceRole, null), slaves);
                            Interlocked.Exchange(ref m_MSGroup, msGroup);
                        }
                        else
                        {
                            var oldSlaves = msGroup.ExchangeSlaves(slaves);
                            if (oldSlaves != null)
                                oldSlaves.Dispose();
                        }
                    }
                    break;
                case RedisRole.Sentinel:
                    nodesGroup = m_Sentinels;
                    if (nodesGroup == null || nodesGroup.Disposed)
                    {
                        nodesGroup = new RedisManagedSentinelGroup(m_MasterName, null);
                        Interlocked.Exchange(ref m_Sentinels, (RedisManagedSentinelGroup)nodesGroup);
                    }
                    break;
            }
            return nodesGroup;
        }

        private void ApplyStateChange(string channel, RedisRole instanceRole, RedisEndPoint instanceEndPoint, RedisManagedNodesGroup nodesGroup)
        {
            if (nodesGroup != null && !nodesGroup.Disposed)
            {
                var nodes = nodesGroup.Nodes;
                if (nodes != null)
                {
                    var instanceNode = nodes.FirstOrDefault(n => n != null && instanceEndPoint == n.EndPoint);

                    // Found but disposed
                    if (instanceNode != null && instanceNode.Disposed)
                    {
                        nodesGroup.RemoveNode(instanceNode);
                        instanceNode = null;
                    }

                    if (instanceNode == null)
                    {
                        if (channel == RedisConstants.SDownExited ||
                            channel == RedisConstants.ODownExited ||
                            channel == RedisConstants.SentinelDiscovered)
                        {
                            // TODO: Try to add as new node
                            var endPointResolver = m_EndPointResolver;
                            if (endPointResolver != null && !endPointResolver.Disposed)
                            {
                                var nodeInfo = endPointResolver.DiscoverNode(instanceEndPoint);
                                if (nodeInfo != null)
                                {
                                    var role = nodeInfo.Item1;
                                    var socket = nodeInfo.Item3;

                                    if (role == RedisRole.Undefined)
                                        socket.DisposeSocket();
                                    else
                                    {
                                        var settings = m_Settings.Clone(instanceEndPoint.Host, instanceEndPoint.Port);
                                        var newPool = new RedisManagedConnectionPool(role, m_Name, (RedisPoolSettings)settings);
                                        
                                        instanceNode = new RedisManagedNode(role, newPool);
                                        nodesGroup.AppendNode(instanceNode);

                                        PingPool(newPool);
                                        if (!newPool.IsDown && role == RedisRole.Sentinel)
                                            RefreshSentinels();
                                    }
                                }
                            }
                        }
                        return;
                    }

                    var instancePool = instanceNode.Pool;
                    if (instancePool != null && !instancePool.Disposed)
                    {
                        switch (channel)
                        {
                            case RedisConstants.SDownEntered:
                                instancePool.SDown = true;
                                break;
                            case RedisConstants.SDownExited:
                                instancePool.SDown = false;
                                break;
                            case RedisConstants.ODownEntered:
                                instancePool.ODown = true;
                                break;
                            case RedisConstants.ODownExited:
                                instancePool.ODown = false;
                                break;
                            case RedisConstants.SentinelDiscovered:
                                instancePool.SDown = false;
                                instancePool.ODown = false;
                                break;
                        }
                    }
                }
            }
        }

        #endregion Instance State Changed

        #region Master Switched

        private void MasterSwitched(RedisMasterSwitchedMessage message)
        {
            if (!Disposed && message != null)
            {
                var task = new Task(() =>
                {
                    lock (m_SyncRoot)
                    {
                        var msGroup = m_MSGroup;
                        if (msGroup != null && !msGroup.Disposed)
                        {
                            SetMasterDown(msGroup, message.OldEndPoint, true);
                            PromoteSlaveToMaster(msGroup, message.OldEndPoint);
                        }
                    }
                });
                task.Start();
            }
        }

        private static void SetMasterDown(RedisManagedMSGroup msGroup, RedisEndPoint masterEndPoint, bool isDown)
        {
            if (msGroup != null && !msGroup.Disposed &&
               masterEndPoint != null && !masterEndPoint.IsEmpty)
            {
                var masters = msGroup.Masters;
                if (masters != null && !masters.Disposed)
                {
                    var masterNodes = masters.Nodes;
                    if (masterNodes != null)
                    {
                        var changedNode = masterNodes.FirstOrDefault(n => n != null && !n.Disposed && masterEndPoint == n.EndPoint);
                        if (changedNode != null)
                        {
                            var changedPool = changedNode.Pool;
                            if (changedPool != null && !changedPool.Disposed)
                            {
                                changedPool.SDown = isDown;
                                changedPool.ODown = isDown;
                            }
                        }
                    }
                }
            }
        }

        private static void PromoteSlaveToMaster(RedisManagedMSGroup msGroup, RedisEndPoint slaveEndPoint)
        {
            if (msGroup != null && !msGroup.Disposed &&
               slaveEndPoint != null && !slaveEndPoint.IsEmpty)
            {
                var slaves = msGroup.Slaves;
                if (slaves != null && !slaves.Disposed)
                {
                    var slaveNodes = slaves.Nodes;
                    if (slaveNodes != null)
                    {
                        var slaveNode = slaveNodes.FirstOrDefault(n => n != null && !n.Disposed && slaveEndPoint == n.EndPoint);
                        if (slaveNode != null)
                        {
                            msGroup.ChangeGroup(slaveNode);

                            var changedPool = slaveNode.Pool;
                            if (changedPool != null && !changedPool.Disposed)
                            {
                                changedPool.SDown = false;
                                changedPool.ODown = false;
                            }
                        }
                    }
                }
            }
        }

        #endregion Master Switched

        #region Monitor Sentinels

        private void PubSubConnectionDropped(object sender)
        {
            if (!Disposed)
            {
                var task = new Task(() =>
                {
                    TestNode(sender as RedisManagedNode, m_Sentinels);
                    RefreshSentinels();
                });
                task.Start();
            }
        }

        private void RefreshSentinels()
        {
            if (!Disposed)
            {
                try
                {
                    HealthCheckAll();

                    var sentinels = GetSentinelsGroups();
                    if (sentinels != null)
                    {
                        var nodes = sentinels.Nodes;
                        if (nodes != null && nodes.Length > 0)
                        {
                            sentinels.RegisterMessageEvents(MasterSwitched, InstanceStateChanged);
                            sentinels.Monitor(PubSubConnectionDropped);
                        }
                    }
                }
                catch (Exception)
                { }
            }
        }

        private void HealthCheckAll()
        {
            HealthCheckGroup(m_Sentinels);

            var msGroup = m_MSGroup;
            if (msGroup != null && !msGroup.Disposed)
            {
                HealthCheckGroup(msGroup.Masters);
                HealthCheckGroup(msGroup.Slaves);
            }
        }

        private static void HealthCheckGroup(RedisManagedNodesGroup nodesGroup)
        {
            if (nodesGroup != null && !nodesGroup.Disposed)
            {
                var nodes = nodesGroup.Nodes;
                if (nodes != null)
                {
                    foreach (var node in nodes)
                        TestNode(node, nodesGroup);
                }
            }
        }

        private RedisManagedSentinelGroup GetSentinelsGroups()
        {
            var sentinels = m_Sentinels;

            var discover = sentinels == null;
            if (!discover)
            {
                var nodes = sentinels.Nodes;
                discover = (nodes == null || nodes.Length == 0);
            }

            if (discover)
            {
                DiscoverSentinels();
                sentinels = m_Sentinels;
            }
            return sentinels;
        }

        private void DiscoverSentinels()
        {
            if (!Disposed)
            {
                var msGroup = m_MSGroup;

                var settings = (RedisPoolSettings)null;
                if (msGroup != null && !msGroup.Disposed)
                {
                    settings = FindValidSetting(msGroup.Masters);
                    if (settings == null)
                    {
                        settings = FindValidSetting(msGroup.Slaves);
                        if (settings == null)
                            settings = FindValidSetting(m_Sentinels);
                    }
                }

                if (settings == null)
                    Refresh();
                else
                {
                    // TODO: Get the settings and discover healthy nodes
                }
            }
        }

        private static RedisPoolSettings FindValidSetting(RedisManagedNodesGroup nodesGroup)
        {
            if (nodesGroup != null && !nodesGroup.Disposed)
            {
                var nodes = nodesGroup.Nodes;
                if (nodes != null)
                {
                    foreach (var node in nodes)
                    {
                        try
                        {
                            if (node != null && !node.Disposed)
                            {
                                var pool = node.Pool;
                                if (pool != null && !pool.Disposed)
                                {
                                    PingPool(node.Pool);
                                    return pool.Settings;
                                }
                            }
                        }
                        catch (Exception)
                        { }
                    }
                }
            }

            return null;
        }

        #endregion Monitor Sentinels

        #endregion Methods
    }
}
