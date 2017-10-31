﻿#region License
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
using System.Collections.Concurrent;
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

        #region Static Members

        private static readonly object s_NodeActionQLock = new object();
        private static readonly ConcurrentQueue<Action> s_NodeActionQ = new ConcurrentQueue<Action>();

        #endregion Static Members

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

        private RedisManagerEventQueue m_EventQ;

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

            m_EventQ = new RedisManagerEventQueue(this);
            m_EndPointResolver = new RedisManagedEndPointResolver(m_Name, settings);
        }

        #endregion .Ctors

        #region Destructors

        protected override void OnDispose(bool disposing)
        {
            base.OnDispose(disposing);

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

        public IRedisTransaction BeginTransaction(Func<RedisManagedNodeInfo, bool> nodeSelector, int dbIndex = 0)
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

        public IRedisPipeline CreatePipeline(Func<RedisManagedNodeInfo, bool> nodeSelector, int dbIndex = 0)
        {
            ValidateNotDisposed();
            var pool = SelectMasterOrSlavePool(nodeSelector);
            if (pool.IsAlive())
                return pool.CreatePipeline(dbIndex);
            return null;
        }

        public IRedisAdmin GetAdmin(Func<RedisManagedNodeInfo, bool> nodeSelector)
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

        public IRedisDb GetDb(Func<RedisManagedNodeInfo, bool> nodeSelector, int dbIndex = 0)
        {
            ValidateNotDisposed();
            var pool = SelectMasterOrSlavePool(nodeSelector);
            if (pool.IsAlive())
                return pool.GetDb(dbIndex);
            return null;
        }

        public IRedisMonitorChannel GetMonitorChannel(Func<RedisManagedNodeInfo, bool> nodeSelector)
        {
            ValidateNotDisposed();
            var pool = SelectMasterOrSlavePool(nodeSelector);
            if (pool.IsAlive())
                return pool.MonitorChannel;
            return null;
        }

        public IRedisPubSubChannel GetPubSubChannel(Func<RedisManagedNodeInfo, bool> nodeSelector)
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

        #region Node Selection

        private RedisManagedConnectionPool SelectMasterOrSlavePool(Func<RedisManagedNodeInfo, bool> nodeSelector)
        {
            if (nodeSelector != null)
            {
                var msGroup = m_MSGroup;
                if (msGroup.IsAlive())
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
                        if (node.IsAlive())
                        {
                            try
                            {
                                var pool = node.Pool;
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

        private RedisManagedConnectionPool NextPool(bool readOnly)
        {
            InitializeNodes();

            lock (m_SyncRoot)
            {
                var msGroup = m_MSGroup;
                if (!msGroup.IsAlive())
                    throw new RedisFatalException("Can not discover masters and slaves", RedisErrorCode.ConnectionError);

                var grp = SelectGroup(msGroup, readOnly);
                if (!grp.IsAlive())
                    throw new RedisFatalException(String.Format("No {0} group found", readOnly ? "slave" : "master"), RedisErrorCode.ConnectionError);

                var pool = grp.Next();
                if (!pool.IsAlive())
                    throw new RedisFatalException(String.Format("No {0} node found", readOnly ? "slave" : "master"), RedisErrorCode.ConnectionError);

                return pool;
            }
        }

        private static RedisManagedNodesGroup SelectGroup(RedisManagedMSGroup msGroup, bool readOnly)
        {
            if (msGroup.IsAlive())
            {
                var grp = (RedisManagedNodesGroup)null;
                if (!readOnly)
                    grp = msGroup.Masters;
                else
                {
                    grp = msGroup.Slaves;
                    if (!grp.IsAlive())
                        grp = msGroup.Masters;
                    else
                    {
                        var nodes = grp.Nodes;
                        if (nodes.IsEmpty() ||
                            !nodes.Any(n => n.IsAlive() && n.Pool.IsAlive()))
                            grp = msGroup.Masters;
                    }
                }
                return grp;
            }
            return null;
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

                        var objectsToDispose = new List<IRedisDisposable>();
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

                                        DetachFromCardio(oldMSGroup.Masters);
                                        DetachFromCardio(oldMSGroup.Slaves);
                                    }

                                    if (msGroup.IsAlive())
                                    {
                                        msGroup.SetOnPulseStateChange(OnProbeStateChange);

                                        AttachToCardio(msGroup.Masters);
                                        AttachToCardio(msGroup.Slaves);
                                    }

                                    var oldSentinels = Interlocked.Exchange(ref m_Sentinels, sentinels);
                                    if (oldSentinels.IsAlive())
                                    {
                                        oldSentinels.SetOnPulseStateChange(null);
                                        objectsToDispose.Add(oldSentinels);

                                        DetachFromCardio(oldSentinels);
                                    }

                                    if (sentinels.IsAlive())
                                    {
                                        sentinels.SetOnPulseStateChange(OnProbeStateChange);
                                        AttachToCardio(sentinels);
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

                                    DetachFromCardio(oldMSGroup.Masters);
                                    DetachFromCardio(oldMSGroup.Slaves);
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
                                                       var oldMasters = m_MSGroup.ExchangeMasters(newMasters);
                                                       if (oldMasters != null)
                                                       {
                                                           oldMasters.SetOnPulseStateChange(null);
                                                           DetachFromCardio(oldMasters);
                                                       }
                                                       if (newMasters != null)
                                                       {
                                                           newMasters.SetOnPulseStateChange(OnProbeStateChange);
                                                           AttachToCardio(newMasters);
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
                                                       var oldSlaves = m_MSGroup.ExchangeSlaves(newSlaves);
                                                       if (oldSlaves != null)
                                                       {
                                                           oldSlaves.SetOnPulseStateChange(null);
                                                           DetachFromCardio(oldSlaves);
                                                       }
                                                       if (newSlaves != null)
                                                       {
                                                           newSlaves.SetOnPulseStateChange(OnProbeStateChange);
                                                           AttachToCardio(newSlaves);
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
                                               var oldSentinel = Interlocked.Exchange(ref m_Sentinels, (RedisManagedSentinelGroup)newSentinels);
                                               if (oldSentinel != null)
                                               {
                                                   oldSentinel.SetOnPulseStateChange(null);
                                                   DetachFromCardio(oldSentinel);
                                               }
                                               if (newSentinels != null)
                                               {
                                                   newSentinels.SetOnPulseStateChange(OnProbeStateChange);
                                                   AttachToCardio(newSentinels);
                                               }
                                               return oldSentinel;
                                           },
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

        private static void AttachToCardio(RedisManagedNodesGroup nodesGroup)
        {
            if (nodesGroup.IsAlive())
            {
                var nodes = nodesGroup.Nodes;
                if (nodes != null)
                {
                    foreach (var node in nodes)
                    {
                        if (node != null)
                        {
                            var pool = node.Pool;
                            if (pool.IsAlive())
                                pool.AttachToCardio();
                        }
                    }
                }
            }
        }

        private static void DetachFromCardio(RedisManagedNodesGroup nodesGroup)
        {
            if (nodesGroup.IsAlive())
            {
                var nodes = nodesGroup.Nodes;
                if (nodes != null)
                {
                    foreach (var node in nodes)
                    {
                        if (node != null)
                        {
                            var pool = node.Pool;
                            if (pool.IsAlive())
                                pool.DetachFromCardio();
                        }
                    }
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
                    sentinels.RegisterMessageEvents(OnMasterSwitch, OnSentinelMessage);
                    sentinels.Monitor(OnSentinelConnectionDrop);
                }
            }
            catch (Exception)
            { }
        }

        private RedisRole TestNode(RedisManagedNode node)
        {
            try
            {
                var nodesGroup = GetNodesGroup(node);
                if (nodesGroup.IsAlive())
                {
                    if (!node.IsAlive())
                        nodesGroup.RemoveNode(node);
                    else
                    {
                        var pool = node.Pool;
                        if (!pool.IsAlive())
                        {
                            if (nodesGroup.RemoveNode(node))
                                node.Dispose();

                            return RedisRole.Undefined;
                        }

                        var wasDown = pool.IsDown;
                        var isDown = !PingPool(pool);

                        if (pool.IsDown != isDown)
                            pool.IsDown = isDown;

                        if (wasDown && !pool.IsDown)
                        {
                            var endPointResolver = m_EndPointResolver;
                            if (endPointResolver != null)
                            {
                                var tuple = endPointResolver.DiscoverNode(pool.EndPoint);
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

        private bool TryToIdentifyPool(RedisConnectionPool pool, out RedisManagedNodesGroup nodesGroup, out RedisManagedNode node)
        {
            node = null;
            nodesGroup = null;

            if (pool != null)
            {
                // Sentinels
                var grp = (RedisManagedNodesGroup)m_Sentinels;

                node = FindNodeOfPool(pool, grp);
                if (node != null)
                {
                    nodesGroup = grp;
                    return true;
                }

                var msGroup = m_MSGroup;
                if (msGroup != null)
                {
                    // Masters
                    grp = msGroup.Masters;

                    node = FindNodeOfPool(pool, grp);
                    if (node != null)
                    {
                        nodesGroup = grp;
                        return true;
                    }

                    // Slaves
                    grp = msGroup.Slaves;

                    node = FindNodeOfPool(pool, grp);
                    if (node != null)
                    {
                        nodesGroup = grp;
                        return true;
                    }
                }
            }
            return false;
        }

        private RedisManagedNode FindNodeOfPool(RedisConnectionPool pool, RedisManagedNodesGroup nodesGroup)
        {
            if (pool != null && nodesGroup != null)
            {
                var nodes = nodesGroup.Nodes;
                if (nodes != null)
                {
                    var endPoint = pool.EndPoint;
                    var hasEndPoint = (endPoint != null) && !endPoint.IsEmpty;

                    return nodes.FirstOrDefault(n => !ReferenceEquals(n.Pool, null) &&
                        (ReferenceEquals(n.Pool, pool) || (hasEndPoint && n.Pool.EndPoint == endPoint)));
                }
            }
            return null;
        }

        private RedisManagedNodesGroup GetNodesGroup(RedisManagedNode node)
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

        private bool PingPool(RedisManagedConnectionPool pool)
        {
            if (pool.IsAlive())
            {
                var result = false;
                try
                {
                    result = pool.Ping(pool.IsDown);
                    return result;
                }
                catch (Exception)
                { }
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

        #region Attached Events

        private void OnProbeStateChange(object sender, RedisCardioPulseStatus status)
        {
            if (!Disposed)
            {
                var pool = sender as RedisConnectionPool;
                if (pool != null)
                {
                    var refreshSentinels = true;
                    try
                    {
                        RedisManagedNode node;
                        RedisManagedNodesGroup nodesGroup;

                        if (TryToIdentifyPool(pool, out nodesGroup, out node))
                        {
                            refreshSentinels = false;
                            InvokeNodeStateChanged(node);
                        }
                    }
                    finally
                    {
                        if (refreshSentinels)
                            RefreshSentinels();
                    }
                }
            }
        }

        private void OnSentinelMessage(RedisNodeStateChangedMessage message)
        {
            if (!Disposed && message != null)
            {
                var eventQ = m_EventQ;
                if (eventQ.IsAlive())
                {
                    eventQ.Enqueu(() =>
                    {
                        if (!Disposed)
                        {
                            var instanceEndPoint = message.InstanceEndPoint;
                            if (!instanceEndPoint.IsEmpty())
                            {
                                lock (m_SyncRoot)
                                {
                                    var instanceRole = ToRedisRole(message.InstanceType);
                                    var nodesGroup = GetNodesGroup(instanceRole, message.MasterName);

                                    if (nodesGroup.IsAlive())
                                        ApplyStateChange(message.Channel, instanceRole, instanceEndPoint, nodesGroup);
                                }
                            }
                        }
                    });
                }
            }
        }

        private void OnSentinelConnectionDrop(object sender)
        {
            if (!Disposed && !ReferenceEquals(sender, null))
            {
                var task = new Task(() =>
                {
                    var node = sender as RedisManagedNode;
                    if (node.IsAlive())
                        TestNode(node);

                    RefreshSentinels();
                });
                task.Start();
            }
        }

        private void OnMasterSwitch(RedisMasterSwitchedMessage message)
        {
            if (!Disposed && message != null)
            {
                var eventQ = m_EventQ;
                if (eventQ.IsAlive())
                {
                    eventQ.Enqueu(() =>
                    {
                        if (!Disposed)
                        {
                            lock (m_SyncRoot)
                            {
                                PromoteToMaster(m_MSGroup, message.NewEndPoint, message.OldEndPoint);
                            }
                        }
                    });
                }
            }
        }

        #endregion Attached Events

        private void InvokeNodeStateChanged(object sender)
        {
            if (!Disposed && !ReferenceEquals(sender, null))
            {
                var task = new Task(() =>
                {
                    var node = sender as RedisManagedNode;
                    if (node.IsAlive())
                    {
                        TestNode(node);

                        var pool = node.Pool;
                        if (pool.IsAlive())
                        {
                            switch (pool.Role)
                            {
                                case RedisRole.Sentinel:
                                    RefreshSentinels();
                                    break;
                                case RedisRole.Master:
                                    if (!pool.IsDown)
                                        NodeIsUp(node);
                                    else
                                        MasterIsDown(m_MSGroup, pool.EndPoint, true);
                                    break;
                            }
                            return;
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
                            msGroup.IsAlive() ? msGroup.Masters : null;

                    if (!nodesGroup.IsAlive())
                    {
                        var masters = new RedisManagedNodesGroup(instanceRole, null, null);
                        if (!msGroup.IsAlive())
                        {
                            msGroup = new RedisManagedMSGroup(masters, new RedisManagedNodesGroup(instanceRole, null, null), OnProbeStateChange);
                            var oldMSGroup = Interlocked.Exchange(ref m_MSGroup, msGroup);

                            if (oldMSGroup != null)
                            {
                                DetachFromCardio(oldMSGroup.Masters);
                                DetachFromCardio(oldMSGroup.Slaves);
                            }

                            AttachToCardio(msGroup.Masters);
                            AttachToCardio(msGroup.Slaves);
                        }
                        else
                        {
                            var oldMasters = msGroup.ExchangeMasters(masters);
                            if (oldMasters != null)
                            {
                                DetachFromCardio(oldMasters);
                                oldMasters.Dispose();
                            }

                            AttachToCardio(masters);
                        }
                    }
                    break;
                case RedisRole.Slave:
                    nodesGroup =
                        (masterName == m_MasterName) &&
                            msGroup.IsAlive() ? msGroup.Slaves : null;

                    if (!nodesGroup.IsAlive())
                    {
                        var slaves = new RedisManagedNodesGroup(instanceRole, null, null);
                        if (!msGroup.IsAlive())
                        {
                            msGroup = new RedisManagedMSGroup(new RedisManagedNodesGroup(instanceRole, null, null), slaves, OnProbeStateChange);
                            var oldMSGroup = Interlocked.Exchange(ref m_MSGroup, msGroup);

                            if (oldMSGroup != null)
                            {
                                DetachFromCardio(oldMSGroup.Masters);
                                DetachFromCardio(oldMSGroup.Slaves);
                            }

                            AttachToCardio(msGroup.Masters);
                            AttachToCardio(msGroup.Slaves);
                        }
                        else
                        {
                            var oldSlaves = msGroup.ExchangeSlaves(slaves);
                            if (oldSlaves != null)
                            {
                                DetachFromCardio(oldSlaves);
                                oldSlaves.Dispose();
                            }

                            AttachToCardio(slaves);
                        }
                    }
                    break;
                case RedisRole.Sentinel:
                    nodesGroup = m_Sentinels;
                    if (!nodesGroup.IsAlive())
                    {
                        nodesGroup = new RedisManagedSentinelGroup(m_MasterName, null, OnProbeStateChange);
                        var oldSentinels = Interlocked.Exchange(ref m_Sentinels, (RedisManagedSentinelGroup)nodesGroup);

                        DetachFromCardio(oldSentinels);
                        AttachToCardio(nodesGroup);
                    }
                    break;
            }
            return nodesGroup;
        }

        private void ApplyStateChange(string channel, RedisRole instanceRole, RedisEndPoint instanceEndPoint, RedisManagedNodesGroup nodesGroup)
        {
            if (nodesGroup.IsAlive())
            {
                var nodes = nodesGroup.Nodes;
                if (nodes != null)
                {
                    var instanceNode = nodes.FirstOrDefault(n => n.IsAlive() && n.EndPoint == instanceEndPoint);

                    // Found but disposed
                    if (!instanceNode.IsAlive())
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
                            if (endPointResolver.IsAlive())
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

                                        instanceNode = new RedisManagedNode(role, newPool, null);
                                        nodesGroup.AppendNode(instanceNode);

                                        var wasDown = newPool.IsDown;
                                        var isDown = !PingPool(newPool);

                                        if (newPool.IsDown != isDown)
                                            newPool.IsDown = isDown;

                                        if (!newPool.IsDown && role == RedisRole.Sentinel)
                                            RefreshSentinels();
                                    }
                                }
                            }
                        }
                        return;
                    }

                    var instancePool = instanceNode.Pool;
                    if (instancePool.IsAlive())
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

        private void NodeIsUp(RedisManagedNode node)
        {
            if (node.IsAlive())
            {
                var nodesGroup = GetNodesGroup(node);
                if (!nodesGroup.IsAlive())
                {
                    var pool = node.Pool;
                    if (pool.IsAlive() && pool.IsDown)
                        pool.IsDown = false;

                    return;
                }

                var masterNodes = nodesGroup.Nodes;
                if (masterNodes != null)
                {
                    var updatedNode = masterNodes.FirstOrDefault(n => n.IsAlive() && n.EndPoint == node.EndPoint);
                    if (updatedNode.IsAlive())
                    {
                        var updatedPool = updatedNode.Pool;
                        if (updatedPool.IsAlive() && updatedPool.IsDown)
                            updatedPool.IsDown = false;
                    }
                }
            }
        }

        private static bool MasterIsDown(RedisManagedMSGroup msGroup, RedisEndPoint masterEndPoint, bool isDown)
        {
            if (msGroup.IsAlive() && !masterEndPoint.IsEmpty())
            {
                var masters = msGroup.Masters;
                if (masters.IsAlive())
                {
                    var masterNodes = masters.Nodes;
                    if (masterNodes != null)
                    {
                        var updatedNode = masterNodes.FirstOrDefault(n => n.IsAlive() && n.EndPoint == masterEndPoint);
                        if (updatedNode.IsAlive())
                        {
                            var updatedPool = updatedNode.Pool;
                            if (updatedPool.IsAlive())
                            {
                                if (updatedPool.IsDown != isDown)
                                    updatedPool.IsDown = isDown;

                                return true;
                            }
                        }
                    }
                }
            }
            return false;
        }

        private static void PromoteToMaster(RedisManagedMSGroup msGroup, RedisEndPoint newEndPoint, RedisEndPoint oldEndPoint)
        {
            if (msGroup.IsAlive() && !newEndPoint.IsEmpty())
            {
                MasterIsDown(msGroup, oldEndPoint, true);

                var switched = false;
                try
                {
                    var slaves = msGroup.Slaves;
                    if (slaves.IsAlive())
                    {
                        var slaveNodes = slaves.Nodes;
                        if (slaveNodes != null)
                        {
                            var slaveNode = slaveNodes.FirstOrDefault(n => n.IsAlive() && n.EndPoint == newEndPoint);
                            if (slaveNode.IsAlive())
                            {
                                msGroup.ChangeGroup(slaveNode);

                                var changedPool = slaveNode.Pool;
                                if (changedPool.IsAlive())
                                {
                                    changedPool.SDown = false;
                                    changedPool.ODown = false;

                                    switched = true;
                                }
                            }
                        }
                    }
                }
                finally
                {
                    if (!switched)
                        MasterIsDown(msGroup, newEndPoint, false);
                }
            }
        }

        private void RefreshSentinels()
        {
            if (!Disposed)
            {
                try
                {
                    HealthCheckGroup(m_Sentinels);

                    var sentinels = GetSentinelsGroups();
                    if (sentinels.IsAlive())
                    {
                        var nodes = sentinels.Nodes;
                        if (!nodes.IsEmpty())
                        {
                            sentinels.RegisterMessageEvents(OnMasterSwitch, OnSentinelMessage);
                            sentinels.Monitor(OnSentinelConnectionDrop);
                        }
                    }
                }
                catch (Exception)
                { }
            }
        }

        private void RefreshMSGroup()
        {
            if (!Disposed)
            {
                var msGroup = m_MSGroup;
                if (msGroup.IsAlive())
                {
                    HealthCheckGroup(msGroup.Masters);
                    HealthCheckGroup(msGroup.Slaves);
                }
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

        private RedisManagedSentinelGroup GetSentinelsGroups()
        {
            var sentinels = m_Sentinels;

            var discover = sentinels == null;
            if (!discover)
            {
                var nodes = sentinels.Nodes;
                discover = nodes.IsEmpty();
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
                if (msGroup.IsAlive())
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

        private RedisPoolSettings FindValidSetting(RedisManagedNodesGroup nodesGroup)
        {
            if (nodesGroup.IsAlive())
            {
                var nodes = nodesGroup.Nodes;
                if (nodes != null)
                {
                    foreach (var node in nodes)
                    {
                        try
                        {
                            if (node.IsAlive())
                            {
                                var pool = node.Pool;
                                if (pool.IsAlive() && PingPool(node.Pool))
                                    return pool.Settings;
                            }
                        }
                        catch (Exception)
                        { }
                    }
                }
            }
            return null;
        }

        #endregion Node State Management

        #endregion Methods
    }
}
