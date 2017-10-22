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

        private RedisManagerSettings m_Settings;
        private RedisEndPointResolver m_EndPointResolver;

        private RedisManagedMSGroup m_MSGroup;
        private RedisManagedNodesGroup m_Sentinels;

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

            m_EndPointResolver = new RedisEndPointResolver(m_Name, settings);
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

        public string Name
        {
            get { return m_Name; }
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
            if (nodeSelector != null)
            {
                var msGroup = m_MSGroup;
                if (msGroup != null && !msGroup.Disposed)
                {
                    var pool = SelectPool(msGroup.Slaves, nodeSelector);
                    if (pool == null)
                        pool = SelectPool(msGroup.Masters, nodeSelector);

                    if (pool != null && !pool.Disposed)
                        return pool.BeginTransaction(dbIndex);
                }
            }
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
            if (nodeSelector != null)
            {
                var msGroup = m_MSGroup;
                if (msGroup != null && !msGroup.Disposed)
                {
                    var pool = SelectPool(msGroup.Slaves, nodeSelector);
                    if (pool == null)
                        pool = SelectPool(msGroup.Masters, nodeSelector);

                    if (pool != null && !pool.Disposed)
                        return pool.CreatePipeline(dbIndex);
                }
            }
            return null;
        }

        public IRedisAdmin GetAdmin(Func<RedisManagedNodeInfo, bool> nodeSelector)
        {
            ValidateNotDisposed();
            if (nodeSelector != null)
            {
                var msGroup = m_MSGroup;
                if (msGroup != null && !msGroup.Disposed)
                {
                    var pool = SelectPool(msGroup.Slaves, nodeSelector);
                    if (pool == null)
                        pool = SelectPool(msGroup.Masters, nodeSelector);

                    if (pool != null && !pool.Disposed)
                        return pool.GetAdmin();
                }
            }
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
            if (nodeSelector != null)
            {
                var msGroup = m_MSGroup;
                if (msGroup != null && !msGroup.Disposed)
                {
                    var pool = SelectPool(msGroup.Slaves, nodeSelector);
                    if (pool == null)
                        pool = SelectPool(msGroup.Masters, nodeSelector);

                    if (pool != null && !pool.Disposed)
                        return pool.GetDb(dbIndex);
                }
            }
            return null;
        }

        public IRedisMonitorChannel GetMonitorChannel(Func<RedisManagedNodeInfo, bool> nodeSelector)
        {
            ValidateNotDisposed();
            if (nodeSelector != null)
            {
                var msGroup = m_MSGroup;
                if (msGroup != null && !msGroup.Disposed)
                {
                    var pool = SelectPool(msGroup.Slaves, nodeSelector);
                    if (pool == null)
                        pool = SelectPool(msGroup.Masters, nodeSelector);

                    if (pool != null && !pool.Disposed)
                        return pool.MonitorChannel;
                }
            }
            return null;
        }

        public IRedisPubSubChannel GetPubSubChannel(Func<RedisManagedNodeInfo, bool> nodeSelector)
        {
            ValidateNotDisposed();
            if (nodeSelector != null)
            {
                var msGroup = m_MSGroup;
                if (msGroup != null && !msGroup.Disposed)
                {
                    var pool = SelectPool(msGroup.Slaves, nodeSelector);
                    if (pool == null)
                        pool = SelectPool(msGroup.Masters, nodeSelector);

                    if (pool != null && !pool.Disposed)
                        return pool.PubSubChannel;
                }
            }
            return null;
        }

        public void Refresh()
        {
            RefreshNodes(true);
        }

        private RedisConnectionPool SelectPool(RedisManagedNodesGroup nodesGroup, Func<RedisManagedNodeInfo, bool> nodeSelector)
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
                            var pool = node.Pool;
                            if (pool != null && !pool.Disposed &&
                                nodeSelector(node.GetNodeInfo()))
                                return pool;
                        }
                    }
                }
            }
            return null;
        }

        private RedisConnectionPool NextPool(bool readOnly)
        {
            InitializeNodes();

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

        #region Initialization Methods

        private void InitializeNodes()
        {
            if (Interlocked.CompareExchange(ref m_InitializationState, (long)InitializationState.Initializing, (long)InitializationState.Undefined) !=
                (long)InitializationState.Initialized)
            {
                lock (m_SyncRoot)
                {
                    if (Interlocked.Read(ref m_InitializationState) != (long)InitializationState.Initialized)
                    {
                        try
                        {
                            RefreshNodes(false);
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

        private void RefreshNodes(bool careValidNodes = true)
        {
            var endPointResolver = m_EndPointResolver;
            if (endPointResolver == null || endPointResolver.Disposed)
                return;

            var tuple = endPointResolver.CreateGroups();
            if (tuple != null)
            {
                var msGroup = tuple.Item1;
                var sentinels = tuple.Item2;

                var disposeList = new List<IRedisDisposable>();
                try
                {
                    if (!careValidNodes)
                    {
                        try
                        {
                            var oldMSGroup = Interlocked.Exchange(ref m_MSGroup, msGroup);
                            if (oldMSGroup != null)
                                disposeList.Add(oldMSGroup);

                            var oldSentinels = Interlocked.Exchange(ref m_Sentinels, sentinels);
                            if (oldSentinels != null)
                                disposeList.Add(oldSentinels);
                        }
                        catch (Exception)
                        {
                            if (msGroup != null) msGroup.Dispose();
                            if (sentinels != null) sentinels.Dispose();
                            throw;
                        }
                        return;
                    }

                    var currMSGroup = m_MSGroup;
                    if (msGroup == null || currMSGroup == null)
                    {
                        var oldMSGroup = Interlocked.Exchange(ref m_MSGroup, msGroup);
                        if (oldMSGroup != null)
                            disposeList.Add(oldMSGroup);
                    }
                    else
                    {
                        RearrangeGroup(msGroup.Masters, currMSGroup.Masters,
                                       (newGroup) => m_MSGroup.ExchangeMasters(newGroup),
                                       disposeList);
                        RearrangeGroup(msGroup.Slaves, currMSGroup.Slaves,
                                       (newGroup) => m_MSGroup.ExchangeSlaves(newGroup),
                                       disposeList);
                    }

                    RearrangeGroup(sentinels, m_Sentinels,
                                   (ng) => Interlocked.Exchange(ref m_Sentinels, ng),
                                   disposeList);
                }
                finally
                {
                    DisposeObjects(disposeList);
                }
            }
        }

        private void RearrangeGroup(RedisManagedNodesGroup newGroup, RedisManagedNodesGroup currGroup,
                                    Func<RedisManagedNodesGroup, RedisManagedNodesGroup> exchangeFunction,
                                    IList<IRedisDisposable> disposeList)
        {
            if (newGroup == null || newGroup.Nodes == null || newGroup.Nodes.Length == 0 ||
                currGroup == null || currGroup.Nodes == null || currGroup.Nodes.Length == 0)
            {
                var oldGroup = exchangeFunction(newGroup);
                if (oldGroup != null)
                    disposeList.Add(oldGroup);
            }
            else
            {
                var currNodes = currGroup.Nodes.ToDictionary(n => n.EndPoint);

                var newNodes = newGroup.Nodes;
                var newLength = newNodes.Length;

                var nodesToKeep = new Dictionary<RedisEndPoint, RedisManagedNode>();

                for (var i = 0; i < newLength; i++)
                {
                    var newNode = newNodes[i];

                    RedisManagedNode currNode;
                    if (currNodes.TryGetValue(newNode.EndPoint, out currNode))
                    {
                        nodesToKeep[currNode.EndPoint] = currNode;

                        var pool = newNode.ExchangePool(currNode.Pool);
                        if (pool != null)
                            disposeList.Add(pool);
                    }
                }

                var oldGroup = exchangeFunction(newGroup);

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
                                    disposeList.Add(oldPool);
                                    disposeList.Add(oldNode);
                                }
                            }
                        }
                    }
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

        #endregion Initialization Methods

        #endregion Methods
    }
}
