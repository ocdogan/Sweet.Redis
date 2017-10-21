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
using System.Net;
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

        private Guid m_Id;
        private string m_Name;

        private RedisManagerSettings m_Settings;
        private RedisEndPointResolver m_EndPointResolver;

        private RedisCluster m_Cluster;
        private RedisManagedNodesGroup m_Sentinels;

        private long m_InitializationState;
        private readonly object m_SyncRoot = new object();

        #endregion Field Members

        #region .Ctors

        public RedisManager(string name, RedisManagerSettings settings = null)
        {
            if (settings == null)
                throw new RedisFatalException(new ArgumentNullException("settings"), RedisErrorCode.MissingParameter);

            m_Id = Guid.NewGuid();
            m_Settings = settings;
            m_Name = !String.IsNullOrEmpty(name) ? name : m_Id.ToString("N").ToUpper();

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

            var cluster = Interlocked.Exchange(ref m_Cluster, null);
            if (cluster != null)
                cluster.Dispose();
        }

        #endregion Destructors

        #region Properties

        public Guid Id
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

        public IRedisTransaction BeginTransaction(bool readOnly, int dbIndex = 0)
        {
            ValidateNotDisposed();
            return NextPool(readOnly).BeginTransaction(dbIndex);
        }

        public IRedisPipeline CreatePipeline(bool readOnly, int dbIndex = 0)
        {
            ValidateNotDisposed();
            return NextPool(readOnly).CreatePipeline(dbIndex);
        }

        public IRedisDb GetDb(bool readOnly, int dbIndex = 0)
        {
            ValidateNotDisposed();
            return NextPool(readOnly).GetDb(dbIndex);
        }

        public IRedisPubSubChannel GetPubSubChannel(Func<RedisManagedNodeInfo, bool> nodeSelector)
        {
            if (nodeSelector != null)
            {
                var cluster = m_Cluster;
                if (cluster != null && !cluster.Disposed)
                {
                    var pool = SelectPool(cluster.Slaves, nodeSelector);
                    if (pool == null)
                        pool = SelectPool(cluster.Masters, nodeSelector);

                    if (pool != null && !pool.Disposed)
                        return pool.PubSubChannel;
                }
            }
            return null;
        }

        public IRedisMonitorChannel GetMonitorChannel(Func<RedisManagedNodeInfo, bool> nodeSelector)
        {
            if (nodeSelector != null)
            {
                var cluster = m_Cluster;
                if (cluster != null && !cluster.Disposed)
                {
                    var pool = SelectPool(cluster.Slaves, nodeSelector);
                    if (pool == null)
                        pool = SelectPool(cluster.Masters, nodeSelector);

                    if (pool != null && !pool.Disposed)
                        return pool.MonitorChannel;
                }
            }
            return null;
        }

        private RedisConnectionPool SelectPool(RedisManagedNodesGroup group, Func<RedisManagedNodeInfo, bool> nodeSelector)
        {
            if (group != null)
            {
                var nodes = group.Nodes;
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

            var cluster = m_Cluster;
            if (cluster == null)
                throw new RedisFatalException("Can not discover cluster", RedisErrorCode.ConnectionError);

            var group = readOnly ? (cluster.Slaves ?? cluster.Masters) : cluster.Masters;
            if (group == null)
                throw new RedisFatalException(String.Format("No {0} group found", readOnly ? "slave" : "master"), RedisErrorCode.ConnectionError);

            var pool = group.Next();
            if (pool == null)
                throw new RedisFatalException(String.Format("No {0} found", readOnly ? "slave" : "master"), RedisErrorCode.ConnectionError);

            return pool;
        }

        private static void DisposeGroups(RedisManagedNodesGroup[] groups)
        {
            if (groups != null)
            {
                foreach (var group in groups)
                {
                    try
                    {
                        if (group != null)
                            group.Dispose();
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
                        RefreshNodes();
                }
            }
        }

        private void RefreshNodes()
        {
            var endPointResolver = m_EndPointResolver;
            if (endPointResolver == null || endPointResolver.Disposed)
                return;

            var tuple = endPointResolver.CreateCluster();
            if (tuple != null)
            {
                var cluster = tuple.Item1;
                var sentinels = tuple.Item2;

                var oldCluster = (RedisCluster)null;
                var oldSentinels = (RedisManagedNodesGroup)null;
                try
                {
                    oldCluster = Interlocked.Exchange(ref m_Cluster, cluster);
                    oldSentinels = Interlocked.Exchange(ref m_Sentinels, sentinels);

                    Interlocked.Exchange(ref m_InitializationState, (long)InitializationState.Initialized);
                }
                catch (Exception)
                {
                    Interlocked.Exchange(ref m_InitializationState, (long)InitializationState.Undefined);

                    if (cluster != null) cluster.Dispose();
                    if (sentinels != null) sentinels.Dispose();

                    throw;
                }
                finally
                {
                    if (oldCluster != null) oldCluster.Dispose();
                    if (oldSentinels != null) oldSentinels.Dispose();
                }
            }
        }

        #endregion Initialization Methods

        #endregion Methods
    }
}
