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

            var cluster = Interlocked.Exchange(ref m_MSGroup, null);
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
                        RefreshNodes();
                }
            }
        }

        private void RefreshNodes()
        {
            var endPointResolver = m_EndPointResolver;
            if (endPointResolver == null || endPointResolver.Disposed)
                return;

            var tuple = endPointResolver.CreateGroups();
            if (tuple != null)
            {
                var msGroup = tuple.Item1;
                var sentinels = tuple.Item2;

                var oldMSGroup = (RedisManagedMSGroup)null;
                var oldSentinels = (RedisManagedNodesGroup)null;
                try
                {
                    oldMSGroup = Interlocked.Exchange(ref m_MSGroup, msGroup);
                    oldSentinels = Interlocked.Exchange(ref m_Sentinels, sentinels);

                    Interlocked.Exchange(ref m_InitializationState, (long)InitializationState.Initialized);
                }
                catch (Exception)
                {
                    Interlocked.Exchange(ref m_InitializationState, (long)InitializationState.Undefined);

                    if (msGroup != null) msGroup.Dispose();
                    if (sentinels != null) sentinels.Dispose();

                    throw;
                }
                finally
                {
                    if (oldMSGroup != null) oldMSGroup.Dispose();
                    if (oldSentinels != null) oldSentinels.Dispose();
                }
            }
        }

        #endregion Initialization Methods

        #endregion Methods
    }
}
