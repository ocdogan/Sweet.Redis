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
using System.Net.Sockets;
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
        }

        #endregion .Ctors

        #region Destructors

        protected override void OnDispose(bool disposing)
        {
            base.OnDispose(disposing);

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
                    {
                        RedisCluster cluster = null;
                        RedisManagedNodesGroup sentinels = null;
                        try
                        {
                            CreateCluster(out cluster, out sentinels);
                        }
                        catch (Exception)
                        {
                            if (cluster != null) cluster.Dispose();
                            if (sentinels != null) sentinels.Dispose();
                            throw;
                        }

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
                            if (cluster != null)
                                cluster.Dispose();
                            if (sentinels != null)
                                sentinels.Dispose();
                            throw;
                        }
                        finally
                        {
                            if (oldCluster != null) oldCluster.Dispose();
                            if (oldSentinels != null) oldSentinels.Dispose();
                        }
                    }
                }
            }
        }

        private void CreateCluster(out RedisCluster cluster, out RedisManagedNodesGroup sentinels)
        {
            cluster = null;
            sentinels = null;

            var settings = m_Settings;
            var ipList = SplitToIPEndPoints(settings.EndPoints);

            if (ipList != null && ipList.Count > 0)
            {
                var ipEPSettings = ipList
                        .Select(ep => (RedisPoolSettings)settings.Clone(ep.Address.ToString(), ep.Port))
                        .ToArray();

                if (ipEPSettings != null && ipEPSettings.Length > 0)
                {
                    var discoveredEndPoints = new HashSet<IPEndPoint>();
                    var emptyTuple = new Tuple<RedisRole, RedisConnectionPool>[0];

                    var groups = ipEPSettings
                        .SelectMany(setting => CreateNodes(discoveredEndPoints, Name, setting) ?? emptyTuple)
                        .Where(node => node != null)
                        .GroupBy(
                                tuple => tuple.Item1,
                                tuple => new RedisManagedNode(tuple.Item2, tuple.Item1),
                                (role, group) => new RedisManagedNodesGroup(role, group.ToArray()))
                        .ToList();

                    if (groups != null && groups.Count > 0)
                    {
                        // 0: Masters, 1: Slaves
                        const int MastersPos = 0, SlavesPos = 1;

                        var result = new RedisManagedNodesGroup[2];
                        foreach (var group in groups)
                        {
                            switch (group.Role)
                            {
                                case RedisRole.Master:
                                    result[MastersPos] = group;
                                    break;
                                case RedisRole.Slave:
                                    result[SlavesPos] = group;
                                    break;
                                case RedisRole.Sentinel:
                                    sentinels = group;
                                    break;
                            }
                        }

                        cluster = new RedisCluster(result[MastersPos], result[SlavesPos]);
                    }
                }
            }
        }

        private static Tuple<RedisRole, RedisConnectionPool>[] CreateNodes(HashSet<IPEndPoint> discoveredEndPoints, 
            string name, RedisPoolSettings settings)
        {
            try
            {
                var endPoints = settings.EndPoints;
                if (endPoints == null || endPoints.Length == 0)
                    return null;

                var ipAddresses = endPoints[0].ResolveHost();
                if (ipAddresses == null || ipAddresses.Length == 0)
                    return null;

                var nodeEndPoint = new IPEndPoint(ipAddresses[0], endPoints[0].Port);
                if (discoveredEndPoints.Contains(nodeEndPoint))
                    return null;

                discoveredEndPoints.Add(nodeEndPoint);

                var dispose = false;
                var connectionPool = new RedisConnectionPool(name, settings);
                try
                {
                    var nodeInfo = GetNodeInfo(connectionPool);

                    if (nodeInfo == null || nodeInfo.Item1 == RedisRole.Undefined)
                        dispose = true;
                    else
                    {
                        var role = nodeInfo.Item1;
                        var siblingEndPoints = nodeInfo.Item2;

                        var list = new List<Tuple<RedisRole, RedisConnectionPool>>();
                        if (role == RedisRole.Master || role == RedisRole.Sentinel)
                        {
                            dispose = (role == RedisRole.Sentinel);

                            if (siblingEndPoints == null || siblingEndPoints.Length == 0)
                                return role == RedisRole.Master ? new[] { new Tuple<RedisRole, RedisConnectionPool>(role, connectionPool) } : null;

                            if (role == RedisRole.Master)
                                list.Add(new Tuple<RedisRole, RedisConnectionPool>(role, connectionPool));

                            foreach (var siblingEndPoint in siblingEndPoints)
                            {
                                try
                                {
                                    if (siblingEndPoint != null && !String.IsNullOrEmpty(siblingEndPoint.Host))
                                    {
                                        var siblingSettings = (RedisPoolSettings)settings.Clone(siblingEndPoint.Host, siblingEndPoint.Port);

                                        var otherNodes = CreateNodes(discoveredEndPoints, name, siblingSettings);
                                        if (otherNodes != null)
                                            list.AddRange(otherNodes);
                                    }
                                }
                                catch (Exception)
                                { }
                            }

                            return list.ToArray();
                        }
                        return new[] { new Tuple<RedisRole, RedisConnectionPool>(role, connectionPool) };
                    }
                }
                finally
                {
                    if (dispose)
                        connectionPool.Dispose();
                }
            }
            catch (Exception)
            { }
            return null;
        }

        private static Tuple<RedisRole, RedisEndPoint[]> GetNodeInfo(RedisConnectionPool pool)
        {
            using (var db = pool.GetDb(-1))
            {
                try
                {
                    var serverInfoRaw = db.Server.Info();
                    if (!ReferenceEquals(serverInfoRaw, null))
                    {
                        var serverInfo = serverInfoRaw.Value;
                        if (serverInfo != null)
                        {
                            var serverSection = serverInfo.Server;
                            if (serverSection != null)
                            {
                                var role = RedisRole.Undefined;

                                var redisMode = (serverSection.RedisMode ?? String.Empty).Trim().ToLowerInvariant();
                                if (redisMode == "sentinel")
                                    role = RedisRole.Sentinel;
                                else
                                {
                                    var replicationSection = serverInfo.Replication;
                                    if (replicationSection != null)
                                    {
                                        var roleStr = (replicationSection.Role ?? String.Empty).ToLowerInvariant();
                                        switch (roleStr)
                                        {
                                            case "master":
                                                role = RedisRole.Master;
                                                break;
                                            case "slave":
                                                role = RedisRole.Slave;
                                                break;
                                            case "sentinel":
                                                role = RedisRole.Sentinel;
                                                break;
                                        }
                                    }

                                    if (role == RedisRole.Undefined)
                                        return null;
                                }

                                switch (role)
                                {
                                    case RedisRole.Slave:
                                        return new Tuple<RedisRole, RedisEndPoint[]>(role, null);
                                    case RedisRole.Master:
                                        {
                                            var slaveEndPoints = new List<RedisEndPoint>();

                                            var replicationSection = serverInfo.Replication;
                                            if (replicationSection != null)
                                            {
                                                var slaves = replicationSection.Slaves;
                                                if (slaves != null)
                                                {
                                                    foreach (var slave in slaves)
                                                    {
                                                        try
                                                        {
                                                            if (slave.Port.HasValue && !String.IsNullOrEmpty(slave.IPAddress))
                                                            {
                                                                var endPoint = new RedisEndPoint(slave.IPAddress, slave.Port.Value);
                                                                slaveEndPoints.Add(endPoint);
                                                            }
                                                        }
                                                        catch (Exception)
                                                        { }
                                                    }
                                                }
                                            }
                                            return new Tuple<RedisRole, RedisEndPoint[]>(role, slaveEndPoints.ToArray());
                                        }
                                    case RedisRole.Sentinel:
                                        {
                                            var masterEndPoints = new List<RedisEndPoint>();

                                            var sentinelSection = serverInfo.Sentinel;
                                            if (sentinelSection != null)
                                            {
                                                var masters = sentinelSection.Masters;
                                                if (masters != null)
                                                {
                                                    foreach (var master in masters)
                                                    {
                                                        try
                                                        {
                                                            if (master.Port.HasValue && !String.IsNullOrEmpty(master.IPAddress))
                                                            {
                                                                var endPoint = new RedisEndPoint(master.IPAddress, master.Port.Value);
                                                                masterEndPoints.Add(endPoint);
                                                            }
                                                        }
                                                        catch (Exception)
                                                        { }
                                                    }
                                                }
                                            }
                                            return new Tuple<RedisRole, RedisEndPoint[]>(role, masterEndPoints.ToArray());
                                        }
                                }
                            }
                        }
                    }
                }
                catch (Exception)
                { }
            }
            return null;
        }

        private static HashSet<IPEndPoint> SplitToIPEndPoints(RedisEndPoint[] endPoints)
        {
            if (endPoints != null && endPoints.Length > 0)
            {
                var ipList = new HashSet<IPEndPoint>();
                foreach (var ep in endPoints)
                {
                    if (ep != null && !ep.IsEmpty)
                    {
                        try
                        {
                            var ipAddresses = ep.ResolveHost();
                            if (ipAddresses != null)
                            {
                                var length = ipAddresses.Length;
                                if (length > 0)
                                {
                                    for (var i = 0; i < length; i++)
                                        ipList.Add(new IPEndPoint(ipAddresses[i], ep.Port));
                                }
                            }
                        }
                        catch (Exception)
                        { }
                    }
                }

                return ipList;
            }
            return null;
        }

        #endregion Initialization Methods

        #endregion Methods
    }
}
