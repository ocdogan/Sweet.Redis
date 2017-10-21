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
    internal class RedisEndPointResolver : RedisInternalDisposable
    {
        #region NodeRoleAndSiblings

        private class NodeRoleAndSiblings
        {
            #region .Ctors

            public NodeRoleAndSiblings(RedisRole role, RedisEndPoint[] siblings)
            {
                Role = role;
                Siblings = siblings;
            }

            #endregion .Ctors

            #region Properties

            public RedisRole Role { get; private set; }

            public RedisEndPoint[] Siblings { get; private set; }

            #endregion Properties
        }

        #endregion NodeRoleAndSiblings

        #region Field Members

        private string m_Name;
        private RedisPoolSettings m_Settings;

        #endregion Field Members

        #region .Ctors

        public RedisEndPointResolver(string name, RedisPoolSettings settings)
        {
            if (settings == null)
                throw new RedisFatalException(new ArgumentNullException("settings"), RedisErrorCode.MissingParameter);

            m_Name = name;
            m_Settings = settings;
        }

        #endregion .Ctors

        #region Destructors

        protected override void OnDispose(bool disposing)
        {
            base.OnDispose(disposing);
            Interlocked.Exchange(ref m_Settings, null);
        }

        #endregion Destructors

        #region Properties

        public RedisPoolSettings Settings
        {
            get { return m_Settings; }
        }

        #endregion Properties

        #region Methods        		

        public Tuple<RedisManagedMSGroup, RedisManagedNodesGroup> CreateGroups()
        {
            ValidateNotDisposed();

            var tuple = CreateGroupSockets();
            if (tuple != null)
            {
                RedisManagedNodesGroup sentinels = null;
                RedisManagedMSGroup mastersAndSlaves = null;

                RedisManagedNodesGroup slaves = null;
                RedisManagedNodesGroup masters = null;
                try
                {
                    masters = ToNodesGroup(RedisRole.Master, tuple.Item1);
                    try
                    {
                        slaves = ToNodesGroup(RedisRole.Slave, tuple.Item2);
                        try
                        {
                            sentinels = ToNodesGroup(RedisRole.Sentinel, tuple.Item3);
                        }
                        catch (Exception)
                        {
                            if (sentinels != null)
                            {
                                sentinels.Dispose();
                                sentinels = null;
                            }
                            throw;
                        }
                    }
                    catch (Exception)
                    {
                        if (slaves != null)
                        {
                            slaves.Dispose();
                            slaves = null;
                        }
                        throw;
                    }
                }
                catch (Exception)
                {
                    if (masters != null)
                    {
                        masters.Dispose();
                        masters = null;
                    }
                    throw;
                }

                mastersAndSlaves = new RedisManagedMSGroup(masters, slaves);
                return new Tuple<RedisManagedMSGroup, RedisManagedNodesGroup>(mastersAndSlaves, sentinels);
            }
            return null;
        }

        private RedisManagedNodesGroup ToNodesGroup(RedisRole role, RedisSocket[] sockets)
        {
            if (sockets != null && sockets.Length > 0)
            {
                var nodeList = new List<RedisManagedNode>();
                foreach (var socket in sockets)
                {
                    try
                    {
                        if (socket.IsConnected())
                        {
                            var endPoint = socket.RemoteEP;
                            var settings = (RedisPoolSettings)m_Settings.Clone(endPoint.Address.ToString(), endPoint.Port);

                            var pool = new RedisConnectionPool(m_Name, settings);
                            pool.ReuseSocket(socket);

                            nodeList.Add(new RedisManagedNode(role, pool));
                        }
                    }
                    catch (Exception)
                    {
                        socket.DisposeSocket();
                    }
                }

                if (nodeList.Count > 0)
                    return new RedisManagedNodesGroup(role, nodeList.ToArray());
            }
            return null;
        }

        private Tuple<RedisSocket[], RedisSocket[], RedisSocket[]> CreateGroupSockets()
        {
            var settings = m_Settings;
            var ipEPList = SplitToIPEndPoints(settings.EndPoints);

            if (ipEPList != null && ipEPList.Count > 0)
            {
                var ipEPSettings = ipEPList
                        .Select(ep => (RedisPoolSettings)settings.Clone(ep.Address.ToString(), ep.Port))
                        .ToArray();

                if (ipEPSettings != null && ipEPSettings.Length > 0)
                {
                    var discoveredEndPoints = new HashSet<IPEndPoint>();
                    var emptyTuple = new Tuple<RedisRole, RedisSocket>[0];

                    var groupsTuples = ipEPSettings
                        .SelectMany(setting => CreateNodes(discoveredEndPoints, m_Name, setting) ?? emptyTuple)
                        .Where(node => node != null)
                        .GroupBy(
                            tuple => tuple.Item1,
                            tuple => tuple.Item2,
                            (role, group) => new Tuple<RedisRole, RedisSocket[]>(role, group.ToArray()))
                        .ToList();

                    if (groupsTuples != null && groupsTuples.Count > 0)
                    {
                        // 0: Masters, 1: Slaves, 2: Sentinels
                        const int MastersPos = 0, SlavesPos = 1, SentinelsPos = 2;

                        var result = new RedisSocket[3][];
                        foreach (var tuple in groupsTuples)
                        {
                            switch (tuple.Item1)
                            {
                                case RedisRole.Master:
                                    result[MastersPos] = tuple.Item2;
                                    break;
                                case RedisRole.Slave:
                                    result[SlavesPos] = tuple.Item2;
                                    break;
                                case RedisRole.Sentinel:
                                    result[SentinelsPos] = tuple.Item2;
                                    break;
                            }
                        }

                        return new Tuple<RedisSocket[], RedisSocket[], RedisSocket[]>(result[MastersPos],
                                 result[SlavesPos], result[SentinelsPos]);
                    }
                }
            }
            return null;
        }

        private static RedisConnection NewConnection(string name, RedisConnectionSettings settings)
        {
            return new RedisDbConnection(name, RedisRole.Any, settings,
                         null,
                         (connection, socket) =>
                         {
                             socket.DisposeSocket();
                         },
                         RedisConstants.MinDbIndex, null, true);
        }

        private static Tuple<RedisRole, RedisSocket>[] CreateNodes(HashSet<IPEndPoint> discoveredEndPoints,
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

                using (var connection = NewConnection(name, settings))
                {
                    var nodeInfo = GetNodeInfo(settings.MasterName, connection);

                    if (!(nodeInfo == null || nodeInfo.Role == RedisRole.Undefined))
                    {
                        var role = nodeInfo.Role;
                        var siblingEndPoints = nodeInfo.Siblings;

                        var list = new List<Tuple<RedisRole, RedisSocket>>();
                        if (role == RedisRole.Slave)
                            return new[] { new Tuple<RedisRole, RedisSocket>(role, connection.RemoveSocket()) };

                        if (siblingEndPoints == null || siblingEndPoints.Length == 0)
                            return role == RedisRole.Master ? new[] { new Tuple<RedisRole, RedisSocket>(role, connection.RemoveSocket()) } : null;

                        list.Add(new Tuple<RedisRole, RedisSocket>(role, connection.RemoveSocket()));

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
                }
            }
            catch (Exception)
            { }
            return null;
        }

        private static NodeRoleAndSiblings GetNodeInfo(string masterName, IRedisConnection connection)
        {
            if (connection != null)
            {
                try
                {
                    var serverInfo = connection.GetServerInfo();
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
                                    return new NodeRoleAndSiblings(role, null);
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
                                        return new NodeRoleAndSiblings(role, slaveEndPoints.ToArray());
                                    }
                                case RedisRole.Sentinel:
                                    {
                                        var sentinelSection = serverInfo.Sentinel;
                                        if (sentinelSection != null)
                                        {
                                            var masters = sentinelSection.Masters;
                                            if (masters != null)
                                            {
                                                var mastersLength = masters.Length;
                                                if (mastersLength > 0)
                                                {
                                                    if (String.IsNullOrEmpty(masterName))
                                                    {
                                                        if (mastersLength == 1)
                                                        {
                                                            var master = masters[0];
                                                            try
                                                            {
                                                                if (master.Port.HasValue && !String.IsNullOrEmpty(master.IPAddress))
                                                                {
                                                                    var endPoint = new RedisEndPoint(master.IPAddress, master.Port.Value);
                                                                    return new NodeRoleAndSiblings(role, new[] { endPoint });
                                                                }
                                                            }
                                                            catch (Exception)
                                                            { }
                                                        }
                                                        return null;
                                                    }

                                                    var masterEndPoints = new List<RedisEndPoint>();

                                                    foreach (var master in masters)
                                                    {
                                                        try
                                                        {
                                                            if (master.Name == masterName &&
                                                                master.Port.HasValue && !String.IsNullOrEmpty(master.IPAddress))
                                                            {
                                                                var endPoint = new RedisEndPoint(master.IPAddress, master.Port.Value);
                                                                masterEndPoints.Add(endPoint);
                                                            }
                                                        }
                                                        catch (Exception)
                                                        { }
                                                    }

                                                    return new NodeRoleAndSiblings(role, masterEndPoints.ToArray());
                                                }
                                            }
                                        }
                                        break;
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
                var ipEPList = new HashSet<IPEndPoint>();
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
                                        ipEPList.Add(new IPEndPoint(ipAddresses[i], ep.Port));
                                }
                            }
                        }
                        catch (Exception)
                        { }
                    }
                }

                return ipEPList;
            }
            return null;
        }

        #endregion Methods
    }
}
