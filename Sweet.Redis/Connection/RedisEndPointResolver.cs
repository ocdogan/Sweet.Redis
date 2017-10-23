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
    internal class RedisEndPointResolver : RedisInternalDisposable, IRedisNamedObject
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

        #region GroupedSockets

        protected class GroupedSockets
        {
            #region .Ctors

            public GroupedSockets(RedisSocket[] masters, RedisSocket[] slaves, RedisSocket[] sentinels)
            {
                Masters = masters;
                Slaves = slaves;
                Sentinels = sentinels;
            }

            #endregion .Ctors

            #region Properties

            public RedisSocket[] Masters { get; private set; }
            
            public RedisSocket[] Slaves { get; private set; }

            public RedisSocket[] Sentinels { get; private set; }

            #endregion Properties
        }

        #endregion GroupedSockets

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

        public string Name { get { return m_Name; } }

        public RedisPoolSettings Settings { get { return m_Settings; } }

        #endregion Properties

        #region Methods

        protected GroupedSockets CreateGroupSockets()
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

                        return new GroupedSockets(result[MastersPos], result[SlavesPos], result[SentinelsPos]);
                    }
                }
            }
            return null;
        }

        protected virtual RedisConnection NewConnection(string name, RedisConnectionSettings settings)
        {
            return new RedisDbConnection(name, RedisRole.Any, settings,
                         null,
                         (connection, socket) =>
                         {
                             socket.DisposeSocket();
                         },
                         RedisConstants.MinDbIndex, null, true);
        }

        private Tuple<RedisRole, RedisSocket>[] CreateNodes(HashSet<IPEndPoint> discoveredEndPoints,
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
                        list.Add(new Tuple<RedisRole, RedisSocket>(role, connection.RemoveSocket()));

                        if (siblingEndPoints != null && siblingEndPoints.Length > 0)
                        {
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
                        var role = DiscoverRoleOfNode(serverInfo);

                        switch (role)
                        {
                            case RedisRole.Slave:
                                return GetMasterOfSlave(serverInfo) ?? new NodeRoleAndSiblings(role, null);
                            case RedisRole.Master:
                                return GetSlavesOfMaster(serverInfo) ?? new NodeRoleAndSiblings(role, null);
                            case RedisRole.Sentinel:
                                return GetMastersOfSentinel(masterName, serverInfo) ?? new NodeRoleAndSiblings(role, null);
                        }
                    }
                }
                catch (Exception)
                { }
            }
            return null;
        }

        private static RedisRole DiscoverRoleOfNode(RedisServerInfo serverInfo)
        {
            if (serverInfo != null)
            {
                var serverSection = serverInfo.Server;
                if (serverSection != null)
                {
                    var redisMode = (serverSection.RedisMode ?? String.Empty).Trim().ToLowerInvariant();
                    if (redisMode == "sentinel")
                        return RedisRole.Sentinel;

                    var replicationSection = serverInfo.Replication;
                    if (replicationSection != null)
                    {
                        var roleStr = (replicationSection.Role ?? String.Empty).ToLowerInvariant();
                        switch (roleStr)
                        {
                            case "master":
                                return RedisRole.Master;
                            case "slave":
                                return RedisRole.Slave;
                            case "sentinel":
                                return RedisRole.Sentinel;
                        }
                    }
                }
            }
            return RedisRole.Undefined;
        }

        private static NodeRoleAndSiblings GetMasterOfSlave(RedisServerInfo serverInfo)
        {
            if (serverInfo != null)
            {
                var replicationSection = serverInfo.Replication;
                if (replicationSection != null)
                {
                    try
                    {
                        if (replicationSection.MasterPort.HasValue &&
                            !String.IsNullOrEmpty(replicationSection.MasterHost))
                        {
                            var endPoint = new RedisEndPoint(replicationSection.MasterHost,
                                                             (int)replicationSection.MasterPort.Value);
                            return new NodeRoleAndSiblings(RedisRole.Slave, new[] { endPoint });

                        }
                    }
                    catch (Exception)
                    { }
                }
            }
            return null;
        }

        private static NodeRoleAndSiblings GetSlavesOfMaster(RedisServerInfo serverInfo)
        {
            if (serverInfo != null)
            {
                var replicationSection = serverInfo.Replication;
                if (replicationSection != null)
                {
                    var slaves = replicationSection.Slaves;
                    if (slaves != null)
                    {
                        var slaveEndPoints = new List<RedisEndPoint>();

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

                        return new NodeRoleAndSiblings(RedisRole.Master, slaveEndPoints.ToArray());
                    }
                }
            }
            return null;
        }

        private static NodeRoleAndSiblings GetMastersOfSentinel(string masterName, RedisServerInfo serverInfo)
        {
            if (serverInfo != null)
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
                                            return new NodeRoleAndSiblings(RedisRole.Sentinel, new[] { endPoint });
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

                            return new NodeRoleAndSiblings(RedisRole.Sentinel, masterEndPoints.ToArray());
                        }
                    }
                }
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
