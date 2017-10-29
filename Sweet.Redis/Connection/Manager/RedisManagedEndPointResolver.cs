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

namespace Sweet.Redis
{
    internal class RedisManagedEndPointResolver : RedisEndPointResolver
    {
        #region .Ctors

        public RedisManagedEndPointResolver(string name, RedisManagerSettings settings)
            : base(name, settings)
        { }

        #endregion .Ctors

        #region Methods

        public Tuple<RedisManagedMSGroup, RedisManagedSentinelGroup> CreateGroups()
        {
            ValidateNotDisposed();

            var tuple = CreateGroupSockets();
            if (tuple != null)
            {
                RedisManagedSentinelGroup sentinels = null;
                RedisManagedMSGroup mastersAndSlaves = null;

                RedisManagedNodesGroup slaves = null;
                RedisManagedNodesGroup masters = null;
                try
                {
                    masters = ToNodesGroup(RedisRole.Master, tuple.Masters);
                    try
                    {
                        slaves = ToNodesGroup(RedisRole.Slave, tuple.Slaves);
                        try
                        {
                            sentinels = (RedisManagedSentinelGroup)ToNodesGroup(RedisRole.Sentinel, tuple.Sentinels);
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
                return new Tuple<RedisManagedMSGroup, RedisManagedSentinelGroup>(mastersAndSlaves, sentinels);
            }
            return null;
        }

        public Tuple<RedisRole, RedisEndPoint[], RedisSocket> DiscoverNode(RedisEndPoint endPoint)
        {
            if (endPoint.IsEmpty())
                throw new RedisFatalException(new ArgumentNullException("endPoint"), RedisErrorCode.MissingParameter);

            ValidateNotDisposed();

            var settings = Settings.Clone(endPoint.Host, endPoint.Port);

            using (var connection = NewConnection(settings))
            {
                var nodeInfo = GetNodeInfo(settings.MasterName, connection);
                if (!(nodeInfo == null || nodeInfo.Role == RedisRole.Undefined))
                {
                    var role = nodeInfo.Role;
                    var siblingEndPoints = nodeInfo.Siblings;

                    return new Tuple<RedisRole, RedisEndPoint[], RedisSocket>(role, siblingEndPoints, connection.RemoveSocket());
                }
            }
            return null;
        }

        private RedisManagedNodesGroup ToNodesGroup(RedisRole role, RedisSocket[] sockets)
        {
            if (!sockets.IsEmpty())
            {
                var baseSettings = Settings;
                var nodeList = new List<RedisManagedNode>();
                foreach (var socket in sockets)
                {
                    try
                    {
                        if (socket.IsConnected())
                        {
                            var endPoint = socket.RemoteEP;
                            var settings = (RedisManagerSettings)baseSettings.Clone(endPoint.Address.ToString(), endPoint.Port);

                            var pool = new RedisManagedConnectionPool(role, Name, settings);
                            pool.ReuseSocket(socket);

                            nodeList.Add(new RedisManagedNode(role, pool, null));
                        }
                    }
                    catch (Exception)
                    {
                        socket.DisposeSocket();
                    }
                }

                if (nodeList.Count > 0)
                    return role == RedisRole.Sentinel ?
                        new RedisManagedSentinelGroup(Settings.MasterName, nodeList.ToArray(), null) :
                        new RedisManagedNodesGroup(role, nodeList.ToArray(), null);
            }
            return null;
        }

        #endregion Methods
    }
}
