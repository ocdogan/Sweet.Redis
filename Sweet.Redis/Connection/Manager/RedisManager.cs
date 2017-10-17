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
    public class RedisManager : RedisDisposable, IRedisNamedObject, IRedisIdentifiedObject
    {
        #region InitializationState

        private enum InitializationState : long
        {
            Undefined = 0,
            Initializing = 1,
            Initialized = 2
        }

        #endregion InitializationState

        #region RedisClusterNode

        private class RedisClusterNode
        {
            #region .Ctors

            public RedisClusterNode(RedisConnectionPool pool, RedisRole role)
            {
                Pool = pool;
                Role = role;
            }

            #endregion .Ctors

            #region Properties

            public RedisConnectionPool Pool { get; private set; }

            public RedisRole Role { get; private set; }

            #endregion Properties
        }

        #endregion RedisClusterNode

        #region Field Members

        private Guid m_Id;
        private string m_Name;

        private RedisSettings m_Settings;
        private RedisClusterGroup[] m_Groups;

        private long m_InitializationState;
        private readonly object m_SyncRoot = new object();

        #endregion Field Members

        #region .Ctors

        public RedisManager(string name, RedisSettings settings = null)
        {
            if (settings == null)
                throw new RedisFatalException(new ArgumentNullException("settings"));

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

            var groups = Interlocked.Exchange(ref m_Groups, null);
            DisposeGroups(groups);
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

        public RedisSettings Settings
        {
            get { return m_Settings; }
        }

        #endregion Properties

        #region Methods

        private static void DisposeGroups(RedisClusterGroup[] groups)
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
            ValidateNotDisposed();

            if (Interlocked.CompareExchange(ref m_InitializationState, (long)InitializationState.Initializing, (long)InitializationState.Undefined) !=
                (long)InitializationState.Initialized)
            {
                lock (m_SyncRoot)
                {
                    if (Interlocked.Read(ref m_InitializationState) != (long)InitializationState.Initialized)
                    {
                        var newGroups = CreateGroups();

                        RedisClusterGroup[] oldGroups = null;
                        try
                        {
                            oldGroups = Interlocked.Exchange(ref m_Groups, newGroups ?? new RedisClusterGroup[0]);
                            Interlocked.Exchange(ref m_InitializationState, (long)InitializationState.Initialized);
                        }
                        catch (Exception)
                        {
                            Interlocked.Exchange(ref m_InitializationState, (long)InitializationState.Undefined);
                            throw;
                        }

                        DisposeGroups(oldGroups);
                    }
                }
            }
        }

        private RedisClusterGroup[] CreateGroups()
        {
            var ipEPSettings = SplitToIPEndPoints(m_Settings);
            if (ipEPSettings.Length == 1)
            {
                var node = CreateNode(Name, ipEPSettings[0]);
                if (node != null)
                    return new[] { new RedisClusterGroup(node.Role, new[] { node.Pool }) };
            }
            else if (ipEPSettings.Length > 1)
            {
                var nodes = new List<RedisClusterNode>();
                foreach (var setting in ipEPSettings)
                {
                    var node = CreateNode(Name, setting);
                    if (node != null)
                        nodes.Add(node);
                }

                if (nodes.Count > 0)
                    return nodes.GroupBy(
                        node => node.Role,
                        node => node.Pool,
                        (role, group) => new RedisClusterGroup(role, group.ToArray())).ToArray();
            }
            return null;
        }

        private static RedisClusterNode CreateNode(string name, RedisSettings settings)
        {
            try
            {
                var pool = new RedisConnectionPool(name, settings);

                var role = DiscoverNodeRole(pool);
                if (role != RedisRole.Undefined)
                    return new RedisClusterNode(pool, role);

                pool.Dispose();
            }
            catch (Exception)
            { }
            return null;
        }

        private static RedisRole DiscoverNodeRole(RedisConnectionPool pool)
        {
            var role = RedisRole.Undefined;
            using (var db = pool.GetDb())
            {
                try
                {
                    var rawInfo = db.Server.Role();
                    if (!ReferenceEquals(rawInfo, null))
                    {
                        var roleInfo = rawInfo.Value;
                        if (!ReferenceEquals(roleInfo, null))
                            role = roleInfo.Role;
                    }
                }
                catch (Exception)
                { }

                if (role == RedisRole.Undefined)
                {
                    try
                    {
                        var rawInfo = db.Server.Info();
                        if (!ReferenceEquals(rawInfo, null))
                        {
                            var serverInfo = rawInfo.Value;
                            if (!ReferenceEquals(serverInfo, null))
                            {
                                var repInfo = serverInfo.Replication;
                                if (!ReferenceEquals(repInfo, null))
                                {
                                    var roleStr = (repInfo.Role ?? String.Empty).ToLower();
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
                            }
                        }
                    }
                    catch (Exception)
                    { }
                }
            }
            return role;
        }

        private static RedisSettings[] SplitToIPEndPoints(RedisSettings settings)
        {
            var endPoints = settings.EndPoints;
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

                return ipList
                    .Select(ep => (RedisSettings)settings.Clone(ep.Address.ToString(), ep.Port))
                    .ToArray();
            }
            return null;
        }

        #endregion Initialization Methods

        #endregion Methods
    }
}
