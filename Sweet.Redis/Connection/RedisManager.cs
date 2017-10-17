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
        #region ContainerState

        private enum ContainerState : long
        {
            Undefined = 0,
            Creating = 1,
            Created = 2
        }

        #endregion ContainerState

        #region RedisConnectionPoolContainer

        private class RedisConnectionPoolContainer : RedisInternalDisposable
        {
            #region Field Members

            private RedisConnectionPool m_Pool;

            #endregion Field Members

            #region .Ctors

            public RedisConnectionPoolContainer(RedisRole role, RedisConnectionPool pool)
            {
                m_Pool = pool;
                Role = role == RedisRole.Undefined ? RedisRole.Master : role;
            }

            #endregion .Ctors

            #region Properties

            public RedisRole Role { get; private set; }

            public RedisConnectionPool Pool { get { return m_Pool; } }

            #endregion Properties

            #region Methods

            protected override void OnDispose(bool disposing)
            {
                base.OnDispose(disposing);

                var pool = Interlocked.Exchange(ref m_Pool, null);
                if (!ReferenceEquals(pool, null))
                    pool.Dispose();
            }

            #endregion Methods
        }

        #endregion RedisConnectionPoolContainer

        #region Field Members

        private Guid m_Id;
        private string m_Name;

        private RedisSettings m_Settings;

        private long m_ContainerStatus;
        private RedisConnectionPoolContainer[] m_Containers;

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

            Interlocked.Exchange(ref m_ContainerStatus, RedisConstants.Zero);
            var containers = Interlocked.Exchange(ref m_Containers, null);

            if (containers != null)
            {
                foreach (var container in containers)
                {
                    try { container.Dispose(); }
                    catch (Exception) { }
                }
            }
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

        private void InitContainers()
        {
            if (Interlocked.CompareExchange(ref m_ContainerStatus, (long)ContainerState.Creating, (long)ContainerState.Undefined) ==
                (long)ContainerState.Undefined)
            {
                var newContainers = CreateContainers();

                RedisConnectionPoolContainer[] containers = null;
                try
                {
                    containers = Interlocked.Exchange(ref m_Containers, newContainers);
                    Interlocked.Exchange(ref m_ContainerStatus, (long)ContainerState.Created);
                }
                catch (Exception)
                {
                    Interlocked.Exchange(ref m_ContainerStatus, (long)ContainerState.Undefined);
                    throw;
                }

                if (containers != null)
                {
                    foreach (var container in containers)
                    {
                        try { container.Dispose(); }
                        catch (Exception) { }
                    }
                }
            }
        }

        private RedisConnectionPoolContainer[] CreateContainers()
        {
            var ipSettings = SplitToIPEndPoints(m_Settings);

            var containers = new List<RedisConnectionPoolContainer>();
            foreach (var setting in ipSettings)
            {
                try
                {
                    var pool = new RedisConnectionPool(Name, setting);

                    RedisRole role;
                    using (var db = pool.GetDb())
                    {
                        role = DiscoverRole(db);
                    }
                    containers.Add(new RedisConnectionPoolContainer((role == RedisRole.Undefined) ? RedisRole.Master : role, pool));
                }
                catch (Exception)
                { }
            }
            return containers.ToArray();
        }

        private static RedisRole DiscoverRole(IRedisDb db)
        {
            var role = RedisRole.Undefined;
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

        #endregion Methods
    }
}
