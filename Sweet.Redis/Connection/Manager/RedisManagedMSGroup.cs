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
using System.Linq;
using System.Threading;

namespace Sweet.Redis
{
    internal class RedisManagedMSGroup : RedisDisposable
    {
        #region Field Members

        private readonly object m_SyncRoot = new object();

        private RedisManagedPoolGroup m_Masters;
        private RedisManagedPoolGroup m_Slaves;

        private RedisManagerSettings m_Settings;
        private bool m_UseSlaveAsMasterWhenNeeded;

        private Action<object, RedisCardioPulseStatus> m_OnPulseStateChange;

        #endregion Field Members

        #region .Ctors

        public RedisManagedMSGroup(RedisManagerSettings settings,
                                   RedisManagedPoolGroup masters, RedisManagedPoolGroup slaves = null,
                                   Action<object, RedisCardioPulseStatus> onPulseStateChange = null)
        {
            m_OnPulseStateChange = onPulseStateChange;
            m_Settings = settings;
            m_UseSlaveAsMasterWhenNeeded = settings.UseSlaveAsMasterIfNoMasterFound;

            ExchangeSlavesInternal(slaves ?? new RedisManagedPoolGroup(settings, RedisRole.Slave, null, null));
            ExchangeMastersInternal(masters ?? new RedisManagedPoolGroup(settings, RedisRole.Master, null, null));
        }

        #endregion .Ctors

        #region Destructors

        protected override void OnDispose(bool disposing)
        {
            base.OnDispose(disposing);

            Interlocked.Exchange(ref m_Settings, null);
            Interlocked.Exchange(ref m_OnPulseStateChange, null);

            var slaves = ExchangeSlavesInternal(null);
            var masters = ExchangeMastersInternal(null);

            if (slaves != null) slaves.Dispose();
            if (masters != null) masters.Dispose();
        }

        #endregion Destructors

        #region Properties

        public RedisManagedPoolGroup Masters { get { return m_Masters; } }

        public RedisManagerSettings Settings { get { return m_Settings; } }

        public RedisManagedPoolGroup Slaves { get { return m_Slaves; } }

        #endregion Properties

        #region Methods

        internal void SetOnPulseStateChange(Action<object, RedisCardioPulseStatus> onPulseStateChange)
        {
            Interlocked.Exchange(ref m_OnPulseStateChange, onPulseStateChange);
        }

        protected virtual void OnPulseStateChange(object sender, RedisCardioPulseStatus status)
        {
            var onPulseStateChange = m_OnPulseStateChange;
            if (onPulseStateChange != null)
                onPulseStateChange(sender, status);
        }

        public RedisManagedPoolGroup ExchangeMasters(RedisManagedPoolGroup masters)
        {
            ValidateNotDisposed();
            return ExchangeMastersInternal(masters);
        }

        private RedisManagedPoolGroup ExchangeMastersInternal(RedisManagedPoolGroup masters)
        {
            lock (m_SyncRoot)
            {
                var oldGroup = Interlocked.Exchange(ref m_Masters, masters);
                if (oldGroup != null)
                    oldGroup.SetOnPulseStateChange(null);

                if (masters != null)
                    masters.SetOnPulseStateChange(OnPulseStateChange);

                return oldGroup;
            }
        }

        public RedisManagedPoolGroup ExchangeSlaves(RedisManagedPoolGroup slaves)
        {
            ValidateNotDisposed();
            return ExchangeSlavesInternal(slaves);
        }

        private RedisManagedPoolGroup ExchangeSlavesInternal(RedisManagedPoolGroup slaves)
        {
            lock (m_SyncRoot)
            {
                var oldGroup = Interlocked.Exchange(ref m_Slaves, slaves);
                if (oldGroup != null)
                    oldGroup.SetOnPulseStateChange(null);

                if (slaves != null)
                    slaves.SetOnPulseStateChange(OnPulseStateChange);

                return oldGroup;
            }
        }

        public RedisManagedPoolGroup SelectGroup(bool readOnly)
        {
            if (!Disposed)
            {
                lock (m_SyncRoot)
                {
                    var result = (RedisManagedPoolGroup)null;
                    if (!readOnly)
                    {
                        result = m_Masters;

                        var settings = m_Settings;
                        if (m_UseSlaveAsMasterWhenNeeded)
                        {
                            if (!result.IsAlive())
                                result = m_Slaves;
                            else
                            {
                                var nodes = result.Nodes;
                                if (nodes.IsEmpty() ||
                                    !nodes.Any(n => n.IsAlive() && n.IsOpen))
                                    result = m_Slaves;
                            }
                        }
                    }
                    else
                    {
                        result = m_Slaves;
                        if (!result.IsAlive())
                            result = m_Masters;
                        else
                        {
                            var nodes = result.Nodes;
                            if (nodes.IsEmpty() ||
                                !nodes.Any(n => n.IsAlive() && n.IsOpen))
                                result = m_Masters;
                        }
                    }
                    return result;
                }
            }
            return null;
        }

        public bool ChangeGroup(RedisManagedNode node)
        {
            if (!Disposed && node.IsAlive())
            {
                lock (m_SyncRoot)
                {
                    var slaves = m_Slaves;
                    var masters = m_Masters;

                    if (slaves.IsAlive() && masters.IsAlive())
                    {
                        if (masters.ContainsNode(node))
                        {
                            node.Role = RedisRole.Slave;
                            if (!masters.RemoveNode(node))
                                node.IsClosed = true;
                            else
                            {
                                if (slaves.AppendNode(node))
                                    return true;

                                node.Dispose();
                            }
                        }
                        else if (slaves.ContainsNode(node))
                        {
                            node.Role = RedisRole.Slave;
                            if (!slaves.RemoveNode(node))
                                node.IsClosed = true;
                            else
                            {
                                var oldMasters = masters.Nodes;
                                if (!masters.AppendNode(node))
                                    node.Dispose();
                                else
                                {
                                    try
                                    {
                                        node.Role = RedisRole.Master;

                                        if (!oldMasters.IsEmpty())
                                        {
                                            var length = oldMasters.Length;
                                            var nodes = new RedisManagedNode[length];

                                            Array.Copy(oldMasters, nodes, length);

                                            foreach (var oldNode in nodes)
                                            {
                                                if (!(ReferenceEquals(oldNode, null) ||
                                                      ReferenceEquals(oldNode, node)))
                                                {
                                                    oldNode.IsClosed = true;
                                                    oldNode.Role = RedisRole.Slave;

                                                    if (masters.RemoveNode(oldNode) && oldNode.IsAlive())
                                                        slaves.AppendNode(node);
                                                }
                                            }
                                        }
                                    }
                                    catch (Exception)
                                    { }

                                    return true;
                                }
                            }
                        }
                    }
                }
            }
            return false;
        }

        public RedisEndPoint[] GetEndPoints()
        {
            if (!Disposed)
            {
                lock (m_SyncRoot)
                {
                    var slaves = m_Slaves;
                    var masters = m_Masters;

                    if (slaves != null || masters != null)
                    {
                        var masterEndPoints = masters.GetEndPoints();
                        if (masterEndPoints.IsEmpty())
                            return slaves.GetEndPoints();

                        var slaveEndPoints = slaves.GetEndPoints();
                        if (slaveEndPoints.IsEmpty())
                            return masters.GetEndPoints();

                        return masterEndPoints
                            .Union(slaveEndPoints)
                            .Distinct()
                            .ToArray();
                    }
                }
            }
            return null;
        }

        public void PromoteToMaster(RedisEndPoint newEndPoint, RedisEndPoint oldEndPoint)
        {
            if (!Disposed && !newEndPoint.IsEmpty())
            {
                lock (m_SyncRoot)
                {
                    SetMasterIsDown(oldEndPoint);

                    var masters = m_Masters;
                    if (masters.IsAlive())
                    {
                        var slaves = m_Slaves;

                        var node = masters.FindNodeOf(newEndPoint);
                        if (node.IsAlive())
                        {
                            node.Ping();
                            if (node.IsClosed)
                                node.IsClosed = false;

                            if (slaves.IsAlive())
                                slaves.RemoveNode(node);
                            return;
                        }

                        if (slaves.IsAlive())
                        {
                            var slaveNode = slaves.FindNodeOf(newEndPoint);
                            if (slaveNode.IsAlive())
                            {
                                if (ChangeGroup(slaveNode))
                                {
                                    if (slaveNode.IsClosed)
                                        slaveNode.IsClosed = false;
                                }
                            }
                        }
                    }
                }
            }
        }

        public bool SetMasterIsDown(RedisEndPoint endPoint)
        {
            if (!Disposed && !endPoint.IsEmpty())
            {
                lock (m_SyncRoot)
                {
                    var masters = m_Masters;
                    if (masters.IsAlive())
                    {
                        var masterNodes = masters.Nodes;
                        if (masterNodes != null)
                        {
                            var masterNode = masterNodes.FirstOrDefault(n => n.IsAlive() && n.EndPoint == endPoint);
                            if (masterNode.IsAlive())
                            {
                                if (!masterNode.IsClosed)
                                    masterNode.IsClosed = true;

                                ChangeGroup(masterNode);
                                return true;
                            }
                        }
                    }
                }
            }
            return false;
        }

        #endregion Methods
    }
}
