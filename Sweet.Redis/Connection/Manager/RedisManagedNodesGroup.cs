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
    internal class RedisManagedNodesGroup : RedisDisposable
    {
        #region Field Members

        private int m_NodeIndex;
        private readonly object m_SyncRoot = new object();

        private RedisManagerSettings m_Settings;

        private RedisManagedNode[] m_Nodes;
        private Action<object, RedisCardioPulseStatus> m_OnPulseStateChange;

        #endregion Field Members

        #region .Ctors

        protected RedisManagedNodesGroup(RedisManagerSettings settings, RedisRole role,
                   RedisManagedNode[] nodes, Action<object, RedisCardioPulseStatus> onPulseStateChange)
        {
            Role = role;
            m_OnPulseStateChange = onPulseStateChange;
            m_Nodes = nodes ?? new RedisManagedNode[0];
            m_Settings = settings;

            if (nodes.IsEmpty())
                m_NodeIndex = -1;
            else
            {
                foreach (var node in nodes)
                    if (node != null)
                        node.SetOnPulseStateChange(OnPulseStateChange);
            }
        }

        #endregion .Ctors

        #region Destructors

        protected override void OnDispose(bool disposing)
        {
            Interlocked.Exchange(ref m_Settings, null);
            Interlocked.Exchange(ref m_OnPulseStateChange, null);

            base.OnDispose(disposing);
            DisposeNodes();
        }

        #endregion Destructors

        #region Properties

        public RedisManagedNode[] Nodes { get { return m_Nodes; } }

        public RedisRole Role { get; internal set; }

        public RedisManagerSettings Settings { get { return m_Settings; } }

        #endregion Properties

        #region Methods

        internal void SetOnPulseStateChange(Action<object, RedisCardioPulseStatus> onPulseStateChange)
        {
            Interlocked.Exchange(ref m_OnPulseStateChange, onPulseStateChange);
        }

        private void OnPulseStateChange(object sender, RedisCardioPulseStatus status)
        {
            var onPulseStateChange = m_OnPulseStateChange;
            if (onPulseStateChange != null)
                onPulseStateChange(sender, status);
        }

        public virtual RedisManagedNode[] ExchangeNodes(RedisManagedNode[] nodes)
        {
            ValidateNotDisposed();
            return ExchangeNodesInternal(nodes);
        }

        private RedisManagedNode[] ExchangeNodesInternal(RedisManagedNode[] nodes)
        {
            lock (m_SyncRoot)
            {
                var oldNodes = Interlocked.Exchange(ref m_Nodes, nodes);
                if (nodes.IsEmpty())
                    m_NodeIndex = -1;
                else
                {
                    foreach (var node in nodes)
                        if (node != null)
                            node.SetOnPulseStateChange(OnPulseStateChange);
                }

                if (!oldNodes.IsEmpty())
                {
                    foreach (var node in oldNodes)
                        if (node != null)
                            node.SetOnPulseStateChange(null);
                }

                return oldNodes;
            }
        }

        private void DisposeNodes()
        {
            Interlocked.Exchange(ref m_NodeIndex, -1);

            var nodes = Interlocked.Exchange(ref m_Nodes, new RedisManagedNode[0]);
            if (nodes != null)
            {
                lock (m_SyncRoot)
                {
                    foreach (var node in nodes)
                    {
                        try
                        {
                            if (node.IsAlive())
                                node.Dispose();
                        }
                        catch (Exception)
                        { }
                    }
                }
            }
        }

        protected virtual RedisManagedNode NextNode()
        {
            if (m_NodeIndex > -1)
            {
                ValidateNotDisposed();

                var nodes = m_Nodes;
                if (nodes != null)
                {
                    lock (m_SyncRoot)
                    {
                        nodes = m_Nodes;
                        if (nodes != null)
                            nodes = (RedisManagedNode[])nodes.Clone();
                    }
                }

                if (nodes != null)
                {
                    var maxLength = nodes.Length;
                    if (maxLength > 0)
                    {
                        var role = Role;
                        var visitCount = 0;
                        while (visitCount++ < maxLength)
                        {
                            var index = Interlocked.Add(ref m_NodeIndex, 1);
                            if (index > maxLength - 1)
                            {
                                index = 0;
                                Interlocked.Exchange(ref m_NodeIndex, 0);
                            }

                            var node = nodes[index];
                            if (node.IsAlive() && node.Role == role && !node.IsClosed)
                                return node;
                        }
                    }
                }
            }
            return null;
        }

        public virtual bool RemoveNode(RedisManagedNode node)
        {
            if (node != null)
            {
                lock (m_SyncRoot)
                {
                    var nodes = m_Nodes;
                    if (nodes != null)
                    {
                        var length = nodes.Length;
                        if (length > 0 && nodes.Contains(node))
                        {
                            if (length == 1)
                            {
                                Interlocked.Exchange(ref m_NodeIndex, -1);
                                Interlocked.Exchange(ref m_Nodes, new RedisManagedNode[0]);
                                return true;
                            }

                            var newNodes = new RedisManagedNode[length - 1];

                            var index = 0;
                            for (var i = 0; i < length; i++)
                            {
                                var groupNode = nodes[i];
                                if (groupNode == node)
                                    continue;

                                newNodes[index++] = groupNode;
                            }

                            Interlocked.Exchange(ref m_Nodes, newNodes);
                            return true;
                        }
                    }
                }
            }
            return false;
        }

        public bool ContainsNode(RedisManagedNode node)
        {
            if (node != null)
            {
                lock (m_SyncRoot)
                {
                    var nodes = m_Nodes;

                    var length = (nodes != null) ? nodes.Length : 0;
                    if (length == 0)
                        return false;

                    return nodes.Contains(node);
                }
            }
            return false;
        }

        public virtual bool AppendNode(RedisManagedNode node)
        {
            if (node != null)
            {
                lock (m_SyncRoot)
                {
                    var nodes = m_Nodes;

                    var length = (nodes != null) ? nodes.Length : 0;
                    if (length == 0)
                    {
                        node.Role = Role;
                        Interlocked.Exchange(ref m_Nodes, new[] { node });
                        Interlocked.Exchange(ref m_NodeIndex, 0);
                        return true;
                    }

                    if (!nodes.Contains(node))
                    {
                        var isDown = node.IsClosed;
                        var newNodes = new RedisManagedNode[length + 1];

                        var index = isDown ? 0 : 1;
                        newNodes[!isDown ? 0 : length] = node;

                        for (var i = 0; i < length; i++)
                            newNodes[index++] = nodes[i];

                        node.Role = Role;
                        Interlocked.Exchange(ref m_Nodes, newNodes);
                        return true;
                    }
                }
            }
            return false;
        }

        public void AttachToCardio()
        {
            if (!Disposed && Settings.HeartBeatEnabled)
            {
                var nodes = m_Nodes;
                if (nodes != null)
                {
                    foreach (var node in nodes)
                        if (node.IsAlive())
                            node.AttachToCardio();
                }
            }
        }

        public void DetachFromCardio()
        {
            if (!Disposed)
            {
                var nodes = m_Nodes;
                if (nodes != null)
                {
                    foreach (var node in nodes)
                        if (node.IsAlive())
                            node.DetachFromCardio();
                }
            }
        }

        public RedisPoolSettings FindValidSettings()
        {
            if (!Disposed)
            {
                var nodes = m_Nodes;
                if (nodes != null)
                {
                    foreach (var node in nodes)
                    {
                        try
                        {
                            if (node.IsAlive())
                            {
                                if (node.Ping())
                                    return node.Settings;
                            }
                        }
                        catch (Exception)
                        { }
                    }
                }
            }
            return null;
        }

        public RedisManagedNode FindNodeOf(RedisEndPoint endPoint)
        {
            if (!(Disposed || endPoint.IsEmpty()))
            {
                var nodes = m_Nodes;
                if (nodes != null)
                    return nodes.FirstOrDefault(n => !ReferenceEquals(n, null) && (n.EndPoint == endPoint));
            }
            return null;
        }

        public RedisEndPoint[] GetEndPoints()
        {
            var nodes = m_Nodes;
            if (!nodes.IsEmpty())
                return nodes.GroupBy(n => n.EndPoint).Select(g => g.Key).Where(ep => !ep.IsEmpty()).ToArray();
            return null;
        }

        #endregion Methods
    }
}
