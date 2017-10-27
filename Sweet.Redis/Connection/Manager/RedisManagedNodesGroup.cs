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

        private RedisManagedNode[] m_Nodes;

        #endregion Field Members

        #region .Ctors

        public RedisManagedNodesGroup(RedisRole role, RedisManagedNode[] nodes)
        {
            Role = role;
            m_Nodes = nodes ?? new RedisManagedNode[0];

            if (nodes.IsEmpty())
                m_NodeIndex = -1;
        }

        #endregion .Ctors

        #region Destructors

        protected override void OnDispose(bool disposing)
        {
            base.OnDispose(disposing);
            DisposeNodes();
        }

        #endregion Destructors

        #region Properties

        public RedisRole Role { get; internal set; }

        public RedisManagedNode[] Nodes { get { return m_Nodes; } }

        #endregion Properties

        #region Methods

        public virtual RedisManagedNode[] ExchangeNodes(RedisManagedNode[] nodes)
        {
            ValidateNotDisposed();
            lock (m_SyncRoot)
            {
                var oldNodes = Interlocked.Exchange(ref m_Nodes, nodes);
                if (nodes.IsEmpty())
                    m_NodeIndex = -1;

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
                            if (node != null)
                                node.Dispose();
                        }
                        catch (Exception)
                        { }
                    }
                }
            }
        }

        public RedisManagedConnectionPool Next()
        {
            if (m_NodeIndex > -1)
            {
                ValidateNotDisposed();
                lock (m_SyncRoot)
                {
                    var nodes = m_Nodes;
                    if (nodes != null)
                    {
                        var maxLength = nodes.Length;
                        if (maxLength > 0)
                        {
                            var visitCount = 0;
                            while (visitCount++ < maxLength)
                            {
                                var index = Interlocked.Add(ref m_NodeIndex, 1);
                                if (index > maxLength - 1)
                                {
                                    index = 0;
                                    Interlocked.Exchange(ref m_NodeIndex, 0);
                                }

                                var result = nodes[index].Pool;
                                if (result.IsAlive() && !result.IsDown)
                                    return result;
                            }
                        }
                    }
                }
            }
            return null;
        }

        public bool RemoveNode(RedisManagedNode node)
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
                                Interlocked.Exchange(ref m_Nodes, null);
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

        public bool AppendNode(RedisManagedNode node)
        {
            if (node != null)
            {
                lock (m_SyncRoot)
                {
                    var nodes = m_Nodes;

                    var length = (nodes != null) ? nodes.Length : 0;
                    if (length == 0)
                    {
                        Interlocked.Exchange(ref m_Nodes, new[] { node });
                        Interlocked.Exchange(ref m_NodeIndex, 0);
                        return true;
                    }

                    if (!nodes.Contains(node))
                    {
                        var isDown = node.Disposed;
                        if (!isDown)
                        {
                            var pool = node.Pool;
                            isDown = !pool.IsAlive() || pool.IsDown;
                        }

                        var newNodes = new RedisManagedNode[length + 1];

                        var index = isDown ? 0 : 1;
                        newNodes[!isDown ? 0 : length] = node;

                        for (var i = 0; i < length; i++)
                            newNodes[index++] = nodes[i];

                        Interlocked.Exchange(ref m_Nodes, newNodes);
                        return true;
                    }
                }
            }
            return false;
        }

        #endregion Methods
    }
}
