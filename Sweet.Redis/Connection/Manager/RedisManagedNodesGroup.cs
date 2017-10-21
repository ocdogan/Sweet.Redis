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
            m_Nodes = nodes;

            if (nodes == null || nodes.Length == 0)
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

        public RedisRole Role { get; private set; }

        internal RedisManagedNode[] Nodes
        {
            get
            {
                var nodes = m_Nodes;
                if (nodes != null)
                {
                    var result = new RedisManagedNode[nodes.Length];
                    Array.Copy(nodes, result, result.Length);
                    return result;
                }
                return null;
            }
        }

        #endregion Properties

        #region Methods

        private void DisposeNodes()
        {
            var nodes = Interlocked.Exchange(ref m_Nodes, null);
            if (nodes != null)
            {
                Interlocked.Exchange(ref m_NodeIndex, -1);
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

        public RedisConnectionPool Next()
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
                            var index = Interlocked.Add(ref m_NodeIndex, 1);
                            if (index > maxLength - 1)
                            {
                                index = 0;
                                Interlocked.Exchange(ref m_NodeIndex, 0);
                            }
                            return nodes[index].Pool;
                        }
                    }
                }
            }
            return null;
        }

        #endregion Methods
    }
}
