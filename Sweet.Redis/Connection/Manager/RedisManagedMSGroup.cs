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

using System.Threading;

namespace Sweet.Redis
{
    internal class RedisManagedMSGroup : RedisDisposable
    {
        #region Field Members

        private readonly object m_SyncRoot = new object();

        private RedisManagedNodesGroup m_Masters;
        private RedisManagedNodesGroup m_Slaves;

        #endregion Field Members

        #region .Ctors

        public RedisManagedMSGroup(RedisManagedNodesGroup masters, RedisManagedNodesGroup slaves = null)
        {
            m_Masters = masters;
            m_Slaves = slaves;
        }

        #endregion .Ctors

        #region Destructors

        protected override void OnDispose(bool disposing)
        {
            base.OnDispose(disposing);

            var masters = Interlocked.Exchange(ref m_Masters, null);
            var slaves = Interlocked.Exchange(ref m_Slaves, null);

            if (masters != null) masters.Dispose();
            if (slaves != null) slaves.Dispose();
        }

        #endregion Destructors

        #region Properties

        public RedisManagedNodesGroup Masters { get { return m_Masters; } }

        public RedisManagedNodesGroup Slaves { get { return m_Slaves; } }

        #endregion Properties

        #region Methods

        public RedisManagedNodesGroup ExchangeMasters(RedisManagedNodesGroup masters)
        {
            ValidateNotDisposed();
            lock (m_SyncRoot)
            {
                return Interlocked.Exchange(ref m_Masters, masters);
            }
        }

        public RedisManagedNodesGroup ExchangeSlaves(RedisManagedNodesGroup slaves)
        {
            ValidateNotDisposed();
            lock (m_SyncRoot)
            {
                return Interlocked.Exchange(ref m_Slaves, slaves);
            }
        }

        public void ChangeGroup(RedisManagedNode node)
        {
            if (node != null && !node.Disposed)
            {
                lock (m_SyncRoot)
                {
                    var slaves = m_Slaves;
                    if (slaves != null && slaves.Disposed)
                    {
                        var masters = m_Masters;
                        if (masters != null && !masters.Disposed)
                        {
                            if (slaves.RemoveNode(node))
                            {
                                if (masters.AppendNode(node))
                                    node.Role = RedisRole.Master;
                            }
                            else if (masters.RemoveNode(node))
                            {
                                if (slaves.AppendNode(node))
                                    node.Role = RedisRole.Slave;
                            }
                        }
                    }
                }
            }
        }

        #endregion Methods
    }
}
