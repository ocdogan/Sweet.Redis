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
    internal class RedisManagedMSGroup : RedisDisposable
    {
        #region Field Members

        private readonly object m_SyncRoot = new object();

        private RedisManagedNodesGroup m_Masters;
        private RedisManagedNodesGroup m_Slaves;

        private Action<object, RedisCardioPulseStatus> m_OnPulseStateChange;

        #endregion Field Members

        #region .Ctors

        public RedisManagedMSGroup(RedisManagedNodesGroup masters, RedisManagedNodesGroup slaves = null,
                                   Action<object, RedisCardioPulseStatus> onPulseStateChange = null)
        {
            m_OnPulseStateChange = onPulseStateChange;

            ExchangeSlavesInternal(slaves ?? new RedisManagedNodesGroup(RedisRole.Slave, null, null));
            ExchangeMastersInternal(masters ?? new RedisManagedNodesGroup(RedisRole.Master, null, null));
        }

        #endregion .Ctors

        #region Destructors

        protected override void OnDispose(bool disposing)
        {
            base.OnDispose(disposing);

            Interlocked.Exchange(ref m_OnPulseStateChange, null);

            var slaves = ExchangeSlavesInternal(null);
            var masters = ExchangeMastersInternal(null);

            if (slaves != null) slaves.Dispose();
            if (masters != null) masters.Dispose();
        }

        #endregion Destructors

        #region Properties

        public RedisManagedNodesGroup Masters { get { return m_Masters; } }

        public RedisManagedNodesGroup Slaves { get { return m_Slaves; } }

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

        public RedisManagedNodesGroup ExchangeMasters(RedisManagedNodesGroup masters)
        {
            ValidateNotDisposed();
            return ExchangeMastersInternal(masters);
        }

        private RedisManagedNodesGroup ExchangeMastersInternal(RedisManagedNodesGroup masters)
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

        public RedisManagedNodesGroup ExchangeSlaves(RedisManagedNodesGroup slaves)
        {
            ValidateNotDisposed();
            return ExchangeSlavesInternal(slaves);
        }

        private RedisManagedNodesGroup ExchangeSlavesInternal(RedisManagedNodesGroup slaves)
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

        public void ChangeGroup(RedisManagedNode node)
        {
            if (node.IsAlive())
            {
                lock (m_SyncRoot)
                {
                    var slaves = m_Slaves;
                    if (slaves.IsAlive())
                    {
                        var masters = m_Masters;
                        if (masters.IsAlive())
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
