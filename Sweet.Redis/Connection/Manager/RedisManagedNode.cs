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
    internal class RedisManagedNode : RedisDisposable
    {
        #region Field Members

        private Action<object, bool> m_OnPulseStateChange;

        private bool m_OwnsPool;
        private RedisRole m_Role;
        private RedisEndPoint m_EndPoint;
        private RedisManagedConnectionPool m_Pool;

        #endregion Field Members

        #region .Ctors

        public RedisManagedNode(RedisRole role, RedisManagedConnectionPool pool,
                                Action<object, bool> onPulseStateChange, bool ownsPool = true)
        {
            m_Pool = pool;
            Role = role;
            m_OwnsPool = ownsPool;
            m_EndPoint = (pool != null) ? pool.EndPoint : RedisEndPoint.Empty;

            m_OnPulseStateChange = onPulseStateChange;

            if (pool != null)
            {
                pool.PoolPulseStateChanged += OnPoolPulseStateChange;
                pool.PubSubPulseStateChanged += OnPubSubPulseStateChange;
            }
        }

        #endregion .Ctors

        #region Destructors

        protected override void OnDispose(bool disposing)
        {
            Interlocked.Exchange(ref m_OnPulseStateChange, null);

            base.OnDispose(disposing);

            var pool = ExchangePoolInternal(null);
            if (m_OwnsPool && pool != null)
                pool.Dispose();
        }

        #endregion Destructors

        #region Properties

        public RedisEndPoint EndPoint { get { return m_EndPoint; } }

        public bool IsDown
        {
            get
            {
                var pool = m_Pool;
                return !pool.IsAlive() || pool.SDown || pool.ODown;
            }
        }

        public bool ODown
        {
            get
            {
                var pool = m_Pool;
                return (pool == null) || pool.ODown;
            }
            set
            {
                var pool = m_Pool;
                if (pool != null)
                    pool.ODown = value;
            }
        }

        public bool OwnsPool { get { return m_OwnsPool; } }

        public RedisManagedConnectionPool Pool { get { return m_Pool; } }

        public RedisRole Role
        {
            get { return m_Role; }
            internal set
            {
                m_Role = value;

                var pool = m_Pool;
                if (pool != null)
                    pool.Role = value;
            }
        }

        public bool SDown
        {
            get
            {
                var pool = m_Pool;
                return (pool == null) || pool.SDown;
            }
            set
            {
                var pool = m_Pool;
                if (pool != null)
                    pool.SDown = value;
            }
        }

        #endregion Properties

        #region Methods

        public RedisManagedNodeInfo GetNodeInfo()
        {
            return new RedisManagedNodeInfo(m_EndPoint, Role);
        }

        public RedisConnectionPool ExchangePool(RedisManagedConnectionPool pool)
        {
            ValidateNotDisposed();
            return ExchangePoolInternal(pool);
        }

        private RedisConnectionPool ExchangePoolInternal(RedisManagedConnectionPool pool)
        {
            var oldPool = Interlocked.Exchange(ref m_Pool, pool);
            if (!Disposed)
                m_EndPoint = pool.IsAlive() ? pool.EndPoint : RedisEndPoint.Empty;

            if (pool.IsAlive())
            {
                pool.Role = m_Role;
                pool.PoolPulseStateChanged += OnPoolPulseStateChange;
                pool.PubSubPulseStateChanged += OnPubSubPulseStateChange;
            }

            if (oldPool != null)
            {
                oldPool.PoolPulseStateChanged -= OnPoolPulseStateChange;
                oldPool.PubSubPulseStateChanged -= OnPubSubPulseStateChange;
            }

            return oldPool;
        }

        internal void SetOnPulseStateChange(Action<object, bool> onPulseStateChange)
        {
            Interlocked.Exchange(ref m_OnPulseStateChange, onPulseStateChange);
        }

        protected virtual void OnPoolPulseStateChange(object sender, bool alive)
        {
            var onPulseStateChange = m_OnPulseStateChange;
            if (onPulseStateChange != null)
                onPulseStateChange(sender, alive);
        }

        protected virtual void OnPubSubPulseStateChange(object sender, bool alive)
        {
            var onPulseStateChange = m_OnPulseStateChange;
            if (onPulseStateChange != null)
                onPulseStateChange(sender, alive);
        }

        #endregion Methods
    }
}
