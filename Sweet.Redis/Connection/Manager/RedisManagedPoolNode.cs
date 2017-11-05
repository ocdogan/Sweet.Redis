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
    internal class RedisManagedPoolNode : RedisManagedNode, IRedisManagedNode
    {
        #region .Ctors

        public RedisManagedPoolNode(RedisManagerSettings settings, RedisRole role, RedisManagedPool pool,
                                    Action<object, RedisCardioPulseStatus> onPulseStateChange, bool ownsSeed = true)
            : base(settings, role, pool, onPulseStateChange, ownsSeed)
        {
            m_EndPoint = (pool != null) ? pool.EndPoint : RedisEndPoint.Empty;

            if (pool != null)
            {
                pool.PoolPulseStateChanged += OnPoolPulseStateChange;
                pool.PubSubPulseStateChanged += OnPubSubPulseStateChange;
            }
        }

        #endregion .Ctors

        #region Properties

        public override bool Disposed
        {
            get
            {
                var result = base.Disposed;
                if (!result)
                {
                    var pool = Pool;
                    return !pool.IsAlive();
                }
                return result;
            }
        }

        public RedisConnectionPool Pool
        {
            get { return (RedisConnectionPool)m_Seed; }
        }

        public override bool IsClosed
        {
            get
            {
                if (!Disposed)
                {
                    var closed = base.IsClosed;
                    if (!closed)
                    {
                        var pool = (RedisConnectionPool)m_Seed;
                        if (!ReferenceEquals(pool, null))
                            closed = pool.IsDown;
                    }
                    return closed;
                }
                return true;
            }
            set
            {
                if (!Disposed)
                {
                    base.IsClosed = value;

                    var pool = (RedisConnectionPool)m_Seed;
                    if (!ReferenceEquals(pool, null))
                        pool.IsDown = value;
                }
            }
        }

        public override bool IsHalfClosed
        {
            get
            {
                if (!Disposed)
                {
                    var closed = base.IsHalfClosed;
                    if (!closed)
                    {
                        var pool = (RedisManagedPool)m_Seed;
                        if (!ReferenceEquals(pool, null))
                            closed = pool.SDown;
                    }
                    return closed;
                }
                return true;
            }
            set
            {
                if (!Disposed)
                {
                    base.IsHalfClosed = value;

                    var pool = (RedisManagedPool)m_Seed;
                    if (!ReferenceEquals(pool, null))
                        pool.SDown = value;
                }
            }
        }

        public override RedisRole Role
        {
            get { return base.Role; }
            set
            {
                base.Role = value;

                var pool = (RedisManagedPool)m_Seed;
                if (!ReferenceEquals(pool, null))
                    pool.Role = value;
            }
        }

        public RedisManagedNodeStatus Status
        {
            get { return m_Status; }
            set
            {
                if (!m_Status.HasFlag(RedisManagedNodeStatus.Disposed))
                    m_Status = value;
            }
        }

        #endregion Properties

        #region Methods

        protected override object ExchangeSeedInternal(object seed)
        {
            if (!(ReferenceEquals(seed, null) || seed is RedisManagedPool))
                throw new RedisException("Invalid seed type");

            var pool = (RedisManagedPool)seed;

            var oldPool = (RedisManagedPool)Interlocked.Exchange(ref m_Seed, pool);
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

        public override bool Ping()
        {
            var pool = (RedisConnectionPool)m_Seed;
            if (pool.IsAlive())
            {
                try
                {
                    return pool.Ping(pool.IsDown);
                }
                catch (Exception)
                { }
            }
            return false;
        }

        #endregion Methods
    }
}
