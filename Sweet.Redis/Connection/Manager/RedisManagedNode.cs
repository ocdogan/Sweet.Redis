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
    internal class RedisManagedNode : RedisDisposable, IRedisHeartBeatProbe
    {
        #region Field Members

        private long m_PulseState;
        private bool m_ProbeAttached;
        private long m_PulseFailCount;

        protected object m_Seed;
        protected RedisRole m_Role;
        protected RedisEndPoint m_EndPoint;
        protected RedisManagedNodeStatus m_Status;

        private bool m_OwnsSeed;
        private RedisManagerSettings m_Settings;

        private Action<object, RedisCardioPulseStatus> m_OnPulseStateChange;

        #endregion Field Members

        #region .Ctors

        protected RedisManagedNode(RedisManagerSettings settings, RedisRole role, object seed,
                                Action<object, RedisCardioPulseStatus> onPulseStateChange, bool ownsSeed = true)
        {
            m_Seed = seed;
            m_Role = role;
            m_OwnsSeed = ownsSeed;
            m_EndPoint = RedisEndPoint.Empty;

            m_Settings = settings;
            m_OnPulseStateChange = onPulseStateChange;
        }

        #endregion .Ctors

        #region Destructors

        protected override void OnDispose(bool disposing)
        {
            m_Status |= RedisManagedNodeStatus.Disposed;
            Interlocked.Exchange(ref m_OnPulseStateChange, null);

            base.OnDispose(disposing);

            var seed = ExchangeSeedInternal(null);
            if (m_OwnsSeed && !ReferenceEquals(seed, null))
            {
                var disposable = seed as IDisposable;
                if (!ReferenceEquals(disposable, null))
                    disposable.Dispose();
                else
                {
                    var rDisposable = seed as IRedisDisposable;
                    if (!ReferenceEquals(rDisposable, null))
                        rDisposable.Dispose();
                    else
                    {
                        var iDisposable = seed as RedisInternalDisposable;
                        if (!ReferenceEquals(iDisposable, null))
                            iDisposable.Dispose();
                    }
                }
            }
        }

        #endregion Destructors

        #region Properties

        public override bool Disposed
        {
            get
            {
                return base.Disposed ||
                       m_Status.HasFlag(RedisManagedNodeStatus.Disposed);
            }
        }

        public RedisEndPoint EndPoint { get { return m_EndPoint; } }

        public virtual bool IsClosed
        {
            get { return m_Status != RedisManagedNodeStatus.Open || base.Disposed; }
            set
            {
                if (!Disposed)
                {
                    if (value)
                        m_Status |= RedisManagedNodeStatus.Closed;
                    else
                        m_Status &= ~(RedisManagedNodeStatus.Closed | RedisManagedNodeStatus.HalfClosed);
                }
            }
        }

        public virtual bool IsHalfClosed
        {
            get { return ((RedisManagedNodeStatus)m_Status).HasFlag(RedisManagedNodeStatus.HalfClosed); }
            set
            {
                if (value)
                    m_Status |= RedisManagedNodeStatus.HalfClosed;
                else
                    m_Status &= ~RedisManagedNodeStatus.HalfClosed;
            }
        }

        public bool IsOpen
        {
            get { return !IsClosed; }
            set { IsClosed = !value; }
        }

        public virtual bool IsSeedAlive
        {
            get
            {
                var seed = m_Seed;
                if (!ReferenceEquals(seed, null))
                {
                    var rDisposable = seed as IRedisDisposable;
                    if (!ReferenceEquals(rDisposable, null))
                        return !rDisposable.Disposed;

                    var iDisposable = seed as RedisInternalDisposable;
                    if (!ReferenceEquals(iDisposable, null))
                        return !iDisposable.Disposed;

                    return true;
                }
                return false;
            }
        }

        public virtual bool IsSeedDown
        {
            get
            {
                var seed = m_Seed;
                if (!ReferenceEquals(seed, null))
                {
                    var pool = seed as RedisConnectionPool;
                    if (!ReferenceEquals(pool, null))
                        return pool.IsDown;

                    var channel = seed as RedisPubSubChannel;
                    if (!ReferenceEquals(channel, null))
                        return channel.Disposed;

                    return true;
                }
                return true;
            }
        }

        public bool OwnsSeed { get { return m_OwnsSeed; } }

        public object Seed { get { return m_Seed; } }

        public virtual RedisRole Role
        {
            get { return m_Role; }
            set { m_Role = value; }
        }

        public RedisManagerSettings Settings { get { return m_Settings; } }

        long IRedisHeartBeatProbe.PulseFailCount
        {
            get { return Interlocked.Read(ref m_PulseFailCount); }
        }

        bool IRedisHeartBeatProbe.Pulsing
        {
            get { return Interlocked.Read(ref m_PulseState) != RedisConstants.Zero; }
        }

        #endregion Properties

        #region Methods

        public RedisNodeInfo GetNodeInfo()
        {
            return new RedisNodeInfo(m_EndPoint, Role);
        }

        public object ExchangeSeed(object seed)
        {
            ValidateNotDisposed();
            return ExchangeSeedInternal(seed);
        }

        protected virtual object ExchangeSeedInternal(object seed)
        {
            return Interlocked.Exchange(ref m_Seed, seed);
        }

        public virtual bool Ping()
        {
            return !IsClosed;
        }

        public void SetOnPulseStateChange(Action<object, RedisCardioPulseStatus> onPulseStateChange)
        {
            Interlocked.Exchange(ref m_OnPulseStateChange, onPulseStateChange);
        }

        protected virtual void OnPoolPulseStateChange(object sender, RedisCardioPulseStatus status)
        {
            var onPulseStateChange = m_OnPulseStateChange;
            if (onPulseStateChange != null)
                onPulseStateChange(sender, status);
        }

        protected virtual void OnPubSubPulseStateChange(object sender, RedisCardioPulseStatus status)
        {
            var onPulseStateChange = m_OnPulseStateChange;
            if (onPulseStateChange != null)
                onPulseStateChange(sender, status);
        }

        #region Pulse

        internal void AttachToCardio()
        {
            if (!Disposed && !m_ProbeAttached)
            {
                var settings = Settings;
                if (settings != null && settings.HeartBeatEnabled)
                {
                    m_ProbeAttached = true;
                    RedisCardio.Default.Attach(this, settings.HearBeatIntervalInSecs);
                }
            }
        }

        internal void DetachFromCardio()
        {
            if (m_ProbeAttached && !Disposed)
                RedisCardio.Default.Detach(this);
        }

        bool IRedisHeartBeatProbe.Pulse()
        {
            if (Interlocked.CompareExchange(ref m_PulseState, RedisConstants.One, RedisConstants.Zero) ==
                RedisConstants.Zero)
            {
                try
                {
                    if (Ping())
                    {
                        Interlocked.Add(ref m_PulseFailCount, RedisConstants.Zero);
                        return true;
                    }
                }
                catch (Exception)
                {
                    if (Interlocked.Read(ref m_PulseFailCount) < long.MaxValue)
                        Interlocked.Add(ref m_PulseFailCount, RedisConstants.One);
                }
                finally
                {
                    Interlocked.Exchange(ref m_PulseState, RedisConstants.Zero);
                }
            }
            return false;
        }

        void IRedisHeartBeatProbe.ResetPulseFailCounter()
        {
            Interlocked.Add(ref m_PulseFailCount, RedisConstants.Zero);
        }

        void IRedisHeartBeatProbe.PulseStateChanged(RedisCardioPulseStatus status)
        {
            OnPulseStateChanged(status);
        }

        protected virtual void OnPulseStateChanged(RedisCardioPulseStatus status)
        {
            var onPulseFail = m_OnPulseStateChange;
            if (onPulseFail != null)
                onPulseFail.InvokeAsync(this, status);
        }

        #endregion Pulse

        #endregion Methods
    }
}
