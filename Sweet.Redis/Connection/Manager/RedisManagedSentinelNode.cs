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
    internal class RedisManagedSentinelNode : RedisManagedNode
    {
        #region .Ctors

        public RedisManagedSentinelNode(RedisManagerSettings settings, RedisManagedSentinelListener listener,
                                    Action<object, RedisCardioPulseStatus> onPulseStateChange, bool ownsSeed = true)
            : base(settings, RedisRole.Sentinel, listener, onPulseStateChange, ownsSeed)
        {
            m_EndPoint = (listener != null) ? listener.EndPoint : RedisEndPoint.Empty;
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
                    var listener = Listener;
                    return !listener.IsAlive();
                }
                return result;
            }
        }

        public RedisManagedSentinelListener Listener
        {
            get { return (RedisManagedSentinelListener)m_Seed; }
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
                        var listener = (RedisManagedSentinelListener)m_Seed;
                        if (!ReferenceEquals(listener, null))
                            closed = listener.IsDown;
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

                    var listener = (RedisManagedSentinelListener)m_Seed;
                    if (!ReferenceEquals(listener, null))
                        listener.IsDown = value;
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
                        var listener = (RedisManagedSentinelListener)m_Seed;
                        if (!ReferenceEquals(listener, null))
                            closed = listener.SDown;
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

                    var listener = (RedisManagedSentinelListener)m_Seed;
                    if (!ReferenceEquals(listener, null))
                        listener.SDown = value;
                }
            }
        }

        public override RedisRole Role
        {
            get { return RedisRole.Sentinel; }
            set
            {
                if (value != RedisRole.Sentinel)
                    throw new RedisException("Can not set a role different than Sentinel");
            }
        }

        #endregion Properties

        #region Methods

        protected override object ExchangeSeedInternal(object seed)
        {
            if (!(ReferenceEquals(seed, null) || seed is RedisManagedSentinelListener))
                throw new RedisException("Invalid seed type");

            var listener = (RedisManagedSentinelListener)seed;

            var oldListener = (RedisManagedSentinelListener)Interlocked.Exchange(ref m_Seed, listener);
            if (!Disposed)
                m_EndPoint = listener.IsAlive() ? listener.EndPoint : RedisEndPoint.Empty;

            if (oldListener != null)
                oldListener.Dispose();

            return oldListener;
        }

        public override bool Ping()
        {
            var listener = (RedisManagedSentinelListener)m_Seed;
            if (listener.IsAlive())
            {
                try
                {
                    return listener.Ping();
                }
                catch (Exception)
                { }
            }
            return false;
        }

        #endregion Methods
    }
}
