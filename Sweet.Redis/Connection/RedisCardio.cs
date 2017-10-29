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
using System.Collections.Generic;
using System.Linq;
using System.Threading;

namespace Sweet.Redis
{
    internal class RedisCardio : RedisInternalDisposable, IRedisCardio
    {
        #region CardioProbe

        private class CardioProbe : IDisposable, IEquatable<CardioProbe>
        {
            #region Field Members

            private bool m_IsDisposable;
            private DateTime? m_LastPulseTime;
            private IRedisHeartBeatProbe m_Probe;

            private long m_PulseState;

            #endregion Field Members

            #region .Ctors

            public CardioProbe(IRedisHeartBeatProbe probe, int intervalInSecs)
            {
                m_Probe = probe;
                m_IsDisposable = probe is IRedisDisposableBase;
                IntervalInSecs = Math.Max(RedisConstants.MinHeartBeatIntervalSecs, Math.Min(RedisConstants.MaxHeartBeatIntervalSecs, intervalInSecs));
            }

            #endregion .Ctors

            #region Properties

            public int IntervalInSecs { get; private set; }

            public IRedisHeartBeatProbe Probe { get { return m_Probe; } }

            #endregion Properties

            #region Methods

            public void Dispose()
            {
                Interlocked.Exchange(ref m_Probe, null);
            }

            public bool Pulse()
            {
                if (CanPulse())
                {
                    if (Interlocked.CompareExchange(ref m_PulseState, RedisConstants.One, RedisConstants.Zero) ==
                        RedisConstants.Zero)
                    {
                        try
                        {
                            m_LastPulseTime = DateTime.UtcNow;
                            return m_Probe.Pulse();
                        }
                        catch (Exception)
                        { }
                        finally
                        {
                            Interlocked.Exchange(ref m_PulseState, RedisConstants.Zero);
                        }
                    }
                }
                return false;
            }

            public bool CanPulse()
            {
                return !ReferenceEquals(m_Probe, null) &&
                    (!m_IsDisposable || !((IRedisDisposableBase)m_Probe).Disposed) &&
                    (!m_LastPulseTime.HasValue || (DateTime.UtcNow - m_LastPulseTime.Value).TotalSeconds >= IntervalInSecs);
            }

            #region Overrides

            public override bool Equals(object obj)
            {
                if (ReferenceEquals(obj, this))
                    return true;

                var cp = obj as CardioProbe;
                if (!ReferenceEquals(cp, null))
                    return ReferenceEquals(m_Probe, cp.Probe);

                return false;
            }

            public bool Equals(CardioProbe other)
            {
                if (ReferenceEquals(other, this))
                    return true;

                if (!ReferenceEquals(other, null))
                    return ReferenceEquals(m_Probe, other.Probe);

                return false;
            }

            public override int GetHashCode()
            {
                return !ReferenceEquals(m_Probe, null) ?
                    m_Probe.GetHashCode() : 0;
            }

            #endregion Overrides

            #endregion Methods

            #region Operator Overloads

            public static bool operator ==(CardioProbe a, CardioProbe b)
            {
                if (ReferenceEquals(a, null))
                    return ReferenceEquals(b, null);
                return ReferenceEquals(a.m_Probe, b.m_Probe);
            }

            public static bool operator !=(CardioProbe a, CardioProbe b)
            {
                return b != a;
            }

            public static bool operator ==(IRedisHeartBeatProbe a, CardioProbe b)
            {
                if (ReferenceEquals(a, null))
                    return ReferenceEquals(b, null);
                return ReferenceEquals(a, b.m_Probe);
            }

            public static bool operator !=(IRedisHeartBeatProbe a, CardioProbe b)
            {
                var bProbe = !ReferenceEquals(b, null) ? b.m_Probe : null;
                return !ReferenceEquals(a, bProbe);
            }

            public static bool operator ==(CardioProbe a, IRedisHeartBeatProbe b)
            {
                if (ReferenceEquals(a, null))
                    return ReferenceEquals(b, null);
                return ReferenceEquals(a.m_Probe, b);
            }

            public static bool operator !=(CardioProbe a, IRedisHeartBeatProbe b)
            {
                var aProbe = !ReferenceEquals(a, null) ? a.m_Probe : null;
                return !ReferenceEquals(aProbe, b);
            }

            #endregion Operator Overloads
        }

        #endregion CardioProbe

        #region Constants

        private const int PulseOnEveryMilliSecs = 1000;

        #endregion Constants
        #region Static Members

        public static readonly IRedisCardio Instance = new RedisCardio();

        #endregion Static Members


        #region Field Members

        private Timer m_Ticker;
        private readonly object m_SyncRoot = new object();
        private HashSet<CardioProbe> m_Probes = new HashSet<CardioProbe>();

        #endregion Field Members

        #region .Ctors

        private RedisCardio()
        { }

        #endregion .Ctors

        #region Destructors

        protected override void OnDispose(bool disposing)
        {
            base.OnDispose(disposing);
            Stop();
        }

        #endregion Destructors

        #region Methods

        public void Attach(IRedisHeartBeatProbe probe, int interval)
        {
            ValidateNotDisposed();

            if (!ReferenceEquals(probe, null))
            {
                var asDisposable = probe as IRedisDisposableBase;
                if (ReferenceEquals(asDisposable, null) || !asDisposable.Disposed)
                {
                    lock (m_SyncRoot)
                    {
                        m_Probes.Add(new CardioProbe(probe, interval));
                        Start();
                    }
                }
            }
        }

        public void Detach(IRedisHeartBeatProbe probe)
        {
            ValidateNotDisposed();
            if (!ReferenceEquals(probe, null))
            {
                lock (m_SyncRoot)
                {
                    m_Probes.RemoveWhere((c) => ReferenceEquals(c.Probe, probe));
                    if (m_Probes.Count == 0)
                        Stop();
                }
            }
        }

        private void Start()
        {
            if (m_Ticker == null)
            {
                lock (m_SyncRoot)
                {
                    if (m_Ticker == null)
                        Interlocked.Exchange(ref m_Ticker, new Timer((state) => { PulseAll(); },
                                                                     null, PulseOnEveryMilliSecs, PulseOnEveryMilliSecs));
                }
            }
        }

        private void Stop()
        {
            var timer = Interlocked.Exchange(ref m_Ticker, null);
            if (timer != null)
                timer.Dispose();
        }

        private void PulseAll()
        {
            if (Disposed)
            {
                Stop();
                return;
            }

            CardioProbe[] probes = null;
            lock (m_SyncRoot)
            {
                if (m_Probes != null && m_Probes.Count > 0)
                    probes = m_Probes.ToArray();
            }

            if (probes != null)
            {
                foreach (var probe in probes)
                {
                    try
                    {
                        if (probe.CanPulse())
                        {
                            Func<bool> pulse = probe.Pulse;
                            pulse.InvokeAsync();
                        }
                    }
                    catch (Exception)
                    { }
                }
            }
        }

        #endregion Methods
    }
}
