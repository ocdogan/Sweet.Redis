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

        private class CardioProbe : RedisInternalDisposable, IEquatable<CardioProbe>
        {
            #region Field Members

            private long m_PulseState;

            private long m_FailCount;
            private long m_SuccessCount;

            private RedisCardioProbeStatus m_Status = RedisCardioProbeStatus.OK;

            private bool m_IsDisposable;
            private DateTime? m_LastPulseTime;
            private IRedisHeartBeatProbe m_Probe;
            private Func<object, RedisCardioPulseStatus, bool> m_OnSetStatus;

            #endregion Field Members

            #region .Ctors

            public CardioProbe(IRedisHeartBeatProbe probe, int intervalInSecs,
                               Func<object, RedisCardioPulseStatus, bool> onSetHealthState)
            {
                m_Probe = probe;
                m_OnSetStatus = onSetHealthState;
                m_IsDisposable = probe is IRedisDisposableBase;
                IntervalInSecs = Math.Max(RedisConstants.MinHeartBeatIntervalSecs, Math.Min(RedisConstants.MaxHeartBeatIntervalSecs, intervalInSecs));
            }

            #endregion .Ctors

            #region Destructors

            protected override void OnDispose(bool disposing)
            {
                base.OnDispose(disposing);

                Interlocked.Exchange(ref m_OnSetStatus, null);
                Interlocked.Exchange(ref m_Probe, null);
            }

            #endregion Destructors

            #region Properties

            public long FailCount
            {
                get { return Interlocked.Read(ref m_FailCount); }
            }

            public int IntervalInSecs { get; private set; }

            public IRedisHeartBeatProbe Probe { get { return m_Probe; } }

            public bool Pulsing
            {
                get { return Interlocked.Read(ref m_PulseState) != RedisConstants.Zero; }
            }

            public RedisCardioProbeStatus Status
            {
                get { return m_Status; }
                set
                {
                    SetCounters(value);

                    var status = new RedisCardioPulseStatus(m_Probe, m_Status, value, FailCount, SuccessCount);
                    if (CanSetStatus(status))
                        m_Status = value;
                }
            }

            public long SuccessCount
            {
                get { return Interlocked.Read(ref m_SuccessCount); }
            }

            #endregion Properties

            #region Methods

            private void SetCounters(RedisCardioProbeStatus status)
            {
                if (status == RedisCardioProbeStatus.OK)
                {
                    Interlocked.Exchange(ref m_FailCount, RedisConstants.Zero);
                    if (Interlocked.Read(ref m_SuccessCount) < long.MaxValue)
                        Interlocked.Add(ref m_SuccessCount, RedisConstants.One);
                }
                else
                {
                    Interlocked.Exchange(ref m_SuccessCount, RedisConstants.Zero);
                    if (Interlocked.Read(ref m_FailCount) < long.MaxValue)
                        Interlocked.Add(ref m_FailCount, RedisConstants.One);
                }
            }

            public bool Pulse()
            {
                if (CanPulse() &&
                    Interlocked.CompareExchange(ref m_PulseState, RedisConstants.One, RedisConstants.Zero) ==
                    RedisConstants.Zero)
                {
                    try
                    {
                        m_LastPulseTime = DateTime.UtcNow;

                        var probe = m_Probe;
                        if (probe != null)
                        {
                            var result = probe.Pulse();
                            Status = result ? RedisCardioProbeStatus.OK : RedisCardioProbeStatus.Down;

                            return result;
                        }
                    }
                    catch (Exception)
                    {
                        Status = RedisCardioProbeStatus.Down;
                    }
                    finally
                    {
                        Interlocked.Exchange(ref m_PulseState, RedisConstants.Zero);
                    }
                }
                return false;
            }

            public bool CanPulse()
            {
                return !Disposed && !Pulsing &&
                    (!m_IsDisposable || ((IRedisDisposableBase)m_Probe).IsAlive()) &&
                    (!m_LastPulseTime.HasValue || (DateTime.UtcNow - m_LastPulseTime.Value).TotalSeconds >= IntervalInSecs);
            }

            private bool CanSetStatus(RedisCardioPulseStatus status)
            {
                try
                {
                    var onSetStatus = m_OnSetStatus;
                    if (onSetStatus != null)
                        return onSetStatus(this, status);
                }
                catch (Exception)
                { }
                return true;
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

        public static readonly IRedisCardio Default = new RedisCardio();

        #endregion Static Members

        #region Field Members

        private long m_PulseState;
        private long m_TickState;

        private Timer m_Ticker;
        private readonly object m_SyncRoot = new object();
        private HashSet<CardioProbe> m_Probes = new HashSet<CardioProbe>();

        private Func<object, RedisCardioPulseStatus, bool> m_StateUpdateStrategy;

        #endregion Field Members

        #region .Ctors

        private RedisCardio()
        { }

        #endregion .Ctors

        #region Destructors

        protected override void OnDispose(bool disposing)
        {
            var probes = Interlocked.Exchange(ref m_Probes, null);
            if (probes != null)
            {
                foreach (var cp in probes)
                {
                    try
                    {
                        if (!ReferenceEquals(cp, null))
                            cp.Dispose();
                    }
                    catch (Exception)
                    { }
                }
            }

            base.OnDispose(disposing);
            Stop();
        }

        #endregion Destructors

        #region Properties

        public bool Pulsing
        {
            get { return Interlocked.Read(ref m_PulseState) != RedisConstants.Zero; }
        }

        #endregion Properties

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
                        var probes = m_Probes;
                        if (probes != null)
                        {
                            var exists = probes.Any(cp => cp.IsAlive() &&
                                                    ReferenceEquals(probe, cp.Probe));
                            if (!exists && !Disposed)
                            {
                                probes.Add(new CardioProbe(probe, interval, (obj, status) =>
                                {
                                    if (status.NewStatus != status.CurrentStatus)
                                    {
                                        var canSet = CanUpdateHealthState(obj, status);
                                        if (canSet)
                                        {
                                            try
                                            {
                                                var cp = (CardioProbe)obj;

                                                var prb = cp.Probe;
                                                if (!ReferenceEquals(prb, null))
                                                    prb.PulseStateChanged(status);
                                            }
                                            catch (Exception)
                                            { }
                                        }
                                        return canSet;
                                    }
                                    return false;
                                }));
                                Start();
                            }
                        }
                    }
                }
            }
        }

        public void Detach(IRedisHeartBeatProbe probe)
        {
            if (!ReferenceEquals(probe, null))
            {
                lock (m_SyncRoot)
                {
                    var probes = m_Probes;
                    if (probes == null)
                        Stop();
                    else
                    {
                        var cp = probes.FirstOrDefault((c) => ReferenceEquals(c.Probe, probe));
                        if (!ReferenceEquals(cp, null))
                        {
                            try
                            {
                                if (probes.Remove(cp))
                                    cp.Dispose();
                            }
                            finally
                            {
                                if (probes.Count == 0)
                                    Stop();
                            }
                        }
                    }
                }
            }
        }

        private void Start()
        {
            if (m_Ticker == null && !Disposed)
            {
                lock (m_SyncRoot)
                {
                    if (m_Ticker == null && !Disposed)
                    {
                        Interlocked.Exchange(ref m_PulseState, RedisConstants.One);

                        Interlocked.Exchange(ref m_Ticker, new Timer((state) =>
                        {
                            if (Interlocked.CompareExchange(ref m_TickState, RedisConstants.One, RedisConstants.Zero) ==
                                RedisConstants.Zero)
                            {
                                try
                                {
                                    PulseAll();
                                }
                                finally
                                {
                                    Interlocked.Exchange(ref m_TickState, RedisConstants.Zero);
                                }
                            }
                        },
                        null, PulseOnEveryMilliSecs, PulseOnEveryMilliSecs));
                    }
                }
            }
        }

        private bool CanUpdateHealthState(object sender, RedisCardioPulseStatus status)
        {
            if (status.NewStatus != status.CurrentStatus)
            {
                var strategy = m_StateUpdateStrategy;
                if (strategy != null)
                    return strategy(sender, status);

                return status.FailCount == RedisConstants.CardioProbeStatusChangeRetryCount ||
                    status.SuccessCount == RedisConstants.CardioProbeStatusChangeRetryCount;
            }
            return false;
        }

        public void SetStateUpdateStrategy(Func<object, RedisCardioPulseStatus, bool> strategy)
        {
            Interlocked.Exchange(ref m_StateUpdateStrategy, strategy);
        }

        private void Stop()
        {
            Interlocked.Exchange(ref m_PulseState, RedisConstants.Zero);

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

            if (Pulsing)
            {
                CardioProbe[] probesList = null;
                lock (m_SyncRoot)
                {
                    var probes = m_Probes;
                    if (probes != null && probes.Count > 0)
                        probesList = probes.ToArray();
                }

                if (probesList != null)
                {
                    foreach (var cp in probesList)
                    {
                        try
                        {
                            if (Disposed || !Pulsing)
                                return;

                            if (cp.CanPulse())
                            {
                                Func<bool> pulse = cp.Pulse;
                                pulse.InvokeAsync();
                            }
                        }
                        catch (Exception)
                        { }
                    }
                }
            }
        }

        #endregion Methods
    }
}
