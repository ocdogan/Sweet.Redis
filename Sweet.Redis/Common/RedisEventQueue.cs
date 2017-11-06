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
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Sweet.Redis
{
    internal class RedisEventQueue : RedisDisposable
    {
        #region ManagerEvent

        private class ManagerEvent : IDisposable
        {
            #region Field Members

            private Action<object> m_Action;
            private RedisEventQueue m_EventQ;
            private object m_State;

            #endregion Field Members

            #region .Ctors

            public ManagerEvent(RedisEventQueue eventQ, Action<object> action, object state)
            {
                m_Action = action;
                m_EventQ = eventQ;
                m_State = state;
            }

            #endregion .Ctors

            #region Properties

            public Action<object> Action { get { return m_Action; } }

            public RedisEventQueue EventQ { get { return m_EventQ; } }

            public object State { get { return m_State; } }

            #endregion Properties

            #region Methods

            public void Dispose()
            {
                Interlocked.Exchange(ref m_Action, null);
                Interlocked.Exchange(ref m_EventQ, null);
                Interlocked.Exchange(ref m_State, null);
            }

            #endregion Methods
        }

        #endregion ManagerEvent

        #region Static Members

        private static long s_ProcessState;
        private static CancellationTokenSource s_CancelationTokenSource;

        private static int s_ProcessedQIndex = -1;
        private static readonly object s_EventQRegistryLock = new object();
        private static readonly List<RedisEventQueue> s_EventQRegistry = new List<RedisEventQueue>();

        public static readonly RedisEventQueue Default = new RedisEventQueue();

        #endregion Static Members

        #region Field Members

        private bool m_Registered;
        private ConcurrentQueue<ManagerEvent> m_ActionQ = new ConcurrentQueue<ManagerEvent>();

        #endregion Field Members

        #region .Ctors

        public RedisEventQueue()
        {
            RegisterEventQ(this);
        }

        #endregion .Ctors

        #region Destructors

        protected override void OnDispose(bool disposing)
        {
            base.OnDispose(disposing);

            UnregisterEventQ(this);
            ClearInternal();
        }

        #endregion Destructors

        #region Properties

        public int Count
        {
            get
            {
                var queue = m_ActionQ;
                return (queue != null) ? queue.Count : 0;
            }
        }

        public static bool Processing
        {
            get { return Interlocked.Read(ref s_ProcessState) != (long)RedisProcessState.Idle; }
        }

        #endregion Properties

        #region Methods

        #region Methods

        public void Enqueu(Action<object> action, object state = null)
        {
            if (action != null && !Disposed)
            {
                var actionQ = m_ActionQ;
                if (actionQ != null)
                {
                    actionQ.Enqueue(new ManagerEvent(this, action, state));
                    Start();
                }
            }
        }

        public void Clear()
        {
            ValidateNotDisposed();
            ClearInternal();
        }

        private void ClearInternal()
        {
            var actionQ = Interlocked.Exchange(ref m_ActionQ, new ConcurrentQueue<ManagerEvent>());
            if (actionQ != null)
            {
                ManagerEvent mEvent;
                while (actionQ.TryDequeue(out mEvent))
                {
                    if (mEvent != null)
                        mEvent.Dispose();
                }
            }
        }

        #endregion Methods

        #region Static Methods

        private static void RegisterEventQ(RedisEventQueue eventQ)
        {
            if ((eventQ != null) && !eventQ.m_Registered)
            {
                lock (s_EventQRegistry)
                {
                    if (!eventQ.m_Registered)
                    {
                        eventQ.m_Registered = true;
                        s_EventQRegistry.Add(eventQ);
                    }
                }
            }
        }

        private static void UnregisterEventQ(RedisEventQueue eventQ)
        {
            if ((eventQ != null) && eventQ.m_Registered)
            {
                lock (s_EventQRegistry)
                {
                    if (eventQ.m_Registered)
                    {
                        eventQ.m_Registered = false;

                        s_EventQRegistry.Remove(eventQ);
                        if (s_EventQRegistry.Count == 0)
                        {
                            var cts = Interlocked.Exchange(ref s_CancelationTokenSource, null);
                            if (cts != null)
                                cts.Cancel();
                        }
                    }
                }
            }
        }

        private static bool Initialize()
        {
            return Interlocked.CompareExchange(ref s_ProcessState, (long)RedisProcessState.Initialized,
                                            (long)RedisProcessState.Idle) == (long)RedisProcessState.Idle;
        }

        private static void Start()
        {
            if (Initialize())
            {
                try
                {
                    s_CancelationTokenSource = new CancellationTokenSource();
                    var token = s_CancelationTokenSource.Token;

                    var task = new Task((stateObject) =>
                    {
                        Process((CancellationToken)stateObject);
                    }, token, token, TaskCreationOptions.LongRunning);

                    task.ContinueWith(t =>
                    {
                        Interlocked.Exchange(ref s_ProcessState, (long)RedisProcessState.Idle);
                    });

                    task.Start();
                }
                catch (Exception)
                {
                    Interlocked.Exchange(ref s_ProcessState, (long)RedisProcessState.Idle);
                }
            }
        }

        private static RedisEventQueue NextQueue()
        {
            lock (s_EventQRegistryLock)
            {
                var index = NextIndex();
                var startIndex = index;
                do
                {
                    if (index < 0)
                        break;

                    var eventQ = s_EventQRegistry[index];
                    if (eventQ.Count > 0)
                        return eventQ;

                    index = NextIndex();
                    if (startIndex >= s_EventQRegistry.Count)
                        startIndex = 0;
                } while (index != startIndex);
                return null;
            }
        }

        private static int NextIndex()
        {
            lock (s_EventQRegistryLock)
            {
                var maxIndex = s_EventQRegistry.Count - 1;
                if (maxIndex < 0)
                    s_ProcessedQIndex = -1;
                else
                {
                    s_ProcessedQIndex++;
                    if (s_ProcessedQIndex > maxIndex)
                        s_ProcessedQIndex = 0;
                }
                return s_ProcessedQIndex;
            }
        }

        private static void Process(CancellationToken token)
        {
            try
            {
                Interlocked.Exchange(ref s_ProcessState, (long)RedisProcessState.Processing);

                var processed = false;
                var idleTime = (DateTime?)null;

                while (Processing && !token.IsCancellationRequested)
                {
                    processed = false;
                    try
                    {
                        var eventQ = NextQueue();
                        if (eventQ != null)
                            processed = eventQ.ProcessEvent();

                        if (processed)
                            idleTime = null;
                        else
                        {
                            if (!idleTime.HasValue)
                                idleTime = DateTime.UtcNow;
                            else if ((DateTime.UtcNow - idleTime.Value).TotalSeconds >= 30)
                            {
                                lock (s_EventQRegistryLock)
                                {
                                    var count = s_EventQRegistry.Count;
                                    if (count == 0)
                                        break;
                                }
                            }
                        }
                    }
                    catch (Exception)
                    { }
                    finally
                    {
                        if (!processed)
                            Thread.Sleep(1);
                    }
                }
            }
            catch (Exception)
            { }
            finally
            {
                Interlocked.Exchange(ref s_ProcessState, (long)RedisProcessState.Idle);
            }
        }

        private bool ProcessEvent()
        {
            if (!Disposed)
            {
                var actionQ = m_ActionQ;
                if (actionQ != null)
                {
                    ManagerEvent mEvent;
                    if (actionQ.TryDequeue(out mEvent) && (mEvent != null))
                    {
                        try
                        {
                            var action = mEvent.Action;
                            if (action != null)
                            {
                                var state = mEvent.State;
                                action(state);
                            }
                            return true;
                        }
                        catch (Exception)
                        { }
                        finally
                        {
                            mEvent.Dispose();
                        }
                    }
                }
            }
            return false;
        }

        #endregion Static Methods

        #endregion Methods
    }
}
