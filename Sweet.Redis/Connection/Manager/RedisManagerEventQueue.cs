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
    internal class RedisManagerEventQueue : RedisDisposable
    {
        #region ManagerEvent

        private class ManagerEvent : IDisposable
        {
            #region Field Members

            private WeakReference m_Action;
            private WeakReference m_EventQ;
            private WeakReference m_Manager;

            #endregion Field Members

            #region .Ctors

            public ManagerEvent(RedisManagerEventQueue eventQ, IRedisManager manager, Action action)
            {
                m_Action = new WeakReference(action);
                m_EventQ = new WeakReference(eventQ);
                m_Manager = new WeakReference(manager);
            }

            #endregion .Ctors

            #region Properties

            public Action Action
            {
                get
                {
                    var reference = m_Action;
                    if (reference != null && reference.IsAlive)
                        return (Action)reference.Target;
                    return null;
                }
            }

            public RedisManagerEventQueue EventQ
            {
                get
                {
                    var reference = m_EventQ;
                    if (reference != null && reference.IsAlive)
                    {
                        var eventQ = (RedisManagerEventQueue)reference.Target;
                        if (eventQ.IsAlive())
                            return eventQ;
                        Interlocked.Exchange(ref m_EventQ, null);
                    }
                    return null;
                }
            }

            public IRedisManager Manager
            {
                get
                {
                    var reference = m_Manager;
                    if (reference != null && reference.IsAlive)
                    {
                        var manager = (IRedisManager)reference.Target;
                        if (manager.IsAlive())
                            return manager;

                        if (manager != null)
                            Interlocked.Exchange(ref m_Manager, null);
                    }
                    return null;
                }
            }

            #endregion Properties

            #region Methods

            public void Dispose()
            {
                Interlocked.Exchange(ref m_Action, null);
                Interlocked.Exchange(ref m_EventQ, null);
                Interlocked.Exchange(ref m_Manager, null);
            }

            #endregion Methods
        }

        #endregion ManagerEvent

        #region Static Members

        private static long s_ProcessState;
        private static CancellationTokenSource s_CancelationTokenSource;

        private static int s_ProcessedQIndex = -1;
        private static readonly object s_EventQRegistryLock = new object();
        private static readonly List<RedisManagerEventQueue> s_EventQRegistry = new List<RedisManagerEventQueue>();

        #endregion Static Members

        #region Field Members

        private bool m_Registered;
        private IRedisManager m_Manager;
        private ConcurrentQueue<ManagerEvent> m_ActionQ = new ConcurrentQueue<ManagerEvent>();

        #endregion Field Members

        #region .Ctors

        public RedisManagerEventQueue(IRedisManager manager)
        {
            m_Manager = manager;
            RegisterEventQ(this);
        }

        #endregion .Ctors

        #region Destructors

        protected override void OnDispose(bool disposing)
        {
            Interlocked.Exchange(ref m_Manager, null);
            base.OnDispose(disposing);

            UnregisterEventQ(this);
            ClearInternal();
        }

        #endregion Destructors

        #region Properties

        public static bool Processing
        {
            get { return Interlocked.Read(ref s_ProcessState) != (long)RedisProcessState.Idle; }
        }

        #endregion Properties

        #region Methods

        #region Methods

        public void Enqueu(Action action)
        {
            if (action != null && !Disposed)
            {
                var actionQ = m_ActionQ;
                if (actionQ != null)
                {
                    actionQ.Enqueue(new ManagerEvent(this, m_Manager, action));
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

        private static void RegisterEventQ(RedisManagerEventQueue eventQ)
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

        private static void UnregisterEventQ(RedisManagerEventQueue eventQ)
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
                catch (Exception e)
                {
                    Interlocked.Exchange(ref s_ProcessState, (long)RedisProcessState.Idle);
                    Console.WriteLine(e);
                }
            }
        }

        private static RedisManagerEventQueue NextQueue()
        {
            lock (s_EventQRegistryLock)
            {
                var maxIndex = s_EventQRegistry.Count - 1;
                if (maxIndex < 0)
                {
                    s_ProcessedQIndex = -1;
                    return null;
                }

                s_ProcessedQIndex++;
                if (s_ProcessedQIndex > maxIndex)
                    s_ProcessedQIndex = 0;

                return s_EventQRegistry[s_ProcessedQIndex];
            }
        }

        private static void Process(CancellationToken token)
        {
            try
            {
                Interlocked.Exchange(ref s_ProcessState, (long)RedisProcessState.Processing);

                while (Processing && !token.IsCancellationRequested)
                {
                    try
                    {
                        var eventQ = NextQueue();
                        if (eventQ == null)
                            break;

                        try { eventQ.ProcessEvent(); }
                        finally
                        {
                            Thread.Sleep(1);
                        }
                    }
                    catch (Exception)
                    { }
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
                            var manager = mEvent.Manager;
                            if (manager.IsAlive())
                            {
                                var action = mEvent.Action;
                                if (action != null)
                                    action();
                                return true;
                            }
                        }
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
