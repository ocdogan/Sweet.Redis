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
using System.Threading.Tasks;

namespace Sweet.Redis
{
    internal class RedisAsyncRequestQProcessor : RedisDisposable
    {
        #region Constants

        protected const int MreSpinCount = 3;
        protected const int SpinSleepTime = 100;

        #endregion Constants

        #region Field Members

        private long m_State;
        private RedisAsyncRequestQ m_AsycRequestQ;

        private Action<RedisAsyncRequest, RedisSocketContext> m_OnProcessRequest;

        private int m_IdleTimeout = 10 * 1000;
        private readonly ManualResetEventSlim m_EnqueueGate = new ManualResetEventSlim(false, MreSpinCount);

        #endregion Field Members

        #region .Ctors

        public RedisAsyncRequestQProcessor(RedisPoolSettings settings,
            Action<RedisAsyncRequest, RedisSocketContext> onProcessRequest = null)
        {
            Settings = settings;
            m_OnProcessRequest = onProcessRequest;
            m_IdleTimeout = settings.ConnectionIdleTimeout;

            var queueTimeout = settings.SendTimeout;
            if (queueTimeout < 0)
                queueTimeout = settings.ReceiveTimeout;
            else
            {
                var receiveTimeout = settings.ReceiveTimeout;
                if (receiveTimeout > 0)
                    queueTimeout += receiveTimeout;
            }

            m_AsycRequestQ = new RedisAsyncRequestQ(queueTimeout);
        }

        #endregion .Ctors

        #region Destructors

        protected override void OnDispose(bool disposing)
        {
            base.OnDispose(disposing);

            Interlocked.Exchange(ref m_OnProcessRequest, null);

            var asycRequestQ = Interlocked.Exchange(ref m_AsycRequestQ, null);
            if (asycRequestQ != null)
                asycRequestQ.Dispose();

            Stop();
        }

        #endregion Destructors

        #region Properties

        public int IdleTimeout
        {
            get { return m_IdleTimeout; }
        }

        public bool Processing
        {
            get
            {
                return !Disposed &&
                  Interlocked.Read(ref m_State) != (long)RedisProcessState.Idle;
            }
        }

        public RedisAsyncRequestQ Queue
        {
            get { return m_AsycRequestQ; }
        }

        public RedisPoolSettings Settings { get; private set; }

        #endregion Properties

        #region Methods

        public void Start()
        {
            if (Interlocked.CompareExchange(ref m_State, (long)RedisProcessState.Initialized,
                                            (long)RedisProcessState.Idle) != (long)RedisProcessState.Idle)
            {
                m_EnqueueGate.Set();
                return;
            }

            try
            {
                StartInternal();
                Interlocked.Exchange(ref m_State, (long)RedisProcessState.Processing);
            }
            catch (Exception)
            {
                Interlocked.Exchange(ref m_State, (long)RedisProcessState.Idle);
            }
        }

        protected virtual void StartInternal()
        {
            ThreadPool.QueueUserWorkItem(ProcessQueueCallback, this);
        }

        public void Stop()
        {
            DoProcessCompleted();
        }

        public Task<T> Enqueue<T>(RedisCommand command, RedisCommandExpect expect, string okIf)
        {
            var asycRequestQ = m_AsycRequestQ;
            if (asycRequestQ != null)
            {
                var asyncRequest = asycRequestQ.Enqueue<T>(command, expect, okIf);
                Start();
                return asyncRequest.Task;
            }
            return null;
        }

        protected virtual void DoProcessCompleted()
        {
            Interlocked.Exchange(ref m_State, (long)RedisProcessState.Idle);
            m_EnqueueGate.Reset();
        }

        private static void OnReleaseSocket(IRedisConnection conn, RedisSocket socket)
        {
            socket.DisposeSocket();
        }

        private static void ProcessQueueCallback(object state)
        {
            var processor = (RedisAsyncRequestQProcessor)state;
            if (processor.IsAlive())
                processor.ProcessQueue();
        }

        protected virtual void DoProcessRequest(RedisAsyncRequest request, RedisSocketContext context)
        {
             request.Process(context);
        }

        protected virtual bool DisposeRequestAfterProcess()
        {
            return true;
        }
       
        protected virtual void ProcessQueue()
        {
            var queue = m_AsycRequestQ;
            if (queue == null)
                return;

            var context = (RedisSocketContext)null;
            try
            {
                var name = String.Format("{0}, {1}", typeof(RedisDbConnection).Name,
                    Guid.NewGuid().ToString("N").ToUpper());

                var idleTime = 0;
                var idleStart = DateTime.MinValue;

                var request = (RedisAsyncRequest)null;

                var onProcessRequest = m_OnProcessRequest;
                if (onProcessRequest == null)
                    onProcessRequest = DoProcessRequest;

                var disposeRequest = DisposeRequestAfterProcess();

                using (var connection =
                    new RedisDbConnection(name, RedisRole.Master, Settings, null, OnReleaseSocket, -1, null, false))
                {
                    var commandDbIndex = -1;
                    var contextDbIndex = connection.DbIndex;

                    m_EnqueueGate.Reset();
                    var idleTimeout = IdleTimeout;

                    while (Processing && !queue.Disposed)
                    {
                        try
                        {
                            if (!queue.TryDequeueOneOf(contextDbIndex, RedisConstants.UninitializedDbIndex, out request))
                            {
                                if (m_EnqueueGate.Wait(SpinSleepTime))
                                    m_EnqueueGate.Reset();
                                else
                                {
                                    idleTime += SpinSleepTime;
                                    if (idleTime >= idleTimeout)
                                        break;

                                    if (idleTime > 0 && idleTime % 2000 == 0)
                                    {
                                        var beatCommand = new RedisCommand(RedisConstants.UninitializedDbIndex,
                                                                            RedisCommandList.Ping);
                                        beatCommand.IsHeartBeat = true;

                                        Enqueue<RedisBool>(beatCommand,
                                                            RedisCommandExpect.OK, RedisConstants.PONG);
                                    }
                                }
                                continue;
                            }

                            try
                            {
                                var command = request.Command;
                                if (ReferenceEquals(command, null) || !command.IsHeartBeat)
                                    idleTime = 0;

                                if (!request.IsCompleted)
                                {
                                    if (!context.IsAlive())
                                    {
                                        try
                                        {
                                            context = new RedisSocketContext(connection.Connect(), connection.Settings);
                                        }
                                        catch (Exception e)
                                        {
                                            if (e.IsSocketError())
                                                break;
                                        }
                                    }

                                    commandDbIndex = command.DbIndex;

                                    if (commandDbIndex != contextDbIndex &&
                                        commandDbIndex > RedisConstants.UninitializedDbIndex &&
                                        context.Socket.SelectDB(connection.Settings, commandDbIndex))
                                    {
                                        contextDbIndex = context.DbIndex;
                                        connection.SelectDB(contextDbIndex);
                                    }

                                    onProcessRequest(request, context);
                                }
                            }
                            catch (Exception)
                            {
                                request.Cancel();
                            }
                            finally
                            {
                                if (disposeRequest && !ReferenceEquals(request, null))
                                    request.Dispose();
                            }
                        }
                        catch (Exception)
                        { }
                    }
                }
            }
            catch (Exception)
            { }
            finally
            {
                if (queue != null) queue.CancelRequests();
                if (context.IsAlive()) context.Dispose();

                DoProcessCompleted();
            }
        }

        #endregion Methods
    }
}
