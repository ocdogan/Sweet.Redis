﻿#region License
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
    internal class RedisAsyncRequestQProcessor : RedisDisposable
    {
        #region ProcessParameters

        private class ProcessParameters
        {
            #region .Ctors

            public ProcessParameters(RedisAsyncRequestQProcessor processor,
                                     RedisAsyncRequestQ queue,
                                     RedisPoolSettings settings)
            {
                Processor = processor;
                Queue = queue;
                Settings = settings;
            }

            #endregion .Ctors

            #region Properties

            public RedisAsyncRequestQProcessor Processor { get; private set; }

            public RedisAsyncRequestQ Queue { get; private set; }

            public RedisPoolSettings Settings { get; private set; }

            #endregion Properties
        }

        #endregion ProcessParameters

        #region Static Members

        private static readonly ManualResetEventSlim s_GateKeeper = new ManualResetEventSlim(false);

        #endregion Static Members

        #region Constants

        private const int MaxIdleSpinCount = 100;
        private const int IdleTimeout = 60 * 1000;

        #endregion Constants

        #region Field Members

        private long m_State;

        #endregion Field Members

        #region .Ctors

        public RedisAsyncRequestQProcessor(RedisAsyncRequestQ queue, RedisPoolSettings settings)
        {
            Queue = queue;
            Settings = settings;
        }

        #endregion .Ctors

        #region Destructors

        protected override void OnDispose(bool disposing)
        {
            base.OnDispose(disposing);

            Queue = null;
            Settings = null;
            Stop();
        }

        #endregion Destructors

        #region Properties

        public bool Processing
        {
            get { return Interlocked.Read(ref m_State) != (long)RedisProcessState.Idle; }
        }

        public RedisAsyncRequestQ Queue { get; private set; }

        public RedisPoolSettings Settings { get; private set; }

        #endregion Properties

        #region Methods

        public void Start()
        {
            if (Interlocked.CompareExchange(ref m_State, (long)RedisProcessState.Initialized,
                                            (long)RedisProcessState.Idle) != (long)RedisProcessState.Idle)
            {
                s_GateKeeper.Set();
                return;
            }

            try
            {
                StopCurrent();

                ThreadPool.QueueUserWorkItem(ProcessQueueImpl,
                    new ProcessParameters(this, Queue, Settings));

                Interlocked.Exchange(ref m_State, (long)RedisProcessState.Processing);
            }
            catch (Exception)
            {
                Interlocked.Exchange(ref m_State, (long)RedisProcessState.Idle);
            }
        }

        public void Stop()
        {
            if (Interlocked.Exchange(ref m_State, (long)RedisProcessState.Idle) !=
                (long)RedisProcessState.Idle)
            {
                StopCurrent();
            }
        }

        private void StopCurrent()
        {
            ProcessCompleted();
        }

        private void ProcessCompleted()
        {
            Interlocked.Exchange(ref m_State, (long)RedisProcessState.Idle);
        }

        private static void OnReleaseSocket(IRedisConnection conn, RedisSocket socket)
        {
            socket.DisposeSocket();
        }

        private static void ProcessQueueImpl(object state)
        {
            ProcessQueue((ProcessParameters)state);
        }

        private static void ProcessQueue(ProcessParameters parameters)
        {
            var processor = parameters.Processor;
            try
            {
                var queue = parameters.Queue;
                var idleStart = DateTime.MinValue;

                var name = String.Format("{0}, {1}", typeof(RedisDbConnection).Name,
                    Guid.NewGuid().ToString("N").ToUpper());

                using (var connection = new RedisDbConnection(name, RedisRole.Master,
                           parameters.Settings, null, OnReleaseSocket, -1, null, false))
                {
                    var queueTimeoutMs = queue.TimeoutMilliseconds;

                    var context = (RedisSocketContext)null;

                    var commandDbIndex = -1;
                    var contextDbIndex = connection.DbIndex;

                    while (processor.Processing && queue.IsAlive())
                    {
                        RedisAsyncRequest request = null;
                        try
                        {
                            request = queue.Dequeue(contextDbIndex);
                            if (request == null && contextDbIndex != -1)
                                request = queue.Dequeue(-1);

                            if (request == null)
                            {
                                s_GateKeeper.Reset();
                                s_GateKeeper.Wait(IdleTimeout);
                                continue;
                            }

                            if (!request.IsCompleted)
                            {
                                var command = request.Command;
                                if (command == null)
                                    request.Cancel();
                                else
                                {
                                    if (queueTimeoutMs > -1 &&
                                        (DateTime.UtcNow - request.CreationTime).TotalMilliseconds >= queueTimeoutMs)
                                        request.Expire(queueTimeoutMs);
                                    else
                                    {
                                        if (!context.IsAlive())
                                            context = new RedisSocketContext(connection.Connect(), connection.Settings);

                                        commandDbIndex = command.DbIndex;
                                        if (commandDbIndex > -1 && commandDbIndex != contextDbIndex &&
                                            context.Socket.SelectDB(connection.Settings, commandDbIndex))
                                        {
                                            contextDbIndex = context.DbIndex;
                                            connection.SetDb(contextDbIndex);
                                        }

                                        request.Process(context);
                                    }
                                }
                            }
                        }
                        catch (Exception)
                        {
                            try
                            {
                                if (request != null)
                                    request.Cancel();
                            }
                            catch (Exception)
                            { }
                        }
                    }
                }
            }
            catch (Exception)
            { }
            finally
            {
                processor.ProcessCompleted();
            }
        }

        #endregion Methods
    }
}
