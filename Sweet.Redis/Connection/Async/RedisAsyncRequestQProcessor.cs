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
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Sweet.Redis
{
    internal class RedisAsyncRequestQProcessor : RedisDisposable
    {
        #region ProcessingState

        private enum ProcessingState : long
        {
            Idle = 0L,
            Starting = 1L,
            Processing = 2L
        }

        #region ProcessParameters

        private class ProcessParameters
        {
            #region .Ctors

            public ProcessParameters(RedisAsyncRequestQProcessor processor,
                                     Thread thread, RedisAsyncRequestQ queue,
                                     RedisSettings settings)
            {
                Me = thread;
                Processor = processor;
                Queue = queue;
                Settings = settings;
            }

            #endregion .Ctors

            #region Properties

            public Thread Me { get; private set; }

            public RedisAsyncRequestQProcessor Processor { get; private set; }

            public RedisAsyncRequestQ Queue { get; private set; }

            public RedisSettings Settings { get; private set; }

            #endregion Properties
        }

        #endregion ProcessParameters

        #endregion ProcessingState

        #region Constants

        private const int IdleTimout = 60 * 1000;

        #endregion Constants

        #region Field Members

        private Thread m_Thread;
        private long m_State;

        #endregion Field Members

        #region .Ctors

        public RedisAsyncRequestQProcessor(RedisAsyncRequestQ queue, RedisSettings settings)
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
            get { return Interlocked.Read(ref m_State) != (long)ProcessingState.Idle; }
        }

        public RedisAsyncRequestQ Queue { get; private set; }

        public RedisSettings Settings { get; private set; }

        #endregion Properties

        #region Methods

        public void Start()
        {
            if (Interlocked.CompareExchange(ref m_State, (long)ProcessingState.Starting,
                                            (long)ProcessingState.Idle) == (long)ProcessingState.Idle)
            {
                try
                {
                    StopCurrent();

                    var thread = new Thread((p) =>
                    {
                        ProcessQueue((ProcessParameters)p);
                    });
                    thread.IsBackground = true;

                    Interlocked.Exchange(ref m_Thread, thread);
                    thread.Start(new ProcessParameters(this, thread, Queue, Settings));

                    Interlocked.Exchange(ref m_State, (long)ProcessingState.Processing);
                }
                catch (Exception)
                {
                    Interlocked.Exchange(ref m_State, (long)ProcessingState.Idle);
                }
            }
        }

        public void Stop()
        {
            if (Interlocked.Exchange(ref m_State, (long)ProcessingState.Idle) !=
                (long)ProcessingState.Idle)
            {
                StopCurrent();
            }
        }

        private void StopCurrent()
        {
            var thread = Interlocked.Exchange(ref m_Thread, null);
            if (thread != null && thread.IsAlive)
            {
                try
                {
                    thread.Interrupt();
                }
                catch (Exception)
                { }
            }
        }

        private void ProcessCompleted(Thread thread)
        {
            if (thread == m_Thread)
            {
                Interlocked.Exchange(ref m_State, (long)ProcessingState.Idle);
            }
        }

        private static void OnReleaseSocket(IRedisConnection conn, RedisSocket socket)
        {
            socket.DisposeSocket();
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
                    var idleSpinCount = 0;
                    var queueTimeoutMs = queue.TimeoutMilliseconds;

                    while (processor.Processing && !queue.Disposed)
                    {
                        RedisAsyncRequest request = null;
                        try
                        {
                            request = queue.Dequeue(connection.DbIndex);
                            if (request == null && connection.DbIndex != -1)
                                request = queue.Dequeue(-1);

                            if (request == null)
                            {
                                if (idleStart == DateTime.MinValue)
                                    idleStart = DateTime.UtcNow;
                                else if ((DateTime.UtcNow - idleStart).TotalMilliseconds >= IdleTimout)
                                    break;

                                idleSpinCount = Math.Max(100, ++idleSpinCount);

                                Thread.Sleep(idleSpinCount < 100 ? 0 : 1);
                                continue;
                            }

                            idleSpinCount = 0;
                            idleStart = DateTime.MinValue;

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
                                        if (command.DbIndex > -1 &&
                                            command.DbIndex != connection.DbIndex)
                                            connection.Select(command.DbIndex);

                                        request.Process(connection);
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
                processor.ProcessCompleted(parameters.Me);
            }
        }

        #endregion Methods
    }
}
