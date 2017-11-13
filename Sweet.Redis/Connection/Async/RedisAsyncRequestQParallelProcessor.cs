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
using System.Threading;
using System.Threading.Tasks;

namespace Sweet.Redis
{
    internal class RedisAsyncRequestQParallelProcessor : RedisAsyncRequestQProcessor
    {
        #region Field Members

        private RedisSocketContext m_CurrentContext;
        private ConcurrentQueue<RedisAsyncRequest> m_ReceiveQ = new ConcurrentQueue<RedisAsyncRequest>();

        private readonly ManualResetEventSlim m_ReceiveGate = new ManualResetEventSlim(false);

        #endregion Field Members

        #region .Ctors

        public RedisAsyncRequestQParallelProcessor(RedisPoolSettings settings)
            : base(settings, null)
        { }

        #endregion .Ctors

        #region Destructors

        protected override void OnDispose(bool disposing)
        {
            base.OnDispose(disposing);

            Interlocked.Exchange(ref m_CurrentContext, null);

            var receiveQ = Interlocked.Exchange(ref m_ReceiveQ, null);
            if (receiveQ != null)
            {
                RedisAsyncRequest request;
                while (receiveQ.TryDequeue(out request))
                    request.Cancel();
            }
        }

        #endregion Destructors

        #region Methods

        protected override void StartInternal()
        {
            base.StartInternal();
            ThreadPool.QueueUserWorkItem(ProcessReceiveQueueCallback, this);
        }

        protected override void DoProcessRequest(RedisAsyncRequest request, RedisSocketContext context)
        {
            var cancelRequest = true;
            try
            {
                if (Disposed)
                    return;

                if (request.Send(context))
                {
                    if (Disposed)
                        return;

                    Interlocked.Exchange(ref m_CurrentContext, context);

                    var receiveQ = m_ReceiveQ;
                    if (receiveQ != null)
                    {
                        receiveQ.Enqueue(request);
                        cancelRequest = false;

                        m_ReceiveGate.Set();
                    }
                }
            }
            catch (Exception)
            { }
            finally
            {
                if (cancelRequest)
                    request.Cancel();
            }
        }

        protected override void DoProcessCompleted()
        {
            base.DoProcessCompleted();
            m_ReceiveGate.Reset();
        }

        private static void ProcessReceiveQueueCallback(object state)
        {
            var processor = (RedisAsyncRequestQParallelProcessor)state;
            if (processor.IsAlive())
                processor.ProcessReceiveQueue();
        }

        protected override bool DisposeRequestAfterProcess()
        {
            return false;
        }

        private void ProcessReceiveQueue()
        {
            var queue = m_ReceiveQ;
            if (queue == null)
                return;

            m_ReceiveGate.Reset();

            var idleTime = 0;
            var spareSpinCount = 0;
            var idleStart = DateTime.MinValue;

            var request = (RedisAsyncRequest)null;

            try
            {
                using (var reader = new RedisSingleResponseReader(Settings))
                {
                    while (Processing)
                    {
                        if (!queue.TryDequeue(out request))
                        {
                            if (spareSpinCount++ < 3)
                            {
                                Thread.Yield();
                                if (m_ReceiveGate.IsSet)
                                    m_ReceiveGate.Reset();
                            }
                            else
                            {
                                if (m_ReceiveGate.Wait(SpinSleepTime))
                                    m_ReceiveGate.Reset();
                                else
                                {
                                    idleTime += SpinSleepTime;
                                    if (idleTime >= IdleTimeout)
                                        break;
                                }
                            }
                            continue;
                        }

                        using (request)
                        {
                            if (idleTime > 0)
                            {
                                var command = request.Command;
                                if (ReferenceEquals(command, null) || !command.IsHeartBeat)
                                {
                                    idleTime = 0;
                                    Thread.Yield();
                                }
                            }

                            try
                            {
                                var context = m_CurrentContext;
                                if (context == null)
                                    continue;

                                if (!request.IsCompleted)
                                {
                                    var socket = context.Socket;
                                    if (socket.IsConnected())
                                    {
                                        try
                                        {
                                            var execResult = reader.Execute(socket);
                                            if (ReferenceEquals(execResult, null))
                                                throw new RedisFatalException("Corrupted redis response data", RedisErrorCode.CorruptResponse);

                                            execResult.HandleError();

                                            var rawObj = RedisRawObject.ToObject(execResult);
                                            if (ReferenceEquals(rawObj, null))
                                                throw new RedisFatalException("Corrupted redis response data", RedisErrorCode.CorruptResponse);

                                            if (!request.ProcessResult(rawObj))
                                                request.Cancel();
                                        }
                                        catch (Exception e)
                                        {
                                            request.SetException(e);
                                        }
                                    }
                                }
                            }
                            catch (Exception)
                            { }
                        }
                    }
                }
            }
            finally
            {
                while (queue.TryDequeue(out request))
                {
                    try { request.Cancel(); }
                    catch (Exception) { }
                }
            }
        }

        #endregion Methods
    }
}
