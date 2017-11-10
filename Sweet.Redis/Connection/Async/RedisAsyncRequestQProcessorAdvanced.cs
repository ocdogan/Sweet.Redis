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
    internal class RedisAsyncRequestQProcessorAdvanced : RedisAsyncRequestQProcessorSimple
    {
        #region Field Members

        private RedisSocketContext m_CurrentContext;
        private ConcurrentQueue<RedisAsyncRequest> m_ReceiveQ = new ConcurrentQueue<RedisAsyncRequest>();

        private readonly ManualResetEventSlim m_ReceiveGate = new ManualResetEventSlim(false);

        #endregion Field Members

        #region .Ctors

        public RedisAsyncRequestQProcessorAdvanced(RedisPoolSettings settings)
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
            ThreadPool.QueueUserWorkItem(ProcessReceiveQueueCallback, this);
        }

        protected override void DoProcessRequest(RedisAsyncRequest request, RedisSocketContext context)
        {
            if (Disposed)
            {
                request.Cancel();
                return;
            }

            try
            {
                if (request.Send(context))
                {
                    if (Disposed)
                    {
                        request.Cancel();
                        return;
                    }

                    Interlocked.Exchange(ref m_CurrentContext, context);

                    var receiveQ = m_ReceiveQ;
                    if (receiveQ == null)
                        request.Cancel();
                    else
                    {
                        receiveQ.Enqueue(request);
                        m_ReceiveGate.Set();
                    }
                }
            }
            catch (Exception)
            {
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
            var processor = (RedisAsyncRequestQProcessorAdvanced)state;
            if (processor.IsAlive())
                processor.ProcessReceiveQueue();
        }

        private void ProcessReceiveQueue()
        {
            var queue = m_ReceiveQ;
            if (queue == null)
                return;

            m_ReceiveGate.Reset();

            var idleTime = 0;
            var idleStart = DateTime.MinValue;

            var request = (RedisAsyncRequest)null;

            while (Processing)
            {
                try
                {
                    if (!queue.TryDequeue(out request))
                    {
                        if (m_ReceiveGate.Wait(SpinSleepTime))
                            m_ReceiveGate.Reset();
                        else
                        {
                            idleTime += SpinSleepTime;
                            if (idleTime >= IdleTimeout)
                                break;
                        }
                        continue;
                    }

                    idleTime = 0;
                    try
                    {
                        var context = m_CurrentContext;
                        if (context == null)
                        {
                            request.Cancel();
                            continue;
                        }

                        if (!request.IsCompleted)
                            request.Receive(context);
                    }
                    catch (Exception)
                    {
                        request.Cancel();
                    }
                }
                catch (Exception)
                {
                    if (request != null)
                        request.Cancel();
                }
            }
        }

        #endregion Methods
    }
}
