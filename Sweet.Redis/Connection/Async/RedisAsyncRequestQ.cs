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
using System.Threading;
using System.Threading.Tasks;

namespace Sweet.Redis
{
    internal class RedisAsyncRequestQ : RedisDisposable
    {
        #region Constants

        private const int MaxTimeout = 60 * 1000;

        #endregion Constants

        #region Field Members

        private int m_Count;
        private int m_TimeoutMilliseconds;

        private RedisAsyncRequest m_QTail;
        private readonly object m_AsyncMessageQLock = new object();
        private LinkedList<RedisAsyncRequest> m_AsyncRequestQ = new LinkedList<RedisAsyncRequest>();

        #endregion Field Members

        #region .Ctors

        public RedisAsyncRequestQ(int timeoutMilliseconds = MaxTimeout)
        {
            m_TimeoutMilliseconds = Math.Min(Math.Max(timeoutMilliseconds, Timeout.Infinite), MaxTimeout);
        }

        #endregion .Ctors

        #region Destructors

        protected override void OnDispose(bool disposing)
        {
            base.OnDispose(disposing);
            CancelRequests();
        }

        #endregion Destructors

        #region Properties

        public object SyncObj
        {
            get { return m_AsyncMessageQLock; }
        }

        public bool IsEmpty
        {
            get
            {
                lock (m_AsyncMessageQLock)
                {
                    return m_Count > 0;
                }
            }
        }

        #endregion Properties

        #region Methods

        public void CancelRequests()
        {
            lock (m_AsyncMessageQLock)
            {
                CancelRequest(Interlocked.Exchange(ref m_QTail, null));

                var store = m_AsyncRequestQ;
                if (store != null)
                {
                    var node = store.First;
                    while (node != null)
                    {
                        var nextNode = node.Next;
                        try
                        {
                            store.Remove(node);
                            CancelRequest(node.Value);
                        }
                        catch (Exception)
                        { }
                        finally
                        {
                            node = nextNode;
                        }
                    }
                }
            }
        }

        private void CancelRequest(RedisAsyncRequest member)
        {
            if (member != null)
            {
                try
                {
                    member.Cancel();
                }
                finally
                {
                    member.Dispose();
                }
            }
        }

        public bool TryDequeue(int dbIndex, out RedisAsyncRequest result)
        {
            result = null;
            var member = (RedisAsyncRequest)null;

            lock (m_AsyncMessageQLock)
            {
                if (m_Count == 0)
                    return false;

                member = m_QTail;
                m_QTail = null;
            }

            if (member != null)
            {
                try
                {
                    var command = member.Command;
                    if (dbIndex < 0 || command.DbIndex == dbIndex)
                    {
                        m_Count--;
                        result = member;
                        return true;
                    }
                }
                catch (Exception)
                { }
            }

            if (m_AsyncRequestQ != null)
            {
                lock (m_AsyncMessageQLock)
                {
                    var store = m_AsyncRequestQ;
                    if (store != null)
                    {
                        var node = store.First;
                        while (node != null)
                        {
                            try
                            {
                                member = node.Value;
                                if (member != null)
                                {
                                    try
                                    {
                                        var command = member.Command;
                                        if (dbIndex < 0 || command.DbIndex == dbIndex)
                                        {
                                            store.Remove(node);
                                            m_Count--;

                                            result = member;
                                            return true;
                                        }
                                    }
                                    catch (Exception)
                                    { }
                                }
                            }
                            catch (Exception)
                            { }

                            node = node.Next;
                        }
                    }
                }
            }
            return false;
        }

        public bool TryDequeueOneOf(int dbIndex1, int dbIndex2, out RedisAsyncRequest result)
        {
            result = null;
            var member = (RedisAsyncRequest)null;

            lock (m_AsyncMessageQLock)
            {
                if (m_Count == 0)
                    return false;

                member = m_QTail;
                m_QTail = null;
            }

            if (member != null)
            {
                try
                {
                    var command = member.Command;
                    if ((dbIndex1 < 0 || command.DbIndex == dbIndex1) ||
                        (dbIndex2 < 0 || command.DbIndex == dbIndex2))
                    {
                        m_Count--;
                        result = member;
                        return true;
                    }
                }
                catch (Exception)
                { }
            }

            if (m_AsyncRequestQ != null)
            {
                lock (m_AsyncMessageQLock)
                {
                    var store = m_AsyncRequestQ;
                    if (store != null)
                    {
                        var node = store.First;
                        while (node != null)
                        {
                            try
                            {
                                member = node.Value;
                                if (member != null)
                                {
                                    try
                                    {
                                        var command = member.Command;
                                        if ((dbIndex1 < 0 || command.DbIndex == dbIndex1) ||
                                            (dbIndex2 < 0 || command.DbIndex == dbIndex2))
                                        {
                                            store.Remove(node);
                                            m_Count--;

                                            result = member;
                                            return true;
                                        }
                                    }
                                    catch (Exception)
                                    { }
                                }
                            }
                            catch (Exception)
                            { }

                            node = node.Next;
                        }
                    }
                }
            }
            return false;
        }

        public bool Enqueue(RedisAsyncRequest request)
        {
            if (request != null)
            {
                ValidateNotDisposed();

                EnqueueInternal(request);
                return true;
            }
            return false;
        }

        public RedisAsyncRequest<T> Enqueue<T>(RedisCommand command, RedisCommandExpect expect, string okIf)
        {
            if (command != null)
            {
                ValidateNotDisposed();

                var member = new RedisAsyncRequest<T>(command, expect, okIf,
                                  new TaskCompletionSource<T>(command));

                EnqueueInternal(member);
                return member;
            }
            return null;
        }

        private void EnqueueInternal(RedisAsyncRequest request)
        {
            lock (m_AsyncMessageQLock)
            {
                var prevTail = Interlocked.Exchange(ref m_QTail, request);
                if (prevTail != null)
                {
                    var store = m_AsyncRequestQ;
                    if (store != null)
                        store.AddLast(prevTail);
                }
                m_Count++;
            }
        }

        #endregion Methods
    }
}
