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
using System.Threading.Tasks;

namespace Sweet.Redis
{
    internal class RedisAsyncMessageQ : RedisDisposable
    {
        #region Field Members

        private RedisAsyncRequest m_QTail;
        private readonly object m_AsyncMessageQLock = new object();
        private LinkedList<RedisAsyncRequest> m_AsyncMessageQ = new LinkedList<RedisAsyncRequest>();

        #endregion Field Members

        #region Destructors

        protected override void OnDispose(bool disposing)
        {
            base.OnDispose(disposing);
            CancelRequests();
        }

        #endregion Destructors

        #region Properties

        public bool IsEmpty
        {
            get
            {
                lock (m_AsyncMessageQLock)
                {
                    if (m_QTail != null)
                        return false;

                    var store = m_AsyncMessageQ;
                    if (store != null)
                        return store.Count == 0;
                }
                return true;
            }
        }

        #endregion Properties

        #region Methods

        private void CancelRequests()
        {
            lock (m_AsyncMessageQLock)
            {
                CancelRequest(Interlocked.Exchange(ref m_QTail, null));

                var store = m_AsyncMessageQ;
                if (store != null)
                {
                    var node = store.First;
                    while (node != null)
                    {
                        try
                        {
                            store.Remove(node);
                            CancelRequest(node.Value);
                        }
                        catch (Exception)
                        { }
                        finally
                        {
                            node = node.Next;
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
                    var tcs = member.CompletionSource;
                    if (tcs != null)
                    {
                        var task = tcs.Task;
                        if (task != null && !task.IsCompleted)
                        {
                            tcs.SetCanceled();
                        }
                    }
                }
                finally
                {
                    member.Dispose();
                }
            }
        }

        public RedisAsyncRequest Dequeue(int db)
        {
            ValidateNotDisposed();

            lock (m_AsyncMessageQLock)
            {
                var member = m_QTail;
                if (member != null)
                {
                    try
                    {
                        var command = member.Command;
                        if (command.Db == db)
                        {
                            m_QTail = null;

                            if (member.IsCompleted)
                                return member;
                        }
                    }
                    catch (Exception)
                    { }
                }
            }

            var store = m_AsyncMessageQ;
            if (store != null)
            {
                lock (m_AsyncMessageQLock)
                {
                    RedisAsyncRequest member;

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
                                    if (command.Db == db)
                                    {
                                        store.Remove(node);
                                        if (member.IsCompleted)
                                            return member;
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
                            node = node.Next;
                        }
                    }
                }
            }
            return null;
        }

        public RedisAsyncRequest Enqueue(RedisCommand command)
        {
            if (command != null)
            {
                ValidateNotDisposed();

                var tcs = new TaskCompletionSource<IRedisResponse>(command);
                var member = new RedisAsyncRequest(command, tcs);

                lock (m_AsyncMessageQLock)
                {
                    var prevTail = Interlocked.Exchange(ref m_QTail, member);
                    if (prevTail != null)
                    {
                        var store = m_AsyncMessageQ;
                        if (store != null)
                        {
                            store.AddLast(prevTail);
                        }
                    }
                }
                return member;
            }
            return null;
        }

        #endregion Methods
    }
}
