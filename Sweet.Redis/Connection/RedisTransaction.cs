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
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Net.Sockets;
using System.Threading;

namespace Sweet.Redis
{
    internal class RedisTransaction : RedisBatch, IRedisTransaction
    {
        #region Field Members

        private ConcurrentQueue<RedisParam> m_WatchQ = new ConcurrentQueue<RedisParam>();

        #endregion Field Members

        #region .Ctors

        public RedisTransaction(RedisConnectionPool pool, int dbIndex, bool throwOnError = true)
            : base(pool, dbIndex, throwOnError)
        { }

        #endregion .Ctors

        #region Destructors

        protected override void OnDispose(bool disposing)
        {
            base.OnDispose(disposing);
            Interlocked.Exchange(ref m_WatchQ, null);
        }

        #endregion Destructors

        #region Methods

        #region Execution Methods

        public bool Commit()
        {
            return Flush();
        }

        public bool Discard()
        {
            return Rollback();
        }

        public bool Watch(RedisParam key, params RedisParam[] keys)
        {
            ValidateNotDisposed();

            if (Interlocked.Read(ref m_State) == (long)RedisBatchState.Executing)
                throw new RedisException("Transaction is being executed");

            if (!key.IsEmpty)
                m_WatchQ.Enqueue(key);

            var length = keys.Length;
            if (length > 0)
            {
                foreach (var k in keys)
                {
                    if (!k.IsEmpty)
                        m_WatchQ.Enqueue(k);
                }
            }
            return true;
        }

        public bool Unwatch()
        {
            ValidateNotDisposed();

            var queue = m_WatchQ;
            if (queue == null || queue.Count > 0)
                Interlocked.Exchange(ref m_WatchQ, new ConcurrentQueue<RedisParam>());

            return true;
        }

        protected override void OnFlush(IList<RedisRequest> requests, RedisSocket socket, RedisSettings settings, out bool success)
        {
            var queue = m_WatchQ;
            if (queue != null && queue.Count > 0)
            {
                RedisParam key;
                if (queue.TryDequeue(out key))
                {
                    var watchCommand = new RedisCommand(DbIndex, RedisCommands.Watch,
                                                        RedisCommandType.SendAndReceive, queue.ToArray().ToBytesArray());
                    var watchResult = watchCommand.ExpectSimpleString(socket, settings, RedisConstants.OK);

                    if (!watchResult)
                    {
                        success = false;
                        return;
                    }
                }
            }

            var multiCommand = new RedisCommand(DbIndex, RedisCommands.Multi);
            var multiResult = multiCommand.ExpectSimpleString(socket, settings, RedisConstants.OK);

            success = multiResult;
            if (!success)
            {
                Cancel(requests);
                return;
            }

            success = Process(requests, socket, settings);
            if (!success)
            {
                Discard(requests, socket, settings);
                return;
            }

            success = Exec(requests, socket, settings);
        }

        protected override RedisBatchRequest<T> CreateRequest<T>(RedisCommand command, RedisCommandExpect expectation, string okIf)
        {
            return new RedisTransactionalRequest<T>(command, expectation, okIf);
        }

        private bool Process(IList<RedisRequest> requests, RedisSocket socket, RedisSettings settings)
        {
            if (requests != null)
            {
                var requestCount = requests.Count;
                if (requestCount > 0)
                {
                    settings = settings ?? RedisSettings.Default;

                    for (var i = 0; i < requestCount; i++)
                    {
                        try
                        {
                            var request = requests[i];
                            request.Process(socket, settings);

                            if (!request.IsStarted)
                            {
                                Discard(requests, socket, settings);
                                return false;
                            }
                        }
                        catch (SocketException e)
                        {
                            Discard(requests, socket, settings, e);
                            throw new RedisFatalException(e);
                        }
                        catch (Exception e)
                        {
                            Discard(requests, socket, settings, e);
                            throw;
                        }
                    }
                    return true;
                }
            }
            return false;
        }

        protected override void Discard(IList<RedisRequest> requests, RedisSocket socket, RedisSettings settings, Exception exception = null)
        {
            try
            {
                base.Discard(requests, socket, settings, exception);
            }
            finally
            {
                if (socket.IsConnected())
                    (new RedisCommand(DbIndex, RedisCommands.Discard)).ExpectSimpleString(socket, settings, RedisConstants.OK);
            }
        }

        private bool Exec(IList<RedisRequest> requests, RedisSocket socket, RedisSettings settings)
        {
            if (requests != null)
            {
                var exec = new RedisCommand(DbIndex, RedisCommands.Exec);

                settings = settings ?? RedisSettings.Default;

                var execResult = exec.ExpectArray(socket, settings);
                if (!ReferenceEquals(execResult, null))
                    return ProcessResult(requests, execResult.Value);
            }
            return false;
        }

        protected virtual bool ProcessResult(IList<RedisRequest> requests, RedisRawObject rawObject)
        {
            if (requests != null)
            {
                var requestCount = requests.Count;
                if (requestCount > 0)
                {
                    var itemCount = 0;
                    IList<RedisRawObject> items = null;

                    if (!ReferenceEquals(rawObject, null))
                    {
                        items = rawObject.Items;
                        if (items != null)
                            itemCount = items.Count;
                        else
                        {
                            ProcessRequest(requests[0], rawObject);
                            Cancel(requests, 1);

                            return true;
                        }
                    }

                    for (var i = 0; i < requestCount; i++)
                    {
                        try
                        {
                            var request = requests[i];
                            if (ReferenceEquals(request, null))
                                continue;

                            var child = (i < itemCount) ? items[i] : null;
                            if (ReferenceEquals(child, null))
                            {
                                request.Cancel();
                                continue;
                            }

                            ProcessRequest(request, child);
                        }
                        catch (Exception e)
                        {
                            for (var j = 0; j < requestCount; j++)
                            {
                                try
                                {
                                    requests[j].SetException(e);
                                }
                                catch (Exception)
                                { }
                            }
                            throw;
                        }
                    }
                    return true;
                }
            }
            return false;
        }

        #endregion Execution Methods

        #endregion Methods
    }
}
