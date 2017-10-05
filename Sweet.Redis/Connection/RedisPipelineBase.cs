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
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Sweet.Redis
{
    internal class RedisPipelineBase : RedisDb
    {
        #region Field Members

        protected long m_State;
        protected List<RedisRequest> m_RequestQ;

        #endregion Field Members

        #region .Ctors

        public RedisPipelineBase(RedisConnectionPool pool, int db, bool throwOnError = true)
            : base(pool, db, throwOnError)
        { }

        #endregion .Ctors

        #region Destructors

        protected override void OnDispose(bool disposing)
        {
            Interlocked.Exchange(ref m_State, (long)RedisBatchState.Disposed);

            base.OnDispose(disposing);

            var requests = Interlocked.Exchange(ref m_RequestQ, null);
            Cancel(requests);
        }

        #endregion Destructors

        #region Properties

        public RedisBatchState Status
        {
            get { return (RedisBatchState)Interlocked.Read(ref m_State); }
        }

        #endregion Properties

        #region Methods

        #region Execution Methods

        protected virtual bool Flush()
        {
            ValidateNotDisposed();

            if (Interlocked.CompareExchange(ref m_State, (long)RedisBatchState.Executing,
                (long)RedisBatchState.WaitingCommit) == (long)RedisBatchState.WaitingCommit)
            {
                var innerState = RedisBatchState.Initialized;
                try
                {
                    var requests = Interlocked.Exchange(ref m_RequestQ, null);
                    if (requests == null)
                    {
                        innerState = RedisBatchState.Initialized;
                        return false;
                    }

                    var requestCount = requests.Count;
                    if (requestCount == 0)
                    {
                        innerState = RedisBatchState.Initialized;
                        return false;
                    }

                    using (var connection = Pool.Connect(DbIndex))
                    {
                        if (connection == null)
                        {
                            innerState = RedisBatchState.Failed;
                            Cancel(requests);
                            return false;
                        }

                        var socket = connection.Connect();
                        if (!socket.IsConnected())
                        {
                            innerState = RedisBatchState.Failed;
                            Cancel(requests);
                            return false;
                        }

                        var settings = connection.Settings;

                        bool processNextChain;
                        OnBeforeFlush(requests, socket, settings, out processNextChain);

                        if (!processNextChain)
                        {
                            innerState = RedisBatchState.Failed;
                            Cancel(requests);

                            return false;
                        }

                        innerState = RedisBatchState.Executing;
                        OnFlush(requests, socket, settings, out processNextChain);

                        if (!processNextChain || Interlocked.Read(ref m_State) != (long)RedisBatchState.Executing)
                        {
                            innerState = RedisBatchState.Failed;
                            Discard(requests, socket, settings);
                            return false;
                        }

                        OnAfterFlush(requests, socket, settings, out processNextChain);
                        if (!processNextChain)
                        {
                            innerState = RedisBatchState.Failed;
                            Discard(requests, socket, settings);
                            return false;
                        }

                        innerState = RedisBatchState.Initialized;
                        return true;
                    }
                }
                finally
                {
                    Interlocked.Exchange(ref m_State, innerState == RedisBatchState.Initialized ?
                                         (long)RedisBatchState.Initialized :
                                         (long)RedisBatchState.Failed);
                }
            }
            return false;
        }

        protected virtual void OnBeforeFlush(IList<RedisRequest> requests, RedisSocket socket, RedisSettings settings, out bool processNextChain)
        {
            processNextChain = true;
        }

        protected virtual void OnFlush(IList<RedisRequest> requests, RedisSocket socket, RedisSettings settings, out bool processNextChain)
        {
            processNextChain = true;
        }

        protected virtual void OnAfterFlush(IList<RedisRequest> requests, RedisSocket socket, RedisSettings settings, out bool processNextChain)
        {
            processNextChain = true;
        }
        
        protected virtual bool Rollback()
        {
            ValidateNotDisposed();

            if (Interlocked.CompareExchange(ref m_State, (long)RedisBatchState.Executing,
                (long)RedisBatchState.WaitingCommit) == (long)RedisBatchState.WaitingCommit)
            {
                var requests = Interlocked.Exchange(ref m_RequestQ, null);
                try
                {
                    Cancel(requests);
                }
                catch (Exception e)
                {
                    Interlocked.Exchange(ref m_State, (long)RedisBatchState.Failed);
                    SetException(requests, e);
                }
                finally
                {
                    Interlocked.CompareExchange(ref m_State, (long)RedisBatchState.Initialized,
                        (long)RedisBatchState.WaitingCommit);
                }
                return true;
            }
            return false;
        }

        protected virtual void Discard(IList<RedisRequest> requests, RedisSocket socket, RedisSettings settings, Exception exception = null)
        {
            try
            {
                if (requests != null)
                {
                    var requestCount = requests.Count;
                    for (var j = 0; j < requestCount; j++)
                    {
                        try
                        {
                            if (exception == null)
                                requests[j].Cancel();
                            else
                                requests[j].SetException(exception);
                        }
                        catch (Exception)
                        { }
                    }
                }
            }
            finally
            {
                if (socket.IsConnected())
                {
                    var discardCommand = new RedisCommand(DbIndex, RedisCommands.Discard);
                    discardCommand.ExpectSimpleString(socket, settings, RedisConstants.OK);
                }
            }
        }

        protected static void Cancel(IList<RedisRequest> requests, int start = 0)
        {
            if (requests != null)
            {
                var count = requests.Count;
                if (count > 0)
                {
                    for (var j = Math.Max(0, start); j < count; j++)
                    {
                        try
                        {
                            requests[j].Cancel();
                        }
                        catch (Exception)
                        { }
                    }
                }
            }
        }

        protected static void SetException(IList<RedisRequest> requests, Exception exception)
        {
            if (exception != null && requests != null)
            {
                var count = requests.Count;
                if (count > 0)
                {
                    for (var j = 0; j < count; j++)
                    {
                        try
                        {
                            requests[j].SetException(exception);
                        }
                        catch (Exception)
                        { }
                    }
                }
            }
        }

        protected internal override T Expect<T>(RedisCommand command, RedisCommandExpect expectation, string okIf = null)
        {
            SetWaitingCommit();

            var requests = m_RequestQ;
            if (requests == null)
                requests = m_RequestQ = new List<RedisRequest>();

            var request = new RedisBatchRequest<T>(command, expectation, okIf);
            requests.Add(request);

            var result = request.Result;
            return !ReferenceEquals(result, null) ? (T)(object)result : default(T);
        }

        protected void SetWaitingCommit()
        {
            var currentState = (RedisBatchState)Interlocked.CompareExchange(ref m_State, (long)RedisBatchState.WaitingCommit,
                (long)RedisBatchState.Initialized);

            if (currentState == RedisBatchState.Executing)
                throw new RedisException("Can not expect any command while executing");
        }

        #endregion Execution Methods

        #endregion Methods
    }
}
