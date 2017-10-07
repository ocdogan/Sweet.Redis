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
using System.Net.Sockets;
using System.Text;
using System.Threading;

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
                var success = false;
                try
                {
                    var requests = Interlocked.Exchange(ref m_RequestQ, null);
                    if (requests == null)
                        return false;

                    var requestCount = requests.Count;
                    if (requestCount == 0)
                        return false;

                    using (var connection = Pool.Connect(DbIndex))
                    {
                        if (connection == null)
                        {
                            Cancel(requests);
                            return false;
                        }

                        var socket = connection.Connect();
                        if (!socket.IsConnected())
                        {
                            Cancel(requests);
                            return false;
                        }

                        try
                        {
                            OnFlush(requests, socket, connection.Settings, out success);

                            if (!success || Interlocked.Read(ref m_State) != (long)RedisBatchState.Executing)
                            {
                                success = false;
                                Discard(requests, socket, connection.Settings);
                                return false;
                            }

                            success = true;
                            return true;
                        }
                        catch (SocketException e)
                        {
                            socket.DisposeSocket();
                            throw new RedisFatalException(e);
                        }
                    }
                }
                finally
                {
                    Interlocked.Exchange(ref m_State, success ?
                                    (long)RedisBatchState.Ready :
                                    (long)RedisBatchState.Failed);
                }
            }
            return false;
        }

        protected virtual void OnBeforeFlush(IList<RedisRequest> requests, RedisSocket socket, RedisSettings settings, out bool success)
        {
            success = true;
        }

        protected virtual void OnFlush(IList<RedisRequest> requests, RedisSocket socket, RedisSettings settings, out bool success)
        {
            success = true;
        }

        protected virtual void OnAfterFlush(IList<RedisRequest> requests, RedisSocket socket, RedisSettings settings, out bool success)
        {
            success = true;
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
                    Interlocked.CompareExchange(ref m_State, (long)RedisBatchState.Ready,
                        (long)RedisBatchState.WaitingCommit);
                }
                return true;
            }
            return false;
        }

        protected virtual void Discard(IList<RedisRequest> requests, RedisSocket socket, RedisSettings settings, Exception exception = null)
        {
            if (requests != null)
            {
                var requestCount = requests.Count;
                for (var i = 0; i < requestCount; i++)
                {
                    try
                    {
                        var request = requests[i];
                        if (request != null)
                        {
                            if (exception == null)
                                request.Cancel();
                            else
                                request.SetException(exception);
                        }
                    }
                    catch (Exception)
                    { }
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
                    for (var i = Math.Max(0, start); i < count; i++)
                    {
                        try
                        {
                            var request = requests[i];
                            if (request != null)
                                request.Cancel();
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
                    for (var i = 0; i < count; i++)
                    {
                        try
                        {
                            var request = requests[i];
                            if (request != null)
                                request.SetException(exception);
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

            var request = CreateRequest<T>(command, expectation, okIf);
            requests.Add(request);

            var result = request.Result;
            return !ReferenceEquals(result, null) ? (T)(object)result : default(T);
        }

        protected virtual RedisBatchRequest<T> CreateRequest<T>(RedisCommand command, RedisCommandExpect expectation, string okIf)
            where T : RedisResult
        {
            return new RedisBatchRequest<T>(command, expectation, okIf);
        }

        protected void SetWaitingCommit()
        {
            var currentState = (RedisBatchState)Interlocked.CompareExchange(ref m_State, (long)RedisBatchState.WaitingCommit,
                (long)RedisBatchState.Ready);

            if (currentState == RedisBatchState.Executing)
                throw new RedisFatalException("Can not expect any command while executing");
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

        protected virtual void ProcessRequest(RedisRequest request, RedisRawObject rawObj)
        {
            if (!ReferenceEquals(rawObj, null))
            {
                var data = rawObj.Data;
                switch (request.Expectation)
                {
                    case RedisCommandExpect.BulkString:
                        {
                            var str = ReferenceEquals(data, null) ? null :
                                (data is byte[] ? Encoding.UTF8.GetString((byte[])data) : data.ToString());

                            request.SetResult(str);
                        }
                        break;
                    case RedisCommandExpect.BulkStringBytes:
                        {
                            data = ReferenceEquals(data, null) ? null :
                                (data is string ? Encoding.UTF8.GetBytes((string)data) : data);

                            request.SetResult(data);
                        }
                        break;
                    case RedisCommandExpect.SimpleString:
                        {
                            var str = ReferenceEquals(data, null) ? null :
                                (data is byte[] ? Encoding.UTF8.GetString((byte[])data) : data.ToString());

                            if (String.IsNullOrEmpty(request.OKIf))
                                request.SetResult(str);
                            else
                                request.SetResult(String.Equals(request.OKIf, str));
                        }
                        break;
                    case RedisCommandExpect.SimpleStringBytes:
                        {
                            data = ReferenceEquals(data, null) ? null :
                                (data is string ? Encoding.UTF8.GetBytes((string)data) : data);

                            if (String.IsNullOrEmpty(request.OKIf))
                                request.SetResult(data);
                            else
                                request.SetResult(Encoding.UTF8.GetBytes(request.OKIf).Equals(data));
                        }
                        break;
                    case RedisCommandExpect.OK:
                        request.SetResult(RedisConstants.OK.Equals(data));
                        break;
                    case RedisCommandExpect.One:
                        request.SetResult(RedisConstants.One.Equals(data));
                        break;
                    case RedisCommandExpect.GreaterThanZero:
                        request.SetResult(RedisConstants.Zero.CompareTo(data) == -1);
                        break;
                    case RedisCommandExpect.Nothing:
                        request.SetResult(RedisVoidVal.Value);
                        break;
                    default:
                        request.SetResult(data);
                        break;
                }
            }
        }

        #endregion Execution Methods

        #endregion Methods
    }
}
