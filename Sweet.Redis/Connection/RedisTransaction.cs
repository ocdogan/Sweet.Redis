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
    internal class RedisTransaction : RedisPipelineBase, IRedisTransaction
    {
        #region .Ctors

        public RedisTransaction(RedisConnectionPool pool, int db, bool throwOnError = true)
            : base(pool, db, throwOnError)
        { }

        #endregion .Ctors

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

        protected override void OnFlush(IList<RedisRequest> requests, RedisSocket socket, RedisSettings settings, out bool success)
        {
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
                    catch (Exception e)
                    {
                        Discard(requests, socket, settings, e);
                        throw;
                    }
                }
                return true;
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
                var execResult = exec.ExpectArray(socket, settings);

                var itemCount = 0;
                IList<RedisRawObject> items = null;

                if (execResult != null)
                {
                    var raw = execResult.Value;
                    if (raw != null)
                    {
                        items = raw.Items;
                        if (items != null)
                            itemCount = items.Count;
                    }
                }

                var requestCount = requests.Count;
                if (itemCount != requestCount)
                {
                    Cancel(requests);
                    return false;
                }

                for (var i = 0; i < requestCount; i++)
                {
                    try
                    {
                        var request = requests[i];
                        if (ReferenceEquals(request, null))
                            continue;

                        if (i >= itemCount)
                        {
                            request.Cancel();
                            continue;
                        }

                        var child = items[i];
                        if (ReferenceEquals(child, null))
                        {
                            request.Cancel();
                            continue;
                        }

                        var data = child.Data;
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
            return false;
        }

        #endregion Execution Methods

        #endregion Methods
    }
}
