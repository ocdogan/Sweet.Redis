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

namespace Sweet.Redis
{
    internal class RedisPipeline : RedisBatch, IRedisPipeline
    {
        #region .Ctors

        public RedisPipeline(RedisConnectionPool pool, int dbIndex, bool throwOnError = true)
            : base(pool, dbIndex, throwOnError)
        { }

        #endregion .Ctors

        #region Methods

        #region Execution Methods

        public bool Execute()
        {
            return Flush();
        }

        public bool Cancel()
        {
            return Rollback();
        }

        protected override RedisBatchRequest<T> CreateRequest<T>(RedisCommand command, RedisCommandExpect expectation, string okIf)
        {
            return new RedisPipelineRequest<T>(command, expectation, okIf);
        }

        protected override void OnFlush(IList<RedisRequest> requests, RedisSocket socket, RedisSettings settings, out bool success)
        {
            settings = settings ?? RedisSettings.Default;

            success = Send(requests, socket, settings);
            if (success && socket.IsConnected())
                success = Receive(requests, socket, settings);
        }

        private bool Send(IList<RedisRequest> requests, RedisSocket socket, RedisSettings settings)
        {
            if (requests != null)
            {
                var requestCount = requests.Count;
                if (requests.Count > 0 && socket.IsConnected())
                {
                    var anySend = false;
                    var stream = socket.GetBufferedStream();
                    try
                    {
                        for (var i = 0; i < requestCount; i++)
                        {
                            try
                            {
                                var request = requests[i];
                                request.Command.WriteTo(stream, false);

                                anySend = true;
                            }
                            catch (Exception)
                            {
                                Cancel(requests, i);
                                break;
                            }
                        }
                    }
                    finally
                    {
                        if (anySend)
                            stream.Flush();
                    }
                    return anySend;
                }
            }
            return false;
        }

        private bool Receive(IList<RedisRequest> requests, RedisSocket socket, RedisSettings settings)
        {
            if (requests != null)
            {
                var requestCount = requests.Count;
                if (requestCount > 0)
                {
                    using (var reader = new RedisSingleResponseReader(settings))
                    {
                        for (var i = 0; i < requestCount; i++)
                        {
                            try
                            {
                                var request = requests[i];
                                if (ReferenceEquals(request, null))
                                    continue;

                                var execResult = reader.Execute(socket);
                                if (ReferenceEquals(execResult, null))
                                    throw new RedisFatalException("Corrupted redis response data");

                                execResult.HandleError();

                                var rawObj = RedisRawObject.ToObject(execResult);
                                if (ReferenceEquals(rawObj, null))
                                    throw new RedisFatalException("Corrupted redis response data");

                                ProcessRequest(request, rawObj);
                            }
                            catch (Exception e)
                            {
                                for (var j = i; j < requestCount; j++)
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
