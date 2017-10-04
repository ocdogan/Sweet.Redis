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
    internal class RedisTransaction : RedisDb, IRedisTransaction
    {
        #region Field Members

        private long m_State;
        private List<RedisRequest> m_RequestList;

        #endregion Field Members

        #region .Ctors

        public RedisTransaction(RedisConnectionPool pool, int db, bool throwOnError = true)
            : base(pool, db, throwOnError)
        { }

        #endregion .Ctors

        #region Destructors

        protected override void OnDispose(bool disposing)
        {
            Interlocked.Exchange(ref m_State, (long)RedisTransactionState.Disposed);

            base.OnDispose(disposing);

            var requests = Interlocked.Exchange(ref m_RequestList, null);
            Cancel(requests);
        }

        #endregion Destructors

        #region Properties

        public RedisTransactionState Status
        {
            get { return (RedisTransactionState)Interlocked.Read(ref m_State); }
        }

        #endregion Properties

        #region Methods

        #region Execution Methods

        public bool Execute()
        {
            ValidateNotDisposed();

            if (Interlocked.CompareExchange(ref m_State, (long)RedisTransactionState.Executing,
                (long)RedisTransactionState.Ready) == (long)RedisTransactionState.Ready)
            {
                var completed = true;
                try
                {
                    var requests = Interlocked.Exchange(ref m_RequestList, null);
                    if (requests != null)
                    {
                        var count = requests.Count;
                        if (count > 0)
                        {
                            using (var connection = Pool.Connect(DbIndex))
                            {
                                if (connection == null)
                                {
                                    completed = false;
                                    Cancel(requests);
                                }
                                else
                                {
                                    var socket = connection.Connect();
                                    if (!socket.IsConnected())
                                    {
                                        completed = false;
                                        Cancel(requests);
                                    }
                                    else
                                    {
                                        var settings = connection.Settings;

                                        for (var i = 0; i < count; i++)
                                        {
                                            try
                                            {
                                                var request = requests[i];

                                                request.Process(socket, settings);
                                                if (!request.IsCompleted)
                                                {
                                                    completed = false;
                                                    Cancel(requests, i);
                                                    break;
                                                }
                                            }
                                            catch (Exception e)
                                            {
                                                for (var j = 0; j < count; j++)
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

                                        return completed;
                                    }
                                }
                            }
                        }
                    }
                }
                finally
                {
                    Interlocked.Exchange(ref m_State, completed ? (long)RedisTransactionState.Initiated : (long)RedisTransactionState.Failed);
                }
            }
            return false;
        }

        private static void Cancel(IList<RedisRequest> requests, int start = 0)
        {
            if (requests != null)
            {
                var count = requests.Count;
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

        protected internal override T Expect<T>(RedisCommand command, RedisCommandExpect expectation, string okIf = null)
        {
            SetReady();

            var requests = m_RequestList;
            if (requests == null)
                requests = m_RequestList = new List<RedisRequest>();

            var request = new RedisRequest<T>(command, expectation, okIf);
            requests.Add(request);

            var result = request.Result;
            return !ReferenceEquals(result, null) ? (T)(object)result : default(T);
        }

        private void SetReady()
        {
            var state = (RedisTransactionState)Interlocked.CompareExchange(ref m_State, (long)RedisTransactionState.Ready,
                (long)RedisTransactionState.Initiated);

            if (state == RedisTransactionState.Executing)
                throw new RedisException("Can not expect any command while executing");
        }

        #endregion Execution Methods

        #endregion Methods
    }
}
