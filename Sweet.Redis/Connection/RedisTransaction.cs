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
        private ConcurrentQueue<RedisRequest> m_RequestQ;

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
            Interlocked.Exchange(ref m_RequestQ, null);           
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
                try
                {
                    throw new NotImplementedException();
                }
                finally
                {
                    Interlocked.Exchange(ref m_State, (long)RedisTransactionState.Empty);
                }
            }
            return false;
        }

        protected internal override T Expect<T>(RedisCommand command, RedisCommandExpect expectation, string okIf = null)
        {
            var state = (RedisTransactionState)Interlocked.CompareExchange(ref m_State, (long)RedisTransactionState.Ready, 
                (long)RedisTransactionState.Empty);

            if (state == RedisTransactionState.Executing)
                throw new RedisException("Can not expect any command while executing");

            var queue = m_RequestQ;
            if (queue == null)
                queue = m_RequestQ = new ConcurrentQueue<RedisRequest>();

            var request = new RedisRequest<T>(command, expectation, okIf);
            queue.Enqueue(request);

            var result = request.Result;
            return (result != null ? result.Value : default(T));
        }

        #endregion Execution Methods

        #endregion Methods
    }
}
