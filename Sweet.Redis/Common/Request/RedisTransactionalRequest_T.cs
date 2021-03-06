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
using System.Threading;

namespace Sweet.Redis
{
    internal class RedisTransactionalRequest<T> : RedisBatchRequest<T>
        where T : RedisResult
    {
        #region .Ctors

        public RedisTransactionalRequest(RedisCommand command, RedisCommandExpect expectation, string okIf = null)
            : base(command, expectation, okIf, RedisRequestType.Transactional)
        { }

        #endregion .Ctors

        #region Methods

        protected override void ProcessInternal(RedisSocketContext context, int timeoutMilliseconds = -1)
        {
            try
            {
                if (context == null || !context.Socket.IsConnected())
                    Interlocked.Exchange(ref m_State, (long)RequestState.Canceled);
                else
                {
                    var command = Command;
                    if (!command.IsAlive())
                    {
                        Interlocked.Exchange(ref m_State, (long)RequestState.Canceled);
                        return;
                    }

                    var queueResult = command.ExpectSimpleString(context, RedisConstants.QUEUED);
                    if (!queueResult)
                        throw new RedisException("An error occured in transaction queue", RedisErrorCode.CorruptResponse);
                }
            }
            catch (Exception e)
            {
                SetException(e);
            }
        }

        #endregion Methods
    }
}
