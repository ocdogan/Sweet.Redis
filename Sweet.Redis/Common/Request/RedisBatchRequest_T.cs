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
using System.Threading;

namespace Sweet.Redis
{
    internal class RedisBatchRequest<T> : RedisRequest<T>
        where T : RedisResult
    {
        #region .Ctors

        public RedisBatchRequest(RedisCommand command, RedisCommandExpect expectation,
                string okIf = null, RedisRequestType requestType = RedisRequestType.Batch)
            : base(command, expectation, okIf, requestType)
        {
            if (requestType - RedisConstants.RedisBatchBase < 0)
                throw new RedisException("Request type must be one of batch types", RedisErrorCode.InvalidParameter);
        }

        #endregion .Ctors

        #region Methods

        protected override void ProcessInternal(RedisSocketContext context)
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

                    if (IsTransactional)
                    {
                        var queueResult = command.ExpectSimpleString(context, RedisConstants.QUEUED);
                        if (!queueResult)
                            throw new RedisException("An error occured in transaction queue", RedisErrorCode.CorruptResponse);
                        return;
                    }

                    var result = CreateResult();

                    switch (Expectation)
                    {
                        case RedisCommandExpect.Response:
                            {
                                var expectation = command.Execute(context);
                                (result as RedisResponse).TrySetResult(expectation);
                            }
                            break;
                        case RedisCommandExpect.Array:
                            {
                                var expectation = command.ExpectArray(context);
                                (result as RedisRaw).TrySetResult(expectation.Value);
                            }
                            break;
                        case RedisCommandExpect.BulkString:
                            {
                                var expectation = command.ExpectBulkString(context);
                                (result as RedisString).TrySetResult(expectation.Value);
                            }
                            break;
                        case RedisCommandExpect.BulkStringBytes:
                            {
                                var expectation = command.ExpectBulkStringBytes(context);
                                (result as RedisBytes).TrySetResult(expectation.Value);
                            }
                            break;
                        case RedisCommandExpect.Double:
                            {
                                var expectation = command.ExpectDouble(context);
                                (result as RedisDouble).TrySetResult(expectation.Value);
                            }
                            break;
                        case RedisCommandExpect.GreaterThanZero:
                            {
                                var expectation = command.ExpectInteger(context);
                                (result as RedisBool).TrySetResult(expectation.Value > RedisConstants.Zero);
                            }
                            break;
                        case RedisCommandExpect.Integer:
                            {
                                var expectation = command.ExpectInteger(context);
                                (result as RedisInteger).TrySetResult(expectation.Value);
                            }
                            break;
                        case RedisCommandExpect.MultiDataBytes:
                            {
                                var expectation = command.ExpectMultiDataBytes(context);
                                (result as RedisMultiBytes).TrySetResult(expectation.Value);
                            }
                            break;
                        case RedisCommandExpect.MultiDataStrings:
                            {
                                var expectation = command.ExpectMultiDataStrings(context);
                                (result as RedisMultiString).TrySetResult(expectation.Value);
                            }
                            break;
                        case RedisCommandExpect.Nothing:
                            {
                                var expectation = command.ExpectNothing(context);
                                (result as RedisVoid).TrySetResult(expectation.Value);
                            }
                            break;
                        case RedisCommandExpect.NullableDouble:
                            {
                                var expectation = command.ExpectNullableDouble(context);
                                (result as RedisNullableDouble).TrySetResult(expectation.Value);
                            }
                            break;
                        case RedisCommandExpect.NullableInteger:
                            {
                                var expectation = command.ExpectNullableInteger(context);
                                (result as RedisNullableInteger).TrySetResult(expectation.Value);
                            }
                            break;
                        case RedisCommandExpect.OK:
                            {
                                var expectation = command.ExpectSimpleString(context, RedisConstants.OK);
                                (result as RedisBool).TrySetResult(expectation.Value);
                            }
                            break;
                        case RedisCommandExpect.One:
                            {
                                var expectation = command.ExpectInteger(context);
                                (result as RedisBool).TrySetResult(expectation.Value == RedisConstants.One);
                            }
                            break;
                        case RedisCommandExpect.SimpleString:
                            {
                                var expectation = command.ExpectSimpleString(context);
                                (result as RedisString).TrySetResult(expectation.Value);
                            }
                            break;
                        case RedisCommandExpect.SimpleStringBytes:
                            {
                                var expectation = command.ExpectSimpleStringBytes(context);
                                (result as RedisBytes).TrySetResult(expectation.Value);
                            }
                            break;
                        default:
                            break;
                    }

                    Interlocked.CompareExchange(ref m_State, (long)RequestState.Completed, (long)RequestState.Initiated);
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
