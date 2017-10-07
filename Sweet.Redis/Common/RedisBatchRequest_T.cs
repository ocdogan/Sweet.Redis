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
                throw new RedisException("Request type must be one of batch types");
        }

        #endregion .Ctors

        #region Methods

        protected override void ProcessInternal(RedisSocket socket, RedisSettings settings)
        {
            try
            {
                if (socket == null || socket.Disposed || !socket.IsConnected())
                    Interlocked.Exchange(ref m_State, (long)RequestState.Canceled);
                else
                {
                    var command = Command;
                    if (command == null || command.Disposed)
                    {
                        Interlocked.Exchange(ref m_State, (long)RequestState.Canceled);
                        return;
                    }

                    if (IsTransactional)
                    {
                        var queueResult = command.ExpectSimpleString(socket, settings, RedisConstants.QUEUED);
                        if (!queueResult)
                            throw new RedisException("An error occured in transaction queue");
                        return;
                    }

                    var result = CreateResult();

                    switch (Expectation)
                    {
                        case RedisCommandExpect.Response:
                            {
                                var expectation = command.Execute(socket, settings);
                                (result as RedisResponse).TrySetResult(expectation);
                            }
                            break;
                        case RedisCommandExpect.Array:
                            {
                                var expectation = command.ExpectArray(socket, settings);
                                (result as RedisRaw).TrySetResult(expectation.Value);
                            }
                            break;
                        case RedisCommandExpect.BulkString:
                            {
                                var expectation = command.ExpectBulkString(socket, settings);
                                (result as RedisString).TrySetResult(expectation.Value);
                            }
                            break;
                        case RedisCommandExpect.BulkStringBytes:
                            {
                                var expectation = command.ExpectBulkStringBytes(socket, settings);
                                (result as RedisBytes).TrySetResult(expectation.Value);
                            }
                            break;
                        case RedisCommandExpect.Double:
                            {
                                var expectation = command.ExpectDouble(socket, settings);
                                (result as RedisDouble).TrySetResult(expectation.Value);
                            }
                            break;
                        case RedisCommandExpect.GreaterThanZero:
                            {
                                var expectation = command.ExpectInteger(socket, settings);
                                (result as RedisBool).TrySetResult(expectation.Value > RedisConstants.Zero);
                            }
                            break;
                        case RedisCommandExpect.Integer:
                            {
                                var expectation = command.ExpectInteger(socket, settings);
                                (result as RedisInteger).TrySetResult(expectation.Value);
                            }
                            break;
                        case RedisCommandExpect.MultiDataBytes:
                            {
                                var expectation = command.ExpectMultiDataBytes(socket, settings);
                                (result as RedisMultiBytes).TrySetResult(expectation.Value);
                            }
                            break;
                        case RedisCommandExpect.MultiDataStrings:
                            {
                                var expectation = command.ExpectMultiDataStrings(socket, settings);
                                (result as RedisMultiString).TrySetResult(expectation.Value);
                            }
                            break;
                        case RedisCommandExpect.Nothing:
                            {
                                var expectation = command.ExpectNothing(socket, settings);
                                (result as RedisVoid).TrySetResult(expectation.Value);
                            }
                            break;
                        case RedisCommandExpect.NullableDouble:
                            {
                                var expectation = command.ExpectNullableDouble(socket, settings);
                                (result as RedisNullableDouble).TrySetResult(expectation.Value);
                            }
                            break;
                        case RedisCommandExpect.NullableInteger:
                            {
                                var expectation = command.ExpectNullableInteger(socket, settings);
                                (result as RedisNullableInteger).TrySetResult(expectation.Value);
                            }
                            break;
                        case RedisCommandExpect.OK:
                            {
                                var expectation = command.ExpectSimpleString(socket, settings, RedisConstants.OK);
                                (result as RedisBool).TrySetResult(expectation.Value);
                            }
                            break;
                        case RedisCommandExpect.One:
                            {
                                var expectation = command.ExpectInteger(socket, settings);
                                (result as RedisBool).TrySetResult(expectation.Value == RedisConstants.One);
                            }
                            break;
                        case RedisCommandExpect.SimpleString:
                            {
                                var expectation = command.ExpectSimpleString(socket, settings);
                                (result as RedisString).TrySetResult(expectation.Value);
                            }
                            break;
                        case RedisCommandExpect.SimpleStringBytes:
                            {
                                var expectation = command.ExpectSimpleStringBytes(socket, settings);
                                (result as RedisBytes).TrySetResult(expectation.Value);
                            }
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
