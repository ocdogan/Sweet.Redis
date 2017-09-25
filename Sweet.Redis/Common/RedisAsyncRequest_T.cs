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
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Sweet.Redis
{
    internal class RedisAsyncRequest<T> : RedisAsyncRequest
    {
        #region Constants

        private const int MaxTimeout = 60 * 1000;

        #endregion Constants

        #region .Ctors

        public RedisAsyncRequest(RedisCommand command, RedisCommandExpect expectation,
            string okFor, TaskCompletionSource<T> completionSource)
            : base(command, expectation, okFor, completionSource)
        { }

        #endregion .Ctors

        #region Properties

        public TaskCompletionSource<T> CompletionSource
        {
            get { return (TaskCompletionSource<T>)StateObject; }
        }

        public override bool IsCompleted
        {
            get
            {
                var tcs = (TaskCompletionSource<T>)StateObject;
                if (tcs != null)
                {
                    var task = tcs.Task;
                    return (task == null) || task.IsCompleted || task.IsCanceled || task.IsFaulted;
                }
                return true;
            }
        }

        public Task<T> Task
        {
            get
            {
                var tcs = (TaskCompletionSource<T>)StateObject;
                if (tcs != null)
                    return tcs.Task;
                return null;
            }
        }

        #endregion Properties

        #region Methods

        public override void Cancel()
        {
            ValidateNotDisposed();

            var tcs = CompletionSource;
            if (tcs != null)
            {
                var task = tcs.Task;
                if (task != null && !task.IsCompleted)
                {
                    tcs.TrySetCanceled();
                }
            }
        }

        public override bool Expire(int timeoutMilliseconds = -1)
        {
            ValidateNotDisposed();

            if (timeoutMilliseconds > -1)
            {
                var tcs = CompletionSource;
                if (tcs != null)
                {
                    var task = tcs.Task;
                    if (task != null)
                    {
                        timeoutMilliseconds = Math.Min(timeoutMilliseconds, MaxTimeout);

                        if (!task.IsCompleted && 
                            (DateTime.UtcNow - CreationTime).TotalMilliseconds >= timeoutMilliseconds)
                        {
                            tcs.TrySetException(new RedisException("Request Timeout"));
                            return true;
                        }
                    }
                }
            }
            return false;
        }

        public override void Process(IRedisConnection connection, int timeoutMilliseconds = -1)
        {
            ValidateNotDisposed();

            var tcs = CompletionSource;
            if (tcs != null)
            {
                try
                {
                    var task = tcs.Task;
                    if (task != null &&
                        !(task.IsCompleted || task.IsFaulted || task.IsCanceled))
                    {
                        if (timeoutMilliseconds > -1)
                        {
                            timeoutMilliseconds = Math.Min(timeoutMilliseconds, MaxTimeout);
                            if ((DateTime.UtcNow - CreationTime).TotalMilliseconds >= timeoutMilliseconds)
                            {
                                tcs.TrySetException(new RedisException("Request Timeout"));
                                return;
                            }
                        }

                        var command = Command;
                        if (command == null || connection == null || connection.Disposed)
                            tcs.TrySetCanceled();
                        else
                        {
                            switch (Expectation)
                            {
                                case RedisCommandExpect.Response:
                                    {
                                        var result = command.Execute(connection);
                                        (tcs as TaskCompletionSource<IRedisResponse>).TrySetResult(result);
                                    }
                                    break;
                                case RedisCommandExpect.Array:
                                    {
                                        var result = command.ExpectArray(connection);
                                        (tcs as TaskCompletionSource<RedisRaw>).TrySetResult(result);
                                    }
                                    break;
                                case RedisCommandExpect.BulkString:
                                    {
                                        var result = command.ExpectBulkString(connection);
                                        (tcs as TaskCompletionSource<RedisString>).TrySetResult(result);
                                    }
                                    break;
                                case RedisCommandExpect.BulkStringBytes:
                                    {
                                        var result = command.ExpectBulkStringBytes(connection);
                                        (tcs as TaskCompletionSource<RedisBytes>).TrySetResult(result);
                                    }
                                    break;
                                case RedisCommandExpect.Double:
                                    {
                                        var result = command.ExpectDouble(connection);
                                        (tcs as TaskCompletionSource<RedisDouble>).TrySetResult(result);
                                    }
                                    break;
                                case RedisCommandExpect.GreaterThanZero:
                                    {
                                        var result = command.ExpectInteger(connection);
                                        (tcs as TaskCompletionSource<RedisBool>).TrySetResult(result > RedisConstants.Zero);
                                    }
                                    break;
                                case RedisCommandExpect.Integer:
                                    {
                                        var result = command.ExpectInteger(connection);
                                        (tcs as TaskCompletionSource<RedisInt>).TrySetResult(result);
                                    }
                                    break;
                                case RedisCommandExpect.MultiDataBytes:
                                    {
                                        var result = command.ExpectMultiDataBytes(connection);
                                        (tcs as TaskCompletionSource<RedisMultiBytes>).TrySetResult(result);
                                    }
                                    break;
                                case RedisCommandExpect.MultiDataStrings:
                                    {
                                        var result = command.ExpectMultiDataStrings(connection);
                                        (tcs as TaskCompletionSource<RedisMultiString>).TrySetResult(result);
                                    }
                                    break;
                                case RedisCommandExpect.NullableDouble:
                                    {
                                        var result = command.ExpectNullableDouble(connection);
                                        (tcs as TaskCompletionSource<RedisNullableDouble>).TrySetResult(result);
                                    }
                                    break;
                                case RedisCommandExpect.NullableInteger:
                                    {
                                        var result = command.ExpectNullableInteger(connection);
                                        (tcs as TaskCompletionSource<RedisNullableInt>).TrySetResult(result);
                                    }
                                    break;
                                case RedisCommandExpect.OK:
                                    {
                                        var result = command.ExpectSimpleString(connection, RedisConstants.OK);
                                        (tcs as TaskCompletionSource<RedisBool>).TrySetResult(result);
                                    }
                                    break;
                                case RedisCommandExpect.One:
                                    {
                                        var result = command.ExpectInteger(connection);
                                        (tcs as TaskCompletionSource<RedisBool>).TrySetResult(result == RedisConstants.One);
                                    }
                                    break;
                                case RedisCommandExpect.SimpleString:
                                    {
                                        var result = command.ExpectSimpleString(connection);
                                        (tcs as TaskCompletionSource<RedisString>).TrySetResult(result);
                                    }
                                    break;
                                case RedisCommandExpect.SimpleStringBytes:
                                    {
                                        var result = command.ExpectSimpleStringBytes(connection);
                                        (tcs as TaskCompletionSource<RedisBytes>).TrySetResult(result);
                                    }
                                    break;
                            }
                        }
                    }
                }
                catch (Exception e)
                {
                    tcs.TrySetException(e);
                }
            }
        }

        #endregion Methods
    }
}
