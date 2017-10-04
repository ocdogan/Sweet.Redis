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
using System.Reflection;
using System.Threading;

namespace Sweet.Redis
{
    internal class RedisRequest<T> : RedisRequest
        where T : RedisResult
    {
        #region RequestState

        private enum RequestState : long
        {
            Waiting = 0L,
            Started = 1L,
            Completed = 2L,
            Canceled = 3L,
            Failed = 4L
        }

        #endregion RequestState

        #region Constants

        private const int MaxTimeout = 60 * 1000;

        #endregion Constants

        #region Static Members

        private static ConstructorInfo s_Ctor;

        #endregion Static Members

        #region Field Members

        private T m_Result;
        private long m_State;
        private Exception m_Exception;

        #endregion Field Members

        #region .Ctors

        public RedisRequest(RedisCommand command, RedisCommandExpect expectation, string okIf)
            : base(command, expectation, okIf, null)
        { }

        #endregion .Ctors

        #region Properties

        public override bool IsCanceled
        {
            get { return Interlocked.Read(ref m_State) == (long)RequestState.Canceled; }
        }

        public override bool IsCompleted
        {
            get
            {
                var state = (RequestState)Interlocked.Read(ref m_State);
                return !(state == RequestState.Waiting || state == RequestState.Started);
            }
        }

        public override bool IsFaulted
        {
            get { return Interlocked.Read(ref m_State) == (long)RequestState.Failed; }
        }

        public override bool IsStarted
        {
            get { return Interlocked.Read(ref m_State) == (long)RequestState.Started; }
        }

        public T Result
        {
            get { return CreateResult(); }
        }

        #endregion Properties

        #region Methods

        private T CreateResult()
        {
            var result = m_Result;
            if (result == null)
            {
                var ctor = s_Ctor;
                if (ctor == null)
                {
                    s_Ctor = typeof(T).GetConstructor(BindingFlags.Public |
                                                      BindingFlags.NonPublic |
                                                      BindingFlags.Instance, null, Type.EmptyTypes, null);
                    ctor = s_Ctor;
                }

                result = (T)ctor.Invoke(null);
                m_Result = result;
            }
            return result;
        }

        public override void Cancel()
        {
            ValidateNotDisposed();

            if (Interlocked.CompareExchange(ref m_State, (long)RequestState.Canceled, (long)RequestState.Waiting) != (long)RequestState.Waiting)
                Interlocked.CompareExchange(ref m_State, (long)RequestState.Canceled, (long)RequestState.Started);
        }

        public override void SetException(Exception exception)
        {
            if (exception != null)
            {
                ValidateNotDisposed();

                Interlocked.Exchange(ref m_State, (long)RequestState.Failed);
                Interlocked.Exchange(ref m_Exception, exception);
            }
        }

        public override void Process(IRedisConnection connection)
        {
            ValidateNotDisposed();

            try
            {
                if (Interlocked.CompareExchange(ref m_State, (long)RequestState.Started, (long)RequestState.Waiting) ==
                    (long)RequestState.Waiting)
                {
                    var command = Command;
                    if (command == null || connection == null || connection.Disposed)
                        Interlocked.Exchange(ref m_State, (long)RequestState.Canceled);
                    else
                    {
                        var result = CreateResult();

                        switch (Expectation)
                        {
                            case RedisCommandExpect.Response:
                                {
                                    var expectation = command.Execute(connection);
                                    if (Interlocked.Read(ref m_State) == (long)RequestState.Started)
                                        (result as RedisResult<IRedisRawResponse>).TrySetResult(expectation);
                                }
                                break;
                            case RedisCommandExpect.Array:
                                {
                                    var expectation = command.ExpectArray(connection);
                                    if (Interlocked.Read(ref m_State) == (long)RequestState.Started)
                                        (result as RedisRaw).TrySetResult(expectation.Value);
                                }
                                break;
                            case RedisCommandExpect.BulkString:
                                {
                                    var expectation = command.ExpectBulkString(connection);
                                    if (Interlocked.Read(ref m_State) == (long)RequestState.Started)
                                        (result as RedisString).TrySetResult(expectation.Value);
                                }
                                break;
                            case RedisCommandExpect.BulkStringBytes:
                                {
                                    var expectation = command.ExpectBulkStringBytes(connection);
                                    if (Interlocked.Read(ref m_State) == (long)RequestState.Started)
                                        (result as RedisBytes).TrySetResult(expectation.Value);
                                }
                                break;
                            case RedisCommandExpect.Double:
                                {
                                    var expectation = command.ExpectDouble(connection);
                                    if (Interlocked.Read(ref m_State) == (long)RequestState.Started)
                                        (result as RedisDouble).TrySetResult(expectation.Value);
                                }
                                break;
                            case RedisCommandExpect.GreaterThanZero:
                                {
                                    var expectation = command.ExpectInteger(connection);
                                    if (Interlocked.Read(ref m_State) == (long)RequestState.Started)
                                        (result as RedisBool).TrySetResult(expectation.Value > RedisConstants.Zero);
                                }
                                break;
                            case RedisCommandExpect.Integer:
                                {
                                    var expectation = command.ExpectInteger(connection);
                                    if (Interlocked.Read(ref m_State) == (long)RequestState.Started)
                                        (result as RedisInteger).TrySetResult(expectation.Value);
                                }
                                break;
                            case RedisCommandExpect.MultiDataBytes:
                                {
                                    var expectation = command.ExpectMultiDataBytes(connection);
                                    if (Interlocked.Read(ref m_State) == (long)RequestState.Started)
                                        (result as RedisMultiBytes).TrySetResult(expectation.Value);
                                }
                                break;
                            case RedisCommandExpect.MultiDataStrings:
                                {
                                    var expectation = command.ExpectMultiDataStrings(connection);
                                    if (Interlocked.Read(ref m_State) == (long)RequestState.Started)
                                        (result as RedisMultiString).TrySetResult(expectation.Value);
                                }
                                break;
                            case RedisCommandExpect.Nothing:
                                {
                                    var expectation = command.ExpectNothing(connection);
                                    if (Interlocked.Read(ref m_State) == (long)RequestState.Started)
                                        (result as RedisVoid).TrySetResult(expectation.Value);
                                }
                                break;
                            case RedisCommandExpect.NullableDouble:
                                {
                                    var expectation = command.ExpectNullableDouble(connection);
                                    if (Interlocked.Read(ref m_State) == (long)RequestState.Started)
                                        (result as RedisNullableDouble).TrySetResult(expectation.Value);
                                }
                                break;
                            case RedisCommandExpect.NullableInteger:
                                {
                                    var expectation = command.ExpectNullableInteger(connection);
                                    if (Interlocked.Read(ref m_State) == (long)RequestState.Started)
                                        (result as RedisNullableInt).TrySetResult(expectation.Value);
                                }
                                break;
                            case RedisCommandExpect.OK:
                                {
                                    var expectation = command.ExpectSimpleString(connection, RedisConstants.OK);
                                    if (Interlocked.Read(ref m_State) == (long)RequestState.Started)
                                        (result as RedisBool).TrySetResult(expectation.Value);
                                }
                                break;
                            case RedisCommandExpect.One:
                                {
                                    var expectation = command.ExpectInteger(connection);
                                    if (Interlocked.Read(ref m_State) == (long)RequestState.Started)
                                        (result as RedisBool).TrySetResult(expectation.Value == RedisConstants.One);
                                }
                                break;
                            case RedisCommandExpect.SimpleString:
                                {
                                    var expectation = command.ExpectSimpleString(connection);
                                    if (Interlocked.Read(ref m_State) == (long)RequestState.Started)
                                        (result as RedisString).TrySetResult(expectation.Value);
                                }
                                break;
                            case RedisCommandExpect.SimpleStringBytes:
                                {
                                    var expectation = command.ExpectSimpleStringBytes(connection);
                                    if (Interlocked.Read(ref m_State) == (long)RequestState.Started)
                                        (result as RedisBytes).TrySetResult(expectation.Value);
                                }
                                break;
                        }

                        Interlocked.CompareExchange(ref m_State, (long)RequestState.Completed, (long)RequestState.Started);
                    }
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
