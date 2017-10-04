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

        protected enum RequestState : long
        {
            Waiting = 0L,
            Initiated = 1L,
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

        public RedisRequest(RedisCommand command, RedisCommandExpect expectation, string okIf = null, bool transactional = false)
            : base(command, expectation, okIf, null, transactional)
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
                return !(state == RequestState.Waiting || state == RequestState.Initiated);
            }
        }

        public override bool IsFaulted
        {
            get { return Interlocked.Read(ref m_State) == (long)RequestState.Failed; }
        }

        public override bool IsStarted
        {
            get { return Interlocked.Read(ref m_State) == (long)RequestState.Initiated; }
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
            if (ReferenceEquals(result, null))
            {
                var ctor = s_Ctor;
                if (ctor == null)
                {
                    ctor = s_Ctor = typeof(T).GetConstructor(BindingFlags.Public |
                                                      BindingFlags.NonPublic |
                                                      BindingFlags.Instance, null, Type.EmptyTypes, null);
                }

                result = m_Result = (T)ctor.Invoke(null);
            }
            return result;
        }

        public override void Cancel()
        {
            ValidateNotDisposed();

            if (Interlocked.CompareExchange(ref m_State, (long)RequestState.Canceled, (long)RequestState.Waiting) != (long)RequestState.Waiting)
                Interlocked.CompareExchange(ref m_State, (long)RequestState.Canceled, (long)RequestState.Initiated);
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

        public override void SetResult(object value)
        {
            if (TrySetCompleted())
            {
                try
                {
                    var result = CreateResult();
                    if (ReferenceEquals(value, null))
                    {
                        result.TrySetResult(null);
                        return;
                    }

                    if (value is RedisResult)
                        value = ((RedisResult)value).RawData;

                    switch (Expectation)
                    {
                        case RedisCommandExpect.Response:
                            result.TrySetResult(value);
                            break;
                        case RedisCommandExpect.Array:
                            result.TrySetResult(value);
                            break;
                        case RedisCommandExpect.BulkString:
                            result.TrySetResult(value);
                            break;
                        case RedisCommandExpect.BulkStringBytes:
                            result.TrySetResult(value);
                            break;
                        case RedisCommandExpect.Double:
                            result.TrySetResult(value);
                            break;
                        case RedisCommandExpect.GreaterThanZero:
                            if (value is bool)
                                result.TrySetResult((bool)value);
                            else if (value is long)
                                result.TrySetResult(RedisConstants.Zero < (long)value);
                            else if (value is int)
                                result.TrySetResult(RedisConstants.Zero < (int)value);
                            else
                                result.TrySetResult(RedisConstants.Zero.CompareTo(value) == -1);
                            break;
                        case RedisCommandExpect.Integer:
                            result.TrySetResult(value);
                            break;
                        case RedisCommandExpect.MultiDataBytes:
                            result.TrySetResult(value);
                            break;
                        case RedisCommandExpect.MultiDataStrings:
                            result.TrySetResult(value);
                            break;
                        case RedisCommandExpect.Nothing:
                            result.TrySetResult(value);
                            break;
                        case RedisCommandExpect.NullableDouble:
                            result.TrySetResult(value);
                            break;
                        case RedisCommandExpect.NullableInteger:
                            result.TrySetResult(value);
                            break;
                        case RedisCommandExpect.OK:
                            if (value is bool)
                                result.TrySetResult(value);
                            else if (value is string)
                                result.TrySetResult(RedisConstants.OK == (string)value);
                            else
                                result.TrySetResult(Object.Equals(RedisConstants.OK, value));
                            break;
                        case RedisCommandExpect.One:
                            if (value is bool)
                                result.TrySetResult(value);
                            else if (value is long)
                                result.TrySetResult(RedisConstants.One == (long)value);
                            else if (value is int)
                                result.TrySetResult(RedisConstants.One == (int)value);
                            else
                                result.TrySetResult(Object.Equals(RedisConstants.One, value));
                            break;
                        case RedisCommandExpect.SimpleString:
                            result.TrySetResult(value);
                            break;
                        case RedisCommandExpect.SimpleStringBytes:
                            result.TrySetResult(value);
                            break;
                    }
                }
                catch (Exception e)
                {
                    SetException(e);
                }
            }
        }

        protected bool TrySetCompleted()
        {
            if (Interlocked.CompareExchange(ref m_State, (long)RequestState.Completed, (long)RequestState.Initiated) == (long)RequestState.Waiting)
                Interlocked.CompareExchange(ref m_State, (long)RequestState.Completed, (long)RequestState.Waiting);
            return Interlocked.Read(ref m_State) == (long)RequestState.Completed;
        }

        public override void Process(IRedisConnection connection)
        {
            ValidateNotDisposed();

            if (Interlocked.CompareExchange(ref m_State, (long)RequestState.Initiated, (long)RequestState.Waiting) ==
                (long)RequestState.Waiting)
            {
                if (connection == null || connection.Disposed)
                    Interlocked.Exchange(ref m_State, (long)RequestState.Canceled);
                else
                    ProcessInternal(connection.Connect(), connection.Settings);
            }
        }

        public override void Process(RedisSocket socket, RedisSettings settings)
        {
            ValidateNotDisposed();

            if (Interlocked.CompareExchange(ref m_State, (long)RequestState.Initiated, (long)RequestState.Waiting) ==
                (long)RequestState.Waiting)
            {
                ProcessInternal(socket, settings);
            }
        }

        protected void ProcessInternal(RedisSocket socket, RedisSettings settings)
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

                    settings = settings ?? RedisSettings.Default;

                    if (Transactional)
                    {
                        command.ExpectSimpleString(socket, settings, RedisConstants.OK);
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
