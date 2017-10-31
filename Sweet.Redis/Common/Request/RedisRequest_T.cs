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
using System.Reflection;
using System.Threading;

namespace Sweet.Redis
{
    internal abstract class RedisRequest<T> : RedisRequest
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

        protected const int MaxTimeout = 60 * 1000;

        #endregion Constants

        #region Static Members

        private static ConstructorInfo s_Ctor;

        #endregion Static Members

        #region Field Members

        protected T m_Result;
        protected long m_State;
        protected Exception m_Exception;

        #endregion Field Members

        #region .Ctors

        protected RedisRequest(RedisCommand command, RedisCommandExpect expectation,
            string okIf = null, RedisRequestType requestType = RedisRequestType.Default)
            : base(command, expectation, okIf, null, requestType)
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

        protected virtual T CreateResult()
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
                        default:
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
                if (!connection.IsAlive())
                    Interlocked.Exchange(ref m_State, (long)RequestState.Canceled);
                else
                    ProcessInternal(new RedisSocketContext(connection.Connect(), connection.Settings));
            }
        }

        public override void Process(RedisSocketContext context)
        {
            ValidateNotDisposed();

            if (Interlocked.CompareExchange(ref m_State, (long)RequestState.Initiated, (long)RequestState.Waiting) ==
                (long)RequestState.Waiting)
            {
                ProcessInternal(context);
            }
        }

        protected virtual void ProcessInternal(RedisSocketContext context)
        { }

        #endregion Methods
    }
}
