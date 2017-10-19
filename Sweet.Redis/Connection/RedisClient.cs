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
using System.Text;

namespace Sweet.Redis
{
    public class RedisClient : RedisDisposable, IRedisClient, IRedisIdentifiedObject
    {
        #region Field Members

        private Guid m_Id;

        #endregion Field Members

        #region .Ctors

        public RedisClient(bool throwOnError = true)
        {
            m_Id = Guid.NewGuid();
            ThrowOnError = throwOnError;
        }

        #endregion .Ctors

        #region Properties

        public Guid Id
        {
            get { return m_Id; }
        }

        public virtual int DbIndex { get { return -1; } }

        public virtual RedisRole Role
        {
            get { return RedisRole.Undefined; }
        }

        public bool ThrowOnError { get; private set; }

        #endregion Properties

        #region Methods

        #region IRedisConnection Methods

        public override void ValidateNotDisposed()
        {
            if (Disposed)
                throw new RedisFatalException(new ObjectDisposedException(GetType().Name + ", " + m_Id.ToString("N").ToUpper()), RedisErrorCode.ObjectDisposed);
        }

        #endregion IRedisConnection Methods

        #region Execution Methods

        protected internal virtual RedisRaw ExpectArray(byte[] cmd, params byte[][] parameters)
        {
            ValidateNotDisposed();
            return Expect<RedisRaw>(new RedisCommand(DbIndex, cmd, RedisCommandType.SendAndReceive, parameters), RedisCommandExpect.Array);
        }

        protected internal virtual RedisString ExpectBulkString(byte[] cmd, params byte[][] parameters)
        {
            ValidateNotDisposed();
            return Expect<RedisString>(new RedisCommand(DbIndex, cmd, RedisCommandType.SendAndReceive, parameters), RedisCommandExpect.BulkString);
        }

        protected internal virtual RedisBytes ExpectBulkStringBytes(byte[] cmd, params byte[][] parameters)
        {
            ValidateNotDisposed();
            return Expect<RedisBytes>(new RedisCommand(DbIndex, cmd, RedisCommandType.SendAndReceive, parameters), RedisCommandExpect.BulkStringBytes);
        }

        protected internal virtual RedisDouble ExpectDouble(byte[] cmd, params byte[][] parameters)
        {
            ValidateNotDisposed();
            return Expect<RedisDouble>(new RedisCommand(DbIndex, cmd, RedisCommandType.SendAndReceive, parameters), RedisCommandExpect.Double);
        }

        protected internal virtual RedisBool ExpectGreaterThanZero(byte[] cmd, params byte[][] parameters)
        {
            ValidateNotDisposed();
            return Expect<RedisBool>(new RedisCommand(DbIndex, cmd, RedisCommandType.SendAndReceive, parameters), RedisCommandExpect.GreaterThanZero);
        }

        protected internal virtual RedisInteger ExpectInteger(byte[] cmd, params byte[][] parameters)
        {
            ValidateNotDisposed();
            return Expect<RedisInteger>(new RedisCommand(DbIndex, cmd, RedisCommandType.SendAndReceive, parameters), RedisCommandExpect.Integer);
        }

        protected internal virtual RedisMultiBytes ExpectMultiDataBytes(byte[] cmd, params byte[][] parameters)
        {
            ValidateNotDisposed();
            return Expect<RedisMultiBytes>(new RedisCommand(DbIndex, cmd, RedisCommandType.SendAndReceive, parameters), RedisCommandExpect.MultiDataBytes);
        }

        protected internal virtual RedisMultiString ExpectMultiDataStrings(byte[] cmd, params byte[][] parameters)
        {
            ValidateNotDisposed();
            return Expect<RedisMultiString>(new RedisCommand(DbIndex, cmd, RedisCommandType.SendAndReceive, parameters), RedisCommandExpect.MultiDataStrings);
        }

        protected internal virtual RedisVoid ExpectNothing(byte[] cmd, params byte[][] parameters)
        {
            ValidateNotDisposed();
            return Expect<RedisVoid>(new RedisCommand(DbIndex, cmd, RedisCommandType.SendNotReceive, parameters), RedisCommandExpect.Nothing);
        }

        protected internal virtual RedisNullableDouble ExpectNullableDouble(byte[] cmd, params byte[][] parameters)
        {
            ValidateNotDisposed();
            return Expect<RedisNullableDouble>(new RedisCommand(DbIndex, cmd, RedisCommandType.SendAndReceive, parameters), RedisCommandExpect.NullableDouble);
        }

        protected internal virtual RedisNullableInteger ExpectNullableInteger(byte[] cmd, params byte[][] parameters)
        {
            ValidateNotDisposed();
            return Expect<RedisNullableInteger>(new RedisCommand(DbIndex, cmd, RedisCommandType.SendAndReceive, parameters), RedisCommandExpect.NullableInteger);
        }

        protected internal virtual RedisBool ExpectOK(byte[] cmd, params byte[][] parameters)
        {
            ValidateNotDisposed();
            return Expect<RedisBool>(new RedisCommand(DbIndex, cmd, RedisCommandType.SendAndReceive, parameters), RedisCommandExpect.OK);
        }

        protected internal virtual RedisBool ExpectOne(byte[] cmd, params byte[][] parameters)
        {
            ValidateNotDisposed();
            return Expect<RedisBool>(new RedisCommand(DbIndex, cmd, RedisCommandType.SendAndReceive, parameters), RedisCommandExpect.One);
        }

        protected internal virtual RedisBool ExpectSimpleString(byte[] cmd, string expectedResult, params byte[][] parameters)
        {
            ValidateNotDisposed();
            return Expect<RedisBool>(new RedisCommand(DbIndex, cmd, RedisCommandType.SendAndReceive, parameters), RedisCommandExpect.SimpleString, expectedResult);
        }

        protected internal virtual RedisString ExpectSimpleString(byte[] cmd, params byte[][] parameters)
        {
            ValidateNotDisposed();
            return Expect<RedisString>(new RedisCommand(DbIndex, cmd, RedisCommandType.SendAndReceive, parameters), RedisCommandExpect.SimpleString);
        }

        protected internal virtual RedisBool ExpectSimpleStringBytes(byte[] cmd, byte[] expectedResult, params byte[][] parameters)
        {
            ValidateNotDisposed();
            return Expect<RedisBool>(new RedisCommand(DbIndex, cmd, RedisCommandType.SendAndReceive, parameters), RedisCommandExpect.SimpleStringBytes,
                expectedResult != null ? Encoding.UTF8.GetString(expectedResult) : null);
        }

        protected internal virtual RedisBytes ExpectSimpleStringBytes(byte[] cmd, params byte[][] parameters)
        {
            ValidateNotDisposed();
            return Expect<RedisBytes>(new RedisCommand(DbIndex, cmd, RedisCommandType.SendAndReceive, parameters), RedisCommandExpect.SimpleStringBytes);
        }

        protected internal virtual T Expect<T>(RedisCommand command, RedisCommandExpect expectation, string okIf = null)
            where T : RedisResult
        {
            throw new RedisException("Undefined exception", RedisErrorCode.NotSupported);
        }

        #endregion Execution Methods

        #endregion Methods
    }
}
