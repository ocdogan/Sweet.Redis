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
    internal class RedisCommandSet : RedisInternalDisposable, IRedisCommandSet, IRedisIdentifiedObject
    {
        #region Field Members

        private RedisClient m_Client;
        private long m_Id = RedisIDGenerator<RedisCommandSet>.NextId();

        #endregion Field Members

        #region .Ctors

        public RedisCommandSet(RedisClient client)
        {
            m_Client = client;
        }

        #endregion .Ctors

        #region Destructors

        protected override void OnDispose(bool disposing)
        {
            Interlocked.Exchange(ref m_Client, null);
        }

        #endregion Destructors

        #region Properties

        public IRedisClient Client
        {
            get { return m_Client; }
        }

        public long Id
        {
            get { return m_Id; }
        }

        #endregion Properties

        #region Methods

        #region Validation Methods

        public override void ValidateNotDisposed()
        {
            if (Disposed)
                throw new RedisFatalException(new ObjectDisposedException(GetType().Name + ", " + m_Id.ToString()), RedisErrorCode.ObjectDisposed);
        }

        protected static void ValidateKeyAndValue(string key, byte[] value, string keyName = null, string valueName = null)
        {
            if (key == null)
                throw new RedisFatalException(new ArgumentNullException(keyName.IsEmpty() ? "key" : keyName), RedisErrorCode.MissingParameter);

            if (value == null)
                throw new RedisFatalException(new ArgumentNullException(valueName.IsEmpty() ? "value" : valueName), RedisErrorCode.MissingParameter);

            if (value.Length > RedisConstants.MaxValueLength)
                throw new RedisFatalException(new ArgumentException("Redis values are limited to 1GB", valueName.IsEmpty() ? "value" : valueName), RedisErrorCode.MissingParameter);
        }

        protected static void ValidateKeyAndValue(RedisParam key, byte[] value, string keyName = null, string valueName = null)
        {
            if (key.IsEmpty)
                throw new RedisFatalException(new ArgumentNullException(keyName.IsEmpty() ? "key" : keyName), RedisErrorCode.MissingParameter);

            if (value == null)
                throw new RedisFatalException(new ArgumentNullException(valueName.IsEmpty() ? "value" : valueName), RedisErrorCode.MissingParameter);

            if (value.Length > RedisConstants.MaxValueLength)
                throw new RedisFatalException(new ArgumentException("Redis values are limited to 1GB", valueName.IsEmpty() ? "value" : valueName), RedisErrorCode.MissingParameter);
        }

        protected static void ValidateKeyAndValue(RedisParam key, RedisParam value, string keyName = null, string valueName = null)
        {
            if (key.IsEmpty)
                throw new RedisFatalException(new ArgumentNullException(keyName.IsEmpty() ? "key" : keyName), RedisErrorCode.MissingParameter);

            if (value.IsNull)
                throw new RedisFatalException(new ArgumentNullException(valueName.IsEmpty() ? "value" : valueName), RedisErrorCode.MissingParameter);

            if (value.Data.Length > RedisConstants.MaxValueLength)
                throw new RedisFatalException(new ArgumentException("Redis values are limited to 1GB", valueName.IsEmpty() ? "value" : valueName), RedisErrorCode.MissingParameter);
        }

        #endregion Validation Methods

        #region Execution Methods

        protected RedisRaw ExpectArray(byte[] cmd, params byte[][] parameters)
        {
            ValidateNotDisposed();
            return m_Client.ExpectArray(cmd, parameters);
        }

        protected RedisString ExpectBulkString(byte[] cmd, params byte[][] parameters)
        {
            ValidateNotDisposed();
            return m_Client.ExpectBulkString(cmd, parameters);
        }

        protected RedisBytes ExpectBulkStringBytes(byte[] cmd, params byte[][] parameters)
        {
            ValidateNotDisposed();
            return m_Client.ExpectBulkStringBytes(cmd, parameters);
        }

        protected RedisDouble ExpectDouble(byte[] cmd, params byte[][] parameters)
        {
            ValidateNotDisposed();
            return m_Client.ExpectDouble(cmd, parameters);
        }

        protected RedisBool ExpectGreaterThanZero(byte[] cmd, params byte[][] parameters)
        {
            ValidateNotDisposed();
            return m_Client.ExpectGreaterThanZero(cmd, parameters);
        }

        protected RedisInteger ExpectInteger(byte[] cmd, params byte[][] parameters)
        {
            ValidateNotDisposed();
            return m_Client.ExpectInteger(cmd, parameters);
        }

        protected RedisMultiBytes ExpectMultiDataBytes(byte[] cmd, params byte[][] parameters)
        {
            ValidateNotDisposed();
            return m_Client.ExpectMultiDataBytes(cmd, parameters);
        }

        protected RedisMultiString ExpectMultiDataStrings(byte[] cmd, params byte[][] parameters)
        {
            ValidateNotDisposed();
            return m_Client.ExpectMultiDataStrings(cmd, parameters);
        }

        protected RedisVoid ExpectNothing(byte[] cmd, params byte[][] parameters)
        {
            ValidateNotDisposed();
            return m_Client.ExpectNothing(cmd, parameters);
        }

        protected RedisNullableDouble ExpectNullableDouble(byte[] cmd, params byte[][] parameters)
        {
            ValidateNotDisposed();
            return m_Client.ExpectNullableDouble(cmd, parameters);
        }

        protected RedisNullableInteger ExpectNullableInteger(byte[] cmd, params byte[][] parameters)
        {
            ValidateNotDisposed();
            return m_Client.ExpectNullableInteger(cmd, parameters);
        }

        protected RedisBool ExpectOK(byte[] cmd, params byte[][] parameters)
        {
            ValidateNotDisposed();
            return m_Client.ExpectOK(cmd, parameters);
        }

        protected RedisBool ExpectOne(byte[] cmd, params byte[][] parameters)
        {
            ValidateNotDisposed();
            return m_Client.ExpectOne(cmd, parameters);
        }

        protected RedisBool ExpectSimpleString(byte[] cmd, string expectedResult, params byte[][] parameters)
        {
            ValidateNotDisposed();
            return m_Client.ExpectSimpleString(cmd, expectedResult, parameters);
        }

        protected RedisString ExpectSimpleString(byte[] cmd, params byte[][] parameters)
        {
            ValidateNotDisposed();
            return m_Client.ExpectSimpleString(cmd, parameters);
        }

        protected RedisBool ExpectSimpleStringBytes(byte[] cmd, byte[] expectedResult, params byte[][] parameters)
        {
            ValidateNotDisposed();
            return m_Client.ExpectSimpleStringBytes(cmd, expectedResult, parameters);
        }

        protected RedisBytes ExpectSimpleStringBytes(byte[] cmd, params byte[][] parameters)
        {
            ValidateNotDisposed();
            return m_Client.ExpectSimpleStringBytes(cmd, parameters);
        }

        #endregion Execution Methods

        #endregion Methods
    }
}
