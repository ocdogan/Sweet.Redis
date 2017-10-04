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
    internal class RedisCommandSet : RedisInternalDisposable, IRedisCommandSet
    {
        #region Field Members

        private RedisDb m_Db;
        private Guid m_Id;

        #endregion Field Members

        #region .Ctors

        public RedisCommandSet(RedisDb db)
        {
            m_Db = db;
            m_Id = db.Id;
        }

        #endregion .Ctors

        #region Destructors

        protected override void OnDispose(bool disposing)
        {
            Interlocked.Exchange(ref m_Db, null);
        }

        #endregion Destructors

        #region Properties

        public IRedisDb Db
        {
            get { return m_Db; }
        }

        public Guid Id
        {
            get { return m_Id; }
        }

        #endregion Properties

        #region Methods

        #region Validation Methods

        public override void ValidateNotDisposed()
        {
            if (Disposed)
                throw new ObjectDisposedException(GetType().Name + ", " + m_Id.ToString("N"));
        }

        protected static void ValidateKeyAndValue(string key, byte[] value, string keyName = null, string valueName = null)
        {
            if (key == null)
                throw new ArgumentNullException(String.IsNullOrEmpty(keyName) ? "key" : keyName);

            if (value == null)
                throw new ArgumentNullException(String.IsNullOrEmpty(valueName) ? "value" : valueName);

            if (value.Length > RedisConstants.MaxValueLength)
                throw new ArgumentException("Redis values are limited to 1GB", String.IsNullOrEmpty(valueName) ? "value" : valueName);
        }

        protected static void ValidateKeyAndValue(RedisParam key, byte[] value, string keyName = null, string valueName = null)
        {
            if (key.IsEmpty)
                throw new ArgumentNullException(String.IsNullOrEmpty(keyName) ? "key" : keyName);

            if (value == null)
                throw new ArgumentNullException(String.IsNullOrEmpty(valueName) ? "value" : valueName);

            if (value.Length > RedisConstants.MaxValueLength)
                throw new ArgumentException("Redis values are limited to 1GB", String.IsNullOrEmpty(valueName) ? "value" : valueName);
        }

        protected static void ValidateKeyAndValue(RedisParam key, RedisParam value, string keyName = null, string valueName = null)
        {
            if (key.IsEmpty)
                throw new ArgumentNullException(String.IsNullOrEmpty(keyName) ? "key" : keyName);

            if (value.IsNull)
                throw new ArgumentNullException(String.IsNullOrEmpty(valueName) ? "value" : valueName);

            if (value.Data.Length > RedisConstants.MaxValueLength)
                throw new ArgumentException("Redis values are limited to 1GB", String.IsNullOrEmpty(valueName) ? "value" : valueName);
        }

        #endregion Validation Methods

        #region Execution Methods

        protected RedisRaw ExpectArray(byte[] cmd, params byte[][] parameters)
        {
            ValidateNotDisposed();
            return m_Db.ExpectArray(cmd, parameters);
        }

        protected RedisString ExpectBulkString(byte[] cmd, params byte[][] parameters)
        {
            ValidateNotDisposed();
            return m_Db.ExpectBulkString(cmd, parameters);
        }

        protected RedisBytes ExpectBulkStringBytes(byte[] cmd, params byte[][] parameters)
        {
            ValidateNotDisposed();
            return m_Db.ExpectBulkStringBytes(cmd, parameters);
        }

        protected RedisDouble ExpectDouble(byte[] cmd, params byte[][] parameters)
        {
            ValidateNotDisposed();
            return m_Db.ExpectDouble(cmd, parameters);
        }

        protected RedisBool ExpectGreaterThanZero(byte[] cmd, params byte[][] parameters)
        {
            ValidateNotDisposed();
            return m_Db.ExpectGreaterThanZero(cmd, parameters);
        }

        protected RedisInteger ExpectInteger(byte[] cmd, params byte[][] parameters)
        {
            ValidateNotDisposed();
            return m_Db.ExpectInteger(cmd, parameters);
        }

        protected RedisMultiBytes ExpectMultiDataBytes(byte[] cmd, params byte[][] parameters)
        {
            ValidateNotDisposed();
            return m_Db.ExpectMultiDataBytes(cmd, parameters);
        }

        protected RedisMultiString ExpectMultiDataStrings(byte[] cmd, params byte[][] parameters)
        {
            ValidateNotDisposed();
            return m_Db.ExpectMultiDataStrings(cmd, parameters);
        }

        protected RedisVoid ExpectNothing(byte[] cmd, params byte[][] parameters)
        {
            ValidateNotDisposed();
            return m_Db.ExpectNothing(cmd, parameters);
        }

        protected RedisNullableDouble ExpectNullableDouble(byte[] cmd, params byte[][] parameters)
        {
            ValidateNotDisposed();
            return m_Db.ExpectNullableDouble(cmd, parameters);
        }

        protected RedisNullableInteger ExpectNullableInteger(byte[] cmd, params byte[][] parameters)
        {
            ValidateNotDisposed();
            return m_Db.ExpectNullableInteger(cmd, parameters);
        }

        protected RedisBool ExpectOK(byte[] cmd, params byte[][] parameters)
        {
            ValidateNotDisposed();
            return m_Db.ExpectOK(cmd, parameters);
        }

        protected RedisBool ExpectOne(byte[] cmd, params byte[][] parameters)
        {
            ValidateNotDisposed();
            return m_Db.ExpectOne(cmd, parameters);
        }

        protected RedisBool ExpectSimpleString(byte[] cmd, string expectedResult, params byte[][] parameters)
        {
            ValidateNotDisposed();
            return m_Db.ExpectSimpleString(cmd, expectedResult, parameters);
        }

        protected RedisString ExpectSimpleString(byte[] cmd, params byte[][] parameters)
        {
            ValidateNotDisposed();
            return m_Db.ExpectSimpleString(cmd, parameters);
        }

        protected RedisBool ExpectSimpleStringBytes(byte[] cmd, byte[] expectedResult, params byte[][] parameters)
        {
            ValidateNotDisposed();
            return m_Db.ExpectSimpleStringBytes(cmd, expectedResult, parameters);
        }

        protected RedisBytes ExpectSimpleStringBytes(byte[] cmd, params byte[][] parameters)
        {
            ValidateNotDisposed();
            return m_Db.ExpectSimpleStringBytes(cmd, parameters);
        }

        #endregion Execution Methods

        #endregion Methods
    }
}
