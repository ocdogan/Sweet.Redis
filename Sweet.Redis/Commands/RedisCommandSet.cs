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
    internal class RedisCommandSet : RedisDisposable
    {
        #region Field Members

        private IRedisDb m_Db;
        private Guid m_Id;

        #endregion Field Members

        #region .Ctors

        public RedisCommandSet(IRedisDb db)
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

        protected RedisRaw ExpectArray(byte[] cmd, params byte[][] parameters)
        {
            ValidateNotDisposed();
            using (var connection = Db.Pool.Connect(Db.Db))
            {
                using (var rcmd = new RedisCommand(Db.Db, cmd, parameters))
                {
                    return rcmd.ExpectArray(connection, Db.ThrowOnError);
                }
            }
        }

        protected RedisString ExpectBulkString(byte[] cmd, params byte[][] parameters)
        {
            ValidateNotDisposed();
            using (var connection = Db.Pool.Connect(Db.Db))
            {
                using (var rcmd = new RedisCommand(Db.Db, cmd, parameters))
                {
                    return rcmd.ExpectBulkString(connection, Db.ThrowOnError);
                }
            }
        }

        protected RedisBytes ExpectBulkStringBytes(byte[] cmd, params byte[][] parameters)
        {
            ValidateNotDisposed();
            using (var connection = Db.Pool.Connect(Db.Db))
            {
                using (var rcmd = new RedisCommand(Db.Db, cmd, parameters))
                {
                    return rcmd.ExpectBulkStringBytes(connection, Db.ThrowOnError);
                }
            }
        }

        protected RedisDouble ExpectDouble(byte[] cmd, params byte[][] parameters)
        {
            ValidateNotDisposed();
            using (var connection = Db.Pool.Connect(Db.Db))
            {
                using (var rcmd = new RedisCommand(Db.Db, cmd, parameters))
                {
                    return rcmd.ExpectDouble(connection, Db.ThrowOnError);
                }
            }
        }

        protected RedisBool ExpectGreaterThanZero(byte[] cmd, params byte[][] parameters)
        {
            ValidateNotDisposed();
            using (var connection = Db.Pool.Connect(Db.Db))
            {
                using (var rcmd = new RedisCommand(Db.Db, cmd, parameters))
                {
                    return rcmd.ExpectInteger(connection, Db.ThrowOnError) > RedisConstants.Zero;
                }
            }
        }

        protected RedisInt ExpectInteger(byte[] cmd, params byte[][] parameters)
        {
            ValidateNotDisposed();
            using (var connection = Db.Pool.Connect(Db.Db))
            {
                using (var rcmd = new RedisCommand(Db.Db, cmd, parameters))
                {
                    return rcmd.ExpectInteger(connection, Db.ThrowOnError);
                }
            }
        }

        protected RedisMultiBytes ExpectMultiDataBytes(byte[] cmd, params byte[][] parameters)
        {
            ValidateNotDisposed();
            using (var connection = Db.Pool.Connect(Db.Db))
            {
                using (var rcmd = new RedisCommand(Db.Db, cmd, parameters))
                {
                    return rcmd.ExpectMultiDataBytes(connection, Db.ThrowOnError);
                }
            }
        }

        public RedisMultiString ExpectMultiDataStrings(byte[] cmd, params byte[][] parameters)
        {
            ValidateNotDisposed();
            using (var connection = Db.Pool.Connect(Db.Db))
            {
                using (var rcmd = new RedisCommand(Db.Db, cmd, parameters))
                {
                    return rcmd.ExpectMultiDataStrings(connection, Db.ThrowOnError);
                }
            }
        }

        public RedisNullableInt ExpectNullableInteger(byte[] cmd, params byte[][] parameters)
        {
            ValidateNotDisposed();
            using (var connection = Db.Pool.Connect(Db.Db))
            {
                using (var rcmd = new RedisCommand(Db.Db, cmd, parameters))
                {
                    return rcmd.ExpectNullableInteger(connection, Db.ThrowOnError);
                }
            }
        }

        public RedisBool ExpectOK(byte[] cmd, params byte[][] parameters)
        {
            return ExpectSimpleString(cmd, "OK", parameters);
        }

        public RedisBool ExpectOne(byte[] cmd, params byte[][] parameters)
        {
            ValidateNotDisposed();
            using (var connection = Db.Pool.Connect(Db.Db))
            {
                using (var rcmd = new RedisCommand(Db.Db, cmd, parameters))
                {
                    return rcmd.ExpectInteger(connection, Db.ThrowOnError) == RedisConstants.One;
                }
            }
        }

        public RedisBool ExpectSimpleString(byte[] cmd, string expectedResult, params byte[][] parameters)
        {
            ValidateNotDisposed();
            using (var connection = Db.Pool.Connect(Db.Db))
            {
                using (var rcmd = new RedisCommand(Db.Db, cmd, parameters))
                {
                    return rcmd.ExpectSimpleString(connection, expectedResult, Db.ThrowOnError);
                }
            }
        }

        public RedisString ExpectSimpleString(byte[] cmd, params byte[][] parameters)
        {
            ValidateNotDisposed();
            using (var connection = Db.Pool.Connect(Db.Db))
            {
                using (var rcmd = new RedisCommand(Db.Db, cmd, parameters))
                {
                    return rcmd.ExpectSimpleString(connection, Db.ThrowOnError);
                }
            }
        }

        public RedisBool ExpectSimpleStringBytes(byte[] cmd, byte[] expectedResult, params byte[][] parameters)
        {
            ValidateNotDisposed();
            using (var connection = Db.Pool.Connect(Db.Db))
            {
                using (var rcmd = new RedisCommand(Db.Db, cmd, parameters))
                {
                    return rcmd.ExpectSimpleStringBytes(connection, expectedResult, Db.ThrowOnError);
                }
            }
        }

        public RedisBytes ExpectSimpleStringBytes(byte[] cmd, params byte[][] parameters)
        {
            ValidateNotDisposed();
            using (var connection = Db.Pool.Connect(Db.Db))
            {
                using (var rcmd = new RedisCommand(Db.Db, cmd, parameters))
                {
                    return rcmd.ExpectSimpleStringBytes(connection, Db.ThrowOnError);
                }
            }
        }

        #endregion Methods
    }
}
