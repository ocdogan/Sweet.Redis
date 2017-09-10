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

        public RedisObject ExpectArray(byte[] cmd, params byte[][] parameters)
        {
            ValidateNotDisposed();
            using (var rcmd = new RedisCommand(Db.Db, cmd, parameters))
            {
                return rcmd.ExpectArray(Db.Pool, Db.ThrowOnError);
            }
        }

        public string ExpectBulkString(byte[] cmd, params byte[][] parameters)
        {
            ValidateNotDisposed();
            using (var rcmd = new RedisCommand(Db.Db, cmd, parameters))
            {
                return rcmd.ExpectBulkString(Db.Pool, Db.ThrowOnError);
            }
        }

        public byte[] ExpectBulkStringBytes(byte[] cmd, params byte[][] parameters)
        {
            ValidateNotDisposed();
            using (var rcmd = new RedisCommand(Db.Db, cmd, parameters))
            {
                return rcmd.ExpectBulkStringBytes(Db.Pool, Db.ThrowOnError);
            }
        }

        public double ExpectDouble(byte[] cmd, params byte[][] parameters)
        {
            ValidateNotDisposed();
            using (var rcmd = new RedisCommand(Db.Db, cmd, parameters))
            {
                return rcmd.ExpectDouble(Db.Pool, Db.ThrowOnError);
            }
        }
        public bool ExpectGreaterThanZero(byte[] cmd, params byte[][] parameters)
        {
            ValidateNotDisposed();
            using (var rcmd = new RedisCommand(Db.Db, cmd, parameters))
            {
                return rcmd.ExpectInteger(Db.Pool, Db.ThrowOnError) > 0L;
            }
        }

        public long ExpectInteger(byte[] cmd, params byte[][] parameters)
        {
            ValidateNotDisposed();
            using (var rcmd = new RedisCommand(Db.Db, cmd, parameters))
            {
                return rcmd.ExpectInteger(Db.Pool, Db.ThrowOnError);
            }
        }

        public byte[][] ExpectMultiDataBytes(byte[] cmd, params byte[][] parameters)
        {
            ValidateNotDisposed();
            using (var rcmd = new RedisCommand(Db.Db, cmd, parameters))
            {
                return rcmd.ExpectMultiDataBytes(Db.Pool, Db.ThrowOnError);
            }
        }

        public string[] ExpectMultiDataStrings(byte[] cmd, params byte[][] parameters)
        {
            ValidateNotDisposed();
            using (var rcmd = new RedisCommand(Db.Db, cmd, parameters))
            {
                return rcmd.ExpectMultiDataStrings(Db.Pool, Db.ThrowOnError);
            }
        }

        public long? ExpectNullableInteger(byte[] cmd, params byte[][] parameters)
        {
            ValidateNotDisposed();
            using (var rcmd = new RedisCommand(Db.Db, cmd, parameters))
            {
                return rcmd.ExpectNullableInteger(Db.Pool, Db.ThrowOnError);
            }
        }

        public bool ExpectOK(byte[] cmd, params byte[][] parameters)
        {
            return ExpectSimpleString(cmd, "OK", parameters);
        }

        public bool ExpectOne(byte[] cmd, params byte[][] parameters)
        {
            ValidateNotDisposed();
            using (var rcmd = new RedisCommand(Db.Db, cmd, parameters))
            {
                return rcmd.ExpectInteger(Db.Pool, Db.ThrowOnError) == 1L;
            }
        }

        public bool ExpectSimpleString(byte[] cmd, string expectedResult, params byte[][] parameters)
        {
            ValidateNotDisposed();
            using (var rcmd = new RedisCommand(Db.Db, cmd, parameters))
            {
                return rcmd.ExpectSimpleString(Db.Pool, expectedResult, Db.ThrowOnError);
            }
        }

        public string ExpectSimpleString(byte[] cmd, params byte[][] parameters)
        {
            ValidateNotDisposed();
            using (var rcmd = new RedisCommand(Db.Db, cmd, parameters))
            {
                return rcmd.ExpectSimpleString(Db.Pool, Db.ThrowOnError);
            }
        }

        public bool ExpectSimpleStringBytes(byte[] cmd, byte[] expectedResult, params byte[][] parameters)
        {
            ValidateNotDisposed();
            using (var rcmd = new RedisCommand(Db.Db, cmd, parameters))
            {
                return rcmd.ExpectSimpleStringBytes(Db.Pool, expectedResult, Db.ThrowOnError);
            }
        }

        public byte[] ExpectSimpleStringBytes(byte[] cmd, params byte[][] parameters)
        {
            ValidateNotDisposed();
            using (var rcmd = new RedisCommand(Db.Db, cmd, parameters))
            {
                return rcmd.ExpectSimpleStringBytes(Db.Pool, Db.ThrowOnError);
            }
        }

        #endregion Methods
    }
}
