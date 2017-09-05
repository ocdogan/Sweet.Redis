﻿using System;
using System.Threading;

namespace Sweet.Redis
{
    internal class RedisDb : RedisDisposable, IRedisDb
    {
        #region Field Members

        private int m_Db;
        private Guid m_Id;

        private RedisConnectionPool m_Pool;

        private IRedisConnectionCommands m_Connection;
        private IRedisHashesCommands m_Hashes;
        private IRedisKeysCommands m_Keys;
        private IRedisListsCommands m_Lists;
        private IRedisServerCommands m_Server;
        private IRedisSetsCommands m_Sets;
        private IRedisStringsCommands m_Strings;

        #endregion Field Members

        #region .Ctors

        public RedisDb(RedisConnectionPool pool, int db)
        {
            m_Id = Guid.NewGuid();
            m_Pool = pool;

            m_Db = db;
            if (db != 0)
            {
                try
                {
                    Select(db, true);
                }
                catch (Exception)
                {
                    m_Pool = null;
                    throw;
                }
            }
        }

        #endregion .Ctors

        #region Destructors

        protected override void OnDispose(bool disposing)
        {
            if (SetDisposed())
                return;

            var pool = Interlocked.Exchange(ref m_Pool, null);
            if (pool == null)
                return;
        }

        #endregion Destructors

        #region Properties

        public IRedisConnectionCommands Connection
        {
            get
            {
                ValidateNotDisposed();
                if (m_Connection == null)
                    m_Connection = new RedisConnectionCommands(this);
                return m_Connection;
            }
        }

        public int Db
        {
            get { return m_Db; }
        }

        public Guid Id
        {
            get { return m_Id; }
        }

        public IRedisHashesCommands Hashes
        {
            get
            {
                ValidateNotDisposed();
                if (m_Hashes == null)
                    m_Hashes = new RedisHashesCommands(this);
                return m_Hashes;
            }
        }

        public IRedisKeysCommands Keys
        {
            get
            {
                ValidateNotDisposed();
                if (m_Keys == null)
                    m_Keys = new RedisKeysCommands(this);
                return m_Keys;
            }
        }

        public IRedisListsCommands Lists
        {
            get
            {
                ValidateNotDisposed();
                if (m_Lists == null)
                    m_Lists = new RedisListsCommands(this);
                return m_Lists;
            }
        }

        public RedisConnectionPool Pool
        {
            get
            {
                ValidateNotDisposed();
                return m_Pool;
            }
        }

        public IRedisServerCommands Server
        {
            get
            {
                ValidateNotDisposed();
                if (m_Server == null)
                    m_Server = new RedisServerCommands(this);
                return m_Server;
            }
        }

        public IRedisSetsCommands Sets
        {
            get
            {
                ValidateNotDisposed();
                if (m_Sets == null)
                    m_Sets = new RedisSetsCommands(this);
                return m_Sets;
            }
        }

        public IRedisStringsCommands Strings
        {
            get
            {
                ValidateNotDisposed();
                if (m_Strings == null)
                    m_Strings = new RedisStringsCommands(this);
                return m_Strings;
            }
        }

        #endregion Properties

        #region Methods

        protected override void ValidateNotDisposed()
        {
            if (Disposed)
                throw new ObjectDisposedException(GetType().Name + ", " + m_Id.ToString("N"));
        }

        private bool Select(int db, bool throwException)
        {
            ValidateNotDisposed();
            return (new RedisCommand(RedisCommands.Select, db.ToBytes())).ExpectSimpleString(m_Pool, "OK", throwException);
        }

        #endregion Methods
    }
}
