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
using System.Text;

namespace Sweet.Redis
{
    internal class RedisDb : RedisCommandExecuterClient, IRedisDb, IRedisCommandExecuterClient
    {
        #region Field Members

        private int m_DbIndex;

        private IRedisConnectionCommands m_Connection;
        private IRedisGeoCommands m_Geo;
        private IRedisHashesCommands m_Hashes;
        private IRedisHyperLogLogCommands m_HyperLogLogCommands;
        private IRedisKeysCommands m_Keys;
        private IRedisListsCommands m_Lists;
        private IRedisPubSubCommands m_PubSubs;
        private IRedisScriptingCommands m_Scripting;
        private IRedisSetsCommands m_Sets;
        private IRedisSortedSetsCommands m_SortedSets;
        private IRedisStringsCommands m_Strings;

        #endregion Field Members

        #region .Ctors

        public RedisDb(RedisConnectionPool pool, int dbIndex, bool throwOnError = true)
            : base(pool, throwOnError)
        {
            m_DbIndex = Math.Min(Math.Max(dbIndex, RedisConstants.MinDbIndex), RedisConstants.MaxDbIndex);
        }

        #endregion .Ctors

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

        public override int DbIndex
        {
            get { return m_DbIndex; }
        }

        public IRedisGeoCommands Geo
        {
            get
            {
                ValidateNotDisposed();
                if (m_Geo == null)
                    m_Geo = new RedisGeoCommands(this);
                return m_Geo;
            }
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

        public IRedisHyperLogLogCommands HyperLogLogCommands
        {
            get
            {
                ValidateNotDisposed();
                if (m_HyperLogLogCommands == null)
                    m_HyperLogLogCommands = new RedisHyperLogLogCommands(this);
                return m_HyperLogLogCommands;
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

        public IRedisPubSubCommands PubSubs
        {
            get
            {
                ValidateNotDisposed();
                if (m_PubSubs == null)
                    m_PubSubs = new RedisPubSubCommands(this);
                return m_PubSubs;
            }
        }

        public IRedisScriptingCommands Scripting
        {
            get
            {
                ValidateNotDisposed();
                if (m_Scripting == null)
                    m_Scripting = new RedisScriptingCommands(this);
                return m_Scripting;
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

        public IRedisSortedSetsCommands SortedSets
        {
            get
            {
                ValidateNotDisposed();
                if (m_SortedSets == null)
                    m_SortedSets = new RedisSortedSetsCommands(this);
                return m_SortedSets;
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
    }
}
