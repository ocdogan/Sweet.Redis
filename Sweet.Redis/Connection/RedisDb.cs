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
    internal class RedisDb : RedisDisposable, IRedisDb
    {
        #region Field Members

        private Guid m_Id;
        private int m_DbIndex;

        private RedisConnectionPool m_Pool;

        private IRedisConnectionCommands m_Connection;
        private IRedisGeoCommands m_Geo;
        private IRedisHashesCommands m_Hashes;
        private IRedisHyperLogLogCommands m_HyperLogLogCommands;
        private IRedisKeysCommands m_Keys;
        private IRedisListsCommands m_Lists;
        private IRedisPubSubCommands m_PubSubs;
        private IRedisScriptingCommands m_Scripting;
        private IRedisServerCommands m_Server;
        private IRedisSetsCommands m_Sets;
        private IRedisSortedSetsCommands m_SortedSets;
        private IRedisStringsCommands m_Strings;

        #endregion Field Members

        #region .Ctors

        public RedisDb(RedisConnectionPool pool, int db, bool throwOnError = true)
        {
            m_DbIndex = Math.Min(Math.Max(db, RedisConstants.MinDbIndex), RedisConstants.MaxDbIndex);

            m_Id = Guid.NewGuid();
            m_Pool = pool;

            ThrowOnError = throwOnError;
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

        public int DbIndex
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

        public RedisConnectionPool Pool
        {
            get
            {
                ValidateNotDisposed();
                return m_Pool;
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

        public bool ThrowOnError { get; private set; }

        #endregion Properties

        #region Methods

        public override void ValidateNotDisposed()
        {
            if (Disposed)
                throw new ObjectDisposedException(GetType().Name + ", " + m_Id.ToString("N"));
        }

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

        protected internal virtual RedisInt ExpectInteger(byte[] cmd, params byte[][] parameters)
        {
            ValidateNotDisposed();
            return Expect<RedisInt>(new RedisCommand(DbIndex, cmd, RedisCommandType.SendAndReceive, parameters), RedisCommandExpect.Integer);
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

        protected internal virtual RedisNullableInt ExpectNullableInteger(byte[] cmd, params byte[][] parameters)
        {
            ValidateNotDisposed();
            return Expect<RedisNullableInt>(new RedisCommand(DbIndex, cmd, RedisCommandType.SendAndReceive, parameters), RedisCommandExpect.NullableInteger);
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
        {
            switch (expectation)
            {
                case RedisCommandExpect.Response:
                    return (T)Pool.Execute(command, ThrowOnError);
                case RedisCommandExpect.Array:
                    return (T)(object)Pool.ExpectArray(command, ThrowOnError);
                case RedisCommandExpect.BulkString:
                    return (T)(object)Pool.ExpectBulkString(command, ThrowOnError);
                case RedisCommandExpect.BulkStringBytes:
                    return (T)(object)Pool.ExpectBulkStringBytes(command, ThrowOnError);
                case RedisCommandExpect.Double:
                    return (T)(object)Pool.ExpectDouble(command, ThrowOnError);
                case RedisCommandExpect.GreaterThanZero:
                    return (T)(object)Pool.ExpectInteger(command, ThrowOnError);
                case RedisCommandExpect.Integer:
                    return (T)(object)Pool.ExpectInteger(command, ThrowOnError);
                case RedisCommandExpect.MultiDataBytes:
                    return (T)(object)Pool.ExpectMultiDataBytes(command, ThrowOnError);
                case RedisCommandExpect.MultiDataStrings:
                    return (T)(object)Pool.ExpectMultiDataStrings(command, ThrowOnError);
                case RedisCommandExpect.Nothing:
                    return (T)(object)Pool.ExpectNothing(command, ThrowOnError);
                case RedisCommandExpect.NullableDouble:
                    return (T)(object)Pool.ExpectNullableDouble(command, ThrowOnError);
                case RedisCommandExpect.NullableInteger:
                    return (T)(object)Pool.ExpectNullableInteger(command, ThrowOnError);
                case RedisCommandExpect.OK:
                    return (T)(object)Pool.ExpectOK(command, ThrowOnError);
                case RedisCommandExpect.One:
                    return (T)(object)Pool.ExpectOne(command, ThrowOnError);
                case RedisCommandExpect.SimpleString:
                    return (T)(object)Pool.ExpectSimpleString(command, ThrowOnError);
                case RedisCommandExpect.SimpleStringBytes:
                    return (T)(object)Pool.ExpectSimpleStringBytes(command, ThrowOnError);
                default:
                    throw new RedisException("Undefined exception");
            }
        }

        #endregion Execution Methods

        #endregion Methods
    }
}
