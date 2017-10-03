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

namespace Sweet.Redis
{
    internal class RedisKeysCommands : RedisCommandSet, IRedisKeysCommands
    {
        #region .Ctors

        public RedisKeysCommands(RedisDb db)
            : base(db)
        { }

        #endregion .Ctors

        #region Methods

        public RedisInt Del(RedisParam key, params RedisParam[] keys)
        {
            if (key.IsNull)
                throw new ArgumentNullException("key");

            ValidateNotDisposed();

            if (keys.Length > 0)
            {
                var parameters = key.Join(keys.ToBytesArray());
                return ExpectInteger(RedisCommands.Del, parameters);
            }
            return ExpectInteger(RedisCommands.Del, keys.ToBytesArray());
        }

        public RedisBytes Dump(RedisParam key)
        {
            if (key.IsNull)
                throw new ArgumentNullException("key");

            return ExpectBulkStringBytes(RedisCommands.Dump, key);
        }

        public RedisBool Exists(RedisParam key)
        {
            if (key.IsNull)
                throw new ArgumentNullException("key");

            return ExpectGreaterThanZero(RedisCommands.Exists, key);
        }

        public RedisBool Expire(RedisParam key, int seconds)
        {
            if (key.IsNull)
                throw new ArgumentNullException("key");

            return ExpectGreaterThanZero(RedisCommands.Expire, key, seconds.ToBytes());
        }

        public RedisBool ExpireAt(RedisParam key, int timestamp)
        {
            if (key.IsNull)
                throw new ArgumentNullException("key");

            return ExpectGreaterThanZero(RedisCommands.ExpireAt, key, timestamp.ToBytes());
        }

        public RedisMultiString Keys(RedisParam pattern)
        {
            if (pattern.IsNull)
                throw new ArgumentNullException("pattern");

            return ExpectMultiDataStrings(RedisCommands.Keys, pattern);
        }

        public RedisBool Migrate(RedisParam host, int port, RedisParam key, int destinationDb, long timeoutMs, bool copy = false, 
            bool replace = false, params RedisParam[] keys)
        {
            if (host.IsNull)
                throw new ArgumentNullException("host");

            if (key.IsEmpty)
                throw new ArgumentNullException("key");

            ValidateNotDisposed();

            var parameters = host.ToBytes()
                                 .Join(port.ToBytes())
                                 .Join(!key.IsNull ? key.Data : RedisCommands.EmptyString)
                                 .Join(destinationDb.ToBytes())
                                 .Join(timeoutMs.ToBytes());

            if (copy)
                parameters = parameters.Join(RedisCommands.Copy);

            if (replace)
                parameters = parameters.Join(RedisCommands.Replace);

            if (key.IsNull)
                parameters = parameters
                                 .Join(RedisCommands.Keys)
                                 .Join(keys.ToBytesArray());

            return ExpectOK(RedisCommands.Migrate, parameters);
        }

        public RedisBool Move(RedisParam key, int db)
        {
            if (key.IsNull)
                throw new ArgumentNullException("key");

            return ExpectGreaterThanZero(RedisCommands.Move, key, db.ToBytes());
        }

        public RedisInt ObjectRefCount(RedisParam key)
        {
            if (key.IsNull)
                throw new ArgumentNullException("key");

            return ExpectInteger(RedisCommands.Object, RedisCommands.RefCount, key);
        }

        public RedisBytes ObjectEncoding(RedisParam key)
        {
            if (key.IsNull)
                throw new ArgumentNullException("key");

            return ExpectBulkStringBytes(RedisCommands.Object, RedisCommands.Encoding, key);
        }

        public RedisString ObjectEncodingString(RedisParam key)
        {
            if (key.IsNull)
                throw new ArgumentNullException("key");

            return ExpectBulkString(RedisCommands.Object, RedisCommands.Encoding, key);
        }

        public RedisInt ObjectIdleTime(RedisParam key)
        {
            if (key.IsNull)
                throw new ArgumentNullException("key");

            return ExpectInteger(RedisCommands.Object, RedisCommands.IdleTime, key);
        }

        public RedisBool Persist(RedisParam key)
        {
            if (key.IsNull)
                throw new ArgumentNullException("key");

            return ExpectGreaterThanZero(RedisCommands.Persist, key);
        }

        public RedisBool PExpire(RedisParam key, long milliseconds)
        {
            if (key.IsNull)
                throw new ArgumentNullException("key");

            return ExpectGreaterThanZero(RedisCommands.PExpire, key, milliseconds.ToBytes());
        }

        public RedisBool PExpireAt(RedisParam key, long millisecondsTimestamp)
        {
            if (key.IsNull)
                throw new ArgumentNullException("key");

            return ExpectGreaterThanZero(RedisCommands.PExpireAt, key, millisecondsTimestamp.ToBytes());
        }

        public RedisInt PTtl(RedisParam key)
        {
            if (key.IsNull)
                throw new ArgumentNullException("key");

            return ExpectInteger(RedisCommands.PTtl, key);
        }

        public RedisString RandomKey()
        {
            return ExpectBulkString(RedisCommands.RandomKey);
        }

        public RedisBool Rename(RedisParam key, RedisParam newKey)
        {
            if (key.IsNull)
                throw new ArgumentNullException("key");

            if (newKey.IsNull)
                throw new ArgumentNullException("newKey");

            return ExpectOK(RedisCommands.Rename, key, newKey);
        }

        public RedisBool RenameNx(RedisParam key, RedisParam newKey)
        {
            if (key.IsNull)
                throw new ArgumentNullException("key");

            if (newKey.IsNull)
                throw new ArgumentNullException("newKey");

            return ExpectGreaterThanZero(RedisCommands.RenameNx, key, newKey);
        }

        public RedisBool Restore(RedisParam key, long ttl, RedisParam value)
        {
            ValidateNotDisposed();
            ValidateKeyAndValue(key, value);

            return ExpectOK(RedisCommands.Rename, key, ttl.ToBytes(), value);
        }

        public RedisMultiBytes Scan(int count = 10, RedisParam? match = null)
        {
            throw new NotImplementedException();
        }

        public RedisMultiString ScanString(int count = 10, RedisParam? match = null)
        {
            throw new NotImplementedException();
        }

        public RedisMultiBytes Sort(RedisParam key, bool descending, bool alpha = false,
                      int start = -1, int end = -1, RedisParam? by = null, RedisParam? get = null)
        {
            if (key.IsNull)
                throw new ArgumentNullException("key");

            ValidateNotDisposed();

            var parameters = new byte[1][] { key.Data };

            if (descending)
                parameters = parameters.Join(RedisCommands.Descending);

            if (alpha)
                parameters = parameters.Join(RedisCommands.Alpha);

            if (start > -1 && end > -1)
                parameters = parameters
                    .Join(RedisCommands.Limit)
                    .Join(start.ToBytes())
                    .Join(end.ToBytes());

            if (by.HasValue && !by.Value.IsEmpty)
                parameters = parameters
                    .Join(RedisCommands.By)
                    .Join(by);

            if (get.HasValue && !get.Value.IsEmpty)
                parameters = parameters
                    .Join(RedisCommands.Get)
                    .Join(get);

            return ExpectMultiDataBytes(RedisCommands.Sort, parameters);
        }

        public RedisInt Touch(RedisParam key, params RedisParam[] keys)
        {
            if (key.IsNull)
                throw new ArgumentNullException("key");

            ValidateNotDisposed();

            if (keys.Length > 0)
            {
                var parameters = key
                                    .Join(keys.ToBytesArray());

                return ExpectInteger(RedisCommands.Touch, parameters);
            }
            return ExpectInteger(RedisCommands.Touch, keys.ToBytesArray());
        }

        public RedisInt Ttl(RedisParam key)
        {
            if (key.IsNull)
                throw new ArgumentNullException("key");

            return ExpectInteger(RedisCommands.Ttl, key);
        }

        public RedisString Type(RedisParam key)
        {
            if (key.IsNull)
                throw new ArgumentNullException("key");

            return ExpectSimpleString(RedisCommands.Type, key.Data);
        }

        public RedisInt Wait(int numberOfSlaves, int timeout)
        {
            return ExpectInteger(RedisCommands.Ttl, numberOfSlaves.ToBytes(), timeout.ToBytes());
        }

        #endregion Methods
    }
}
