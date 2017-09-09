﻿using System;

namespace Sweet.Redis
{
    internal class RedisSetsCommands : RedisCommandSet, IRedisSetsCommands
    {
        #region .Ctors

        public RedisSetsCommands(IRedisDb db)
            : base(db)
        { }

        #endregion .Ctors

        #region Methods

        public long SAdd(string key, byte[] member, params byte[][] members)
        {
            ValidateNotDisposed();
            ValidateKeyAndValue(key, member);

            if (members != null && members.Length > 0)
            {
                var parameters = key.ToBytes()
                                    .Merge(member)
                                    .Merge(members);

                using (var cmd = new RedisCommand(Db.Db, RedisCommands.SAdd, parameters))
                {
                    return cmd.ExpectInteger(Db.Pool, true);
                }
            }
            using (var cmd = new RedisCommand(Db.Db, RedisCommands.SAdd, key.ToBytes(), member))
            {
                return cmd.ExpectInteger(Db.Pool, true);
            }
        }

        public long SAdd(string key, string member, params string[] members)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            ValidateNotDisposed();

            if (members != null && members.Length > 0)
            {
                var parameters = key.ToBytes()
                                    .Merge(member.ToBytes())
                                    .Merge(members.ToBytesArray());

                using (var cmd = new RedisCommand(Db.Db, RedisCommands.SAdd, parameters))
                {
                    return cmd.ExpectInteger(Db.Pool, true);
                }
            }
            using (var cmd = new RedisCommand(Db.Db, RedisCommands.SAdd, key.ToBytes(), member.ToBytes()))
            {
                return cmd.ExpectInteger(Db.Pool, true);
            }
        }

        public long SCard(string key)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            ValidateNotDisposed();
            using (var cmd = new RedisCommand(Db.Db, RedisCommands.SCard, key.ToBytes()))
            {
                return cmd.ExpectInteger(Db.Pool, true);
            }
        }

        public byte[][] SDiff(string fromKey, params string[] keys)
        {
            if (fromKey == null)
                throw new ArgumentNullException("fromKey");

            ValidateNotDisposed();

            if (keys != null && keys.Length > 0)
            {
                var parameters = fromKey.ToBytes()
                                    .Merge(keys.ToBytesArray());

                using (var cmd = new RedisCommand(Db.Db, RedisCommands.SDiff, parameters))
                {
                    return cmd.ExpectMultiDataBytes(Db.Pool, true);
                }
            }
            using (var cmd = new RedisCommand(Db.Db, RedisCommands.SDiff, fromKey.ToBytes()))
            {
                return cmd.ExpectMultiDataBytes(Db.Pool, true);
            }
        }

        public long SDiffStore(string toKey, string fromKey, params string[] keys)
        {
            if (toKey == null)
                throw new ArgumentNullException("toKey");

            if (fromKey == null)
                throw new ArgumentNullException("fromKey");

            ValidateNotDisposed();

            if (keys != null && keys.Length > 0)
            {
                var parameters = toKey.ToBytes()
                                      .Merge(fromKey.ToBytes())
                                      .Merge(keys.ToBytesArray());

                using (var cmd = new RedisCommand(Db.Db, RedisCommands.SDiffStore, parameters))
                {
                    return cmd.ExpectInteger(Db.Pool, true);
                }
            }
            using (var cmd = new RedisCommand(Db.Db, RedisCommands.SDiffStore, toKey.ToBytes(), fromKey.ToBytes()))
            {
                return cmd.ExpectInteger(Db.Pool, true);
            }
        }

        public string[] SDiffString(string fromKey, params string[] keys)
        {
            if (fromKey == null)
                throw new ArgumentNullException("fromKey");

            ValidateNotDisposed();

            if (keys != null && keys.Length > 0)
            {
                var parameters = fromKey.ToBytes()
                                    .Merge(keys.ToBytesArray());

                using (var cmd = new RedisCommand(Db.Db, RedisCommands.SDiff, parameters))
                {
                    return cmd.ExpectMultiDataStrings(Db.Pool, true);
                }
            }
            using (var cmd = new RedisCommand(Db.Db, RedisCommands.SDiff, fromKey.ToBytes()))
            {
                return cmd.ExpectMultiDataStrings(Db.Pool, true);
            }
        }

        public byte[][] SInter(string key, params string[] keys)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            ValidateNotDisposed();

            if (keys != null && keys.Length > 0)
            {
                var parameters = key.ToBytes()
                                    .Merge(keys.ToBytesArray());

                using (var cmd = new RedisCommand(Db.Db, RedisCommands.SInter, parameters))
                {
                    return cmd.ExpectMultiDataBytes(Db.Pool, true);
                }
            }
            using (var cmd = new RedisCommand(Db.Db, RedisCommands.SDiff, key.ToBytes()))
            {
                return cmd.ExpectMultiDataBytes(Db.Pool, true);
            }
        }

        public long SInterStore(string toKey, params string[] keys)
        {
            if (toKey == null)
                throw new ArgumentNullException("toKey");

            ValidateNotDisposed();

            if (keys != null && keys.Length > 0)
            {
                var parameters = toKey.ToBytes()
                                      .Merge(keys.ToBytesArray());

                using (var cmd = new RedisCommand(Db.Db, RedisCommands.SInterStore, parameters))
                {
                    return cmd.ExpectInteger(Db.Pool, true);
                }
            }
            using (var cmd = new RedisCommand(Db.Db, RedisCommands.SInterStore, toKey.ToBytes()))
            {
                return cmd.ExpectInteger(Db.Pool, true);
            }
        }

        public string[] SInterStrings(string key, params string[] keys)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            ValidateNotDisposed();

            if (keys != null && keys.Length > 0)
            {
                var parameters = key.ToBytes()
                                    .Merge(keys.ToBytesArray());

                using (var cmd = new RedisCommand(Db.Db, RedisCommands.SInter, parameters))
                {
                    return cmd.ExpectMultiDataStrings(Db.Pool, true);
                }
            }
            using (var cmd = new RedisCommand(Db.Db, RedisCommands.SDiff, key.ToBytes()))
            {
                return cmd.ExpectMultiDataStrings(Db.Pool, true);
            }
        }

        public bool SIsMember(string key, byte[] member)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            ValidateNotDisposed();
            using (var cmd = new RedisCommand(Db.Db, RedisCommands.SIsMember, key.ToBytes(), member))
            {
                return cmd.ExpectInteger(Db.Pool, true) > 0;
            }
        }

        public bool SIsMember(string key, string member)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            ValidateNotDisposed();
            using (var cmd = new RedisCommand(Db.Db, RedisCommands.SIsMember, key.ToBytes(), member.ToBytes()))
            {
                return cmd.ExpectInteger(Db.Pool, true) > 0;
            }
        }

        public byte[][] SMembers(string key)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            ValidateNotDisposed();
            using (var cmd = new RedisCommand(Db.Db, RedisCommands.SMembers, key.ToBytes()))
            {
                return cmd.ExpectMultiDataBytes(Db.Pool, true);
            }
        }

        public string[] SMemberStrings(string key)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            ValidateNotDisposed();
            using (var cmd = new RedisCommand(Db.Db, RedisCommands.SMembers, key.ToBytes()))
            {
                return cmd.ExpectMultiDataStrings(Db.Pool, true);
            }
        }

        public bool SMove(string fromKey, string toKey, byte[] member)
        {
            if (fromKey == null)
                throw new ArgumentNullException("fromKey");

            if (toKey == null)
                throw new ArgumentNullException("toKey");

            ValidateNotDisposed();
            using (var cmd = new RedisCommand(Db.Db, RedisCommands.SMove, fromKey.ToBytes(), toKey.ToBytes(), member))
            {
                return cmd.ExpectInteger(Db.Pool, true) > 0;
            }
        }

        public bool SMove(string fromKey, string toKey, string member)
        {
            if (fromKey == null)
                throw new ArgumentNullException("fromKey");

            if (toKey == null)
                throw new ArgumentNullException("toKey");

            ValidateNotDisposed();
            using (var cmd = new RedisCommand(Db.Db, RedisCommands.SMove, fromKey.ToBytes(), toKey.ToBytes(), member.ToBytes()))
            {
                return cmd.ExpectInteger(Db.Pool, true) > 0;
            }
        }

        public byte[] SPop(string key)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            ValidateNotDisposed();
            using (var cmd = new RedisCommand(Db.Db, RedisCommands.SPop, key.ToBytes()))
            {
                return cmd.ExpectBulkStringBytes(Db.Pool, true);
            }
        }

        public string SPopString(string key)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            ValidateNotDisposed();
            using (var cmd = new RedisCommand(Db.Db, RedisCommands.SPop, key.ToBytes()))
            {
                return cmd.ExpectBulkString(Db.Pool, true);
            }
        }

        public byte[] SRandMember(string key)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            ValidateNotDisposed();
            using (var cmd = new RedisCommand(Db.Db, RedisCommands.SRandMember, key.ToBytes()))
            {
                return cmd.ExpectBulkStringBytes(Db.Pool, true);
            }
        }

        public byte[][] SRandMember(string key, int count)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            ValidateNotDisposed();
            using (var cmd = new RedisCommand(Db.Db, RedisCommands.SRandMember, key.ToBytes(), count.ToBytes()))
            {
                return cmd.ExpectMultiDataBytes(Db.Pool, true);
            }
        }

        public string SRandMemberString(string key)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            ValidateNotDisposed();
            using (var cmd = new RedisCommand(Db.Db, RedisCommands.SRandMember, key.ToBytes()))
            {
                return cmd.ExpectBulkString(Db.Pool, true);
            }
        }

        public string[] SRandMemberString(string key, int count)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            ValidateNotDisposed();
            using (var cmd = new RedisCommand(Db.Db, RedisCommands.SRandMember, key.ToBytes(), count.ToBytes()))
            {
                return cmd.ExpectMultiDataStrings(Db.Pool, true);
            }
        }

        public long SRem(string key, byte[] member, params byte[][] members)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            if (member == null)
                throw new ArgumentNullException("member");

            ValidateNotDisposed();

            if (members != null && members.Length > 0)
            {
                var parameters = key.ToBytes()
                                      .Merge(member.ToBytes())
                                      .Merge(members);

                using (var cmd = new RedisCommand(Db.Db, RedisCommands.SRem, parameters))
                {
                    return cmd.ExpectInteger(Db.Pool, true);
                }
            }
            using (var cmd = new RedisCommand(Db.Db, RedisCommands.SRem, key.ToBytes(), member))
            {
                return cmd.ExpectInteger(Db.Pool, true);
            }
        }

        public long SRem(string key, string member, params string[] members)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            if (member == null)
                throw new ArgumentNullException("member");

            ValidateNotDisposed();

            if (members != null && members.Length > 0)
            {
                var parameters = key.ToBytes()
                                    .Merge(member.ToBytes())
                                    .Merge(members.ToBytesArray());

                using (var cmd = new RedisCommand(Db.Db, RedisCommands.SRem, parameters))
                {
                    return cmd.ExpectInteger(Db.Pool, true);
                }
            }
            using (var cmd = new RedisCommand(Db.Db, RedisCommands.SRem, key.ToBytes(), member.ToBytes()))
            {
                return cmd.ExpectInteger(Db.Pool, true);
            }
        }

        public byte[][] SScan(string key, int count = 10, string match = null)
        {
            throw new NotImplementedException();
        }

        public string[] SScanString(string key, int count = 10, string match = null)
        {
            throw new NotImplementedException();
        }

        public byte[][] SUnion(string key, params string[] keys)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            ValidateNotDisposed();

            if (keys != null && keys.Length > 0)
            {
                var parameters = key.ToBytes()
                                    .Merge(keys.ToBytesArray());

                using (var cmd = new RedisCommand(Db.Db, RedisCommands.SUnion, parameters))
                {
                    return cmd.ExpectMultiDataBytes(Db.Pool, true);
                }
            }
            using (var cmd = new RedisCommand(Db.Db, RedisCommands.SUnion, key.ToBytes()))
            {
                return cmd.ExpectMultiDataBytes(Db.Pool, true);
            }
        }

        public long SUnionStore(string toKey, params string[] keys)
        {
            if (toKey == null)
                throw new ArgumentNullException("toKey");

            ValidateNotDisposed();

            if (keys != null && keys.Length > 0)
            {
                var parameters = toKey.ToBytes()
                                    .Merge(keys.ToBytesArray());

                using (var cmd = new RedisCommand(Db.Db, RedisCommands.SUnionStore, parameters))
                {
                    return cmd.ExpectInteger(Db.Pool, true);
                }
            }
            using (var cmd = new RedisCommand(Db.Db, RedisCommands.SUnionStore, toKey.ToBytes()))
            {
                return cmd.ExpectInteger(Db.Pool, true);
            }
        }

        public string[] SUnionStrings(string key, params string[] keys)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            ValidateNotDisposed();

            if (keys != null && keys.Length > 0)
            {
                var parameters = key.ToBytes()
                                    .Merge(keys.ToBytesArray());

                using (var cmd = new RedisCommand(Db.Db, RedisCommands.SUnion, parameters))
                {
                    return cmd.ExpectMultiDataStrings(Db.Pool, true);
                }
            }
            using (var cmd = new RedisCommand(Db.Db, RedisCommands.SUnion, key.ToBytes()))
            {
                return cmd.ExpectMultiDataStrings(Db.Pool, true);
            }
        }

        #endregion Methods
    }
}