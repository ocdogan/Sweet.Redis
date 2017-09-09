using System;
using System.Text;

namespace Sweet.Redis
{
    internal class RedisHyperLogLogCommands : RedisCommandSet, IRedisHyperLogLogCommands
    {
        #region .Ctors

        public RedisHyperLogLogCommands(IRedisDb db)
            : base(db)
        { }

        #endregion .Ctors

        #region Methods

        public bool PfAdd(string key, string element, params string[] elements)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            if (element == null)
                throw new ArgumentNullException("element");

            ValidateNotDisposed();

            var length = elements.Length;
            if (length > 0)
            {
                var parameters = key.ToBytes().Join(element.ToBytes()).Join(elements);

                using (var cmd = new RedisCommand(Db.Db, RedisCommands.PfAdd, parameters))
                {
                    return cmd.ExpectInteger(Db.Pool, Db.ThrowOnError) == 1L;
                }
            }

            using (var cmd = new RedisCommand(Db.Db, RedisCommands.PfAdd, key.ToBytes(), element.ToBytes()))
            {
                return cmd.ExpectInteger(Db.Pool, Db.ThrowOnError) == 1L;
            }
        }

        public long PfCount(string key, params string[] keys)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            ValidateNotDisposed();

            var length = keys.Length;
            if (length > 0)
            {
                var parameters = key.ToBytes().Join(keys);

                using (var cmd = new RedisCommand(Db.Db, RedisCommands.PfAdd, parameters))
                {
                    return cmd.ExpectInteger(Db.Pool, Db.ThrowOnError);
                }
            }

            using (var cmd = new RedisCommand(Db.Db, RedisCommands.PfAdd, key.ToBytes()))
            {
                return cmd.ExpectInteger(Db.Pool, Db.ThrowOnError);
            }
        }

        public bool PfMerge(string destKey, string sourceKey, params string[] sourceKeys)
        {
            if (destKey == null)
                throw new ArgumentNullException("destKey");

            if (sourceKey == null)
                throw new ArgumentNullException("sourceKey");

            ValidateNotDisposed();

            var length = sourceKeys.Length;
            if (length > 0)
            {
                var parameters = destKey.ToBytes().Join(sourceKey.ToBytes()).Join(sourceKeys);

                using (var cmd = new RedisCommand(Db.Db, RedisCommands.Quit, parameters))
                {
                    return cmd.ExpectSimpleString(Db.Pool, "OK", Db.ThrowOnError);
                }
            }

            using (var cmd = new RedisCommand(Db.Db, RedisCommands.Quit, destKey.ToBytes(), sourceKey.ToBytes()))
            {
                return cmd.ExpectSimpleString(Db.Pool, "OK", Db.ThrowOnError);
            }
        }

        #endregion Methods
    }
}
