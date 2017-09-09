using System;

namespace Sweet.Redis
{
    internal class RedisConnectionCommands : RedisCommandSet, IRedisConnectionCommands
    {
        #region .Ctors

        public RedisConnectionCommands(IRedisDb db)
            : base(db)
        { }

        #endregion .Ctors

        #region Methods

        public bool Auth(string password)
        {
            if (password == null)
                throw new ArgumentNullException("password");

            ValidateNotDisposed();
            using (var cmd = new RedisCommand(Db.Db, RedisCommands.Auth, password.ToBytes()))
            {
                return cmd.ExpectSimpleString(Db.Pool, "OK", Db.ThrowOnError);
            }
        }

        public string Echo(string msg)
        {
            if (msg == null)
                throw new ArgumentNullException("msg");

            ValidateNotDisposed();
            using (var cmd = new RedisCommand(Db.Db, RedisCommands.Echo, msg.ToBytes()))
            {
                return cmd.ExpectBulkString(Db.Pool, Db.ThrowOnError);
            }
        }

        public string Ping()
        {
            return Ping(null);
        }

        public string Ping(string msg)
        {
            ValidateNotDisposed();
            if (String.IsNullOrEmpty(msg))
                using (var cmd = new RedisCommand(Db.Db, RedisCommands.Ping))
                {
                    return cmd.ExpectSimpleString(Db.Pool, Db.ThrowOnError);
                }

            using (var cmd = new RedisCommand(Db.Db, RedisCommands.Ping, msg.ToBytes()))
            {
                return cmd.ExpectBulkString(Db.Pool, Db.ThrowOnError);
            }
        }

        public bool Quit()
        {
            ValidateNotDisposed();
            using (var cmd = new RedisCommand(Db.Db, RedisCommands.Quit))
            {
                return cmd.ExpectSimpleString(Db.Pool, "OK", Db.ThrowOnError);
            }
        }

        #endregion Methods
    }
}
