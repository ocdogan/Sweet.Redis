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
            using (var cmd = new RedisCommand(RedisCommands.Auth, password.ToBytes()))
            {
                return cmd.ExpectSimpleString(Db.Pool, "OK", true);
            }
        }

        public string Echo(string msg)
        {
            if (msg == null)
                throw new ArgumentNullException("msg");

            ValidateNotDisposed();
            using (var cmd = new RedisCommand(RedisCommands.Echo, msg.ToBytes()))
            {
                return cmd.ExpectBulkString(Db.Pool, true);
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
                using (var cmd = new RedisCommand(RedisCommands.Ping))
                {
                    return cmd.ExpectSimpleString(Db.Pool, true);
                }

            using (var cmd = new RedisCommand(RedisCommands.Ping, msg.ToBytes()))
            {
                return cmd.ExpectBulkString(Db.Pool, true);
            }
        }

        public bool Quit()
        {
            ValidateNotDisposed();
            using (var cmd = new RedisCommand(RedisCommands.Quit))
            {
                return cmd.ExpectSimpleString(Db.Pool, "OK", true);
            }
        }

        #endregion Methods
    }
}
