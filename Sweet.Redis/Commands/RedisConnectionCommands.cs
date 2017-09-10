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

            return ExpectOK(RedisCommands.Auth, password.ToBytes());
        }

        public string Echo(string msg)
        {
            if (msg == null)
                throw new ArgumentNullException("msg");

            return ExpectBulkString(RedisCommands.Echo, msg.ToBytes());
        }

        public string Ping()
        {
            return Ping(null);
        }

        public string Ping(string msg)
        {
            if (String.IsNullOrEmpty(msg))
                return ExpectSimpleString(RedisCommands.Ping);
            return ExpectBulkString(RedisCommands.Ping, msg.ToBytes());
        }

        public bool Quit()
        {
            return ExpectOK(RedisCommands.Quit);
        }

        #endregion Methods
    }
}
