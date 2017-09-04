using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Sweet.Redis
{
    public class RedisSettings
    {
        # region .Ctors

        public RedisSettings(string host = "127.0.0.1", int port = RedisConstants.DefaultPort, 
            int connectionTimeout = RedisConstants.DefaultConnectionTimeout, 
            int sendTimeout = RedisConstants.DefaultSendTimeout, int receiveTimeout = RedisConstants.DefaultReceiveTimeout,
            int maxCount = RedisConstants.DefaultMaxConnectionCount, int waitTimeout = RedisConstants.DefaultWaitTimeout,
            int waitRetryCount = RedisConstants.DefaultWaitRetryCount, int idleTimeout = RedisConstants.DefaultIdleTimeout,
            int readBufferSize = 0, int writeBufferSize = 0)
        {
            Host = host;
            Port = port;
			ConnectionTimeout = Math.Max(RedisConstants.MinConnectionTimeout, Math.Min(RedisConstants.MaxConnectionTimeout, connectionTimeout));
			IdleTimeout = idleTimeout <= 0 ? 0 : Math.Max(RedisConstants.MinIdleTimeout, Math.Min(RedisConstants.MaxIdleTimeout, idleTimeout));
			MaxCount = Math.Max(1, Math.Min(maxCount, RedisConstants.MaxConnectionCount));
			ReadBufferSize = Math.Max(0, readBufferSize);
			ReceiveTimeout = Math.Max(RedisConstants.MinReceiveTimeout, Math.Min(RedisConstants.MaxReceiveTimeout, receiveTimeout));
			SendTimeout = Math.Max(RedisConstants.MinSendTimeout, Math.Min(RedisConstants.MaxSendTimeout, sendTimeout));
            WaitRetryCount = Math.Max(RedisConstants.MinWaitRetryCount, Math.Min(waitRetryCount, RedisConstants.MaxWaitRetryCount));
            WaitTimeout = Math.Max(RedisConstants.MinWaitTimeout, Math.Min(RedisConstants.MaxWaitTimeout, waitTimeout));
			WriteBufferSize = Math.Max(0, writeBufferSize);
		}

        # endregion .Ctors

        # region Properties

		public int ConnectionTimeout { get; private set; }
		public string Host { get; private set; }
		public int IdleTimeout { get; private set; }
		public int MaxCount { get; private set; }
		public int Port { get; private set; }
		public int ReadBufferSize { get; private set; }
		public int ReceiveTimeout { get; private set; }
		public int SendTimeout { get; private set; }
		public int WaitRetryCount { get; private set; }
		public int WaitTimeout { get; private set; }
        public int WriteBufferSize { get; private set; }

        # endregion Properties
    }
}
