namespace Sweet.Redis
{
    public static class RedisConstants
    {
        #region Static Members

        public static readonly byte[] EmptyBytes = new byte[0];
        public static readonly byte[] LineEnd = new byte[] { (byte)'\r', (byte)'\n' };

        #endregion Static Members

        #region Constants

        public const int DefaultBufferSize = 4 * 1024;

        public const int MaxValueLength = 1024 * 1024 * 1024; // 1 GB

        public const int IdleTimerPeriod = 10000; // milliseconds

        public const int MaxDbNo = 16;
        public const int DefaultPort = 6379;

        public const int DefaultConnectionTimeout = 10000;
        public const int MinConnectionTimeout = 100;
        public const int MaxConnectionTimeout = 60000;

        public const int MaxConnectionCount = 1000;
        public const int DefaultMaxConnectionCount = 100;

        public const int DefaultWaitTimeout = 5000;
        public const int MinWaitTimeout = 1000;
        public const int MaxWaitTimeout = 30000;

        public const int DefaultWaitRetryCount = 3;
        public const int MinWaitRetryCount = 1;
        public const int MaxWaitRetryCount = 10;

        public const int DefaultIdleTimeout = 300;
        public const int MinIdleTimeout = 10;
        public const int MaxIdleTimeout = 3600;

        public const int DefaultSendTimeout = 15000;
        public const int MinSendTimeout = 100;
        public const int MaxSendTimeout = 60000;

        public const int DefaultReceiveTimeout = 15000;
        public const int MinReceiveTimeout = 100;
        public const int MaxReceiveTimeout = 60000;

        #endregion Constants
    }
}
