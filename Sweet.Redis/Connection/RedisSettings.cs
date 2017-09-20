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
    public class RedisSettings
    {
        #region Static Members

        public static readonly RedisSettings Default = new RedisSettings();

        #endregion Static Members

        #region .Ctors

        public RedisSettings(string host = "127.0.0.1", int port = RedisConstants.DefaultPort,
            string password = null, int connectionTimeout = RedisConstants.DefaultConnectionTimeout,
            int sendTimeout = RedisConstants.DefaultSendTimeout, int receiveTimeout = RedisConstants.DefaultReceiveTimeout,
            int maxCount = RedisConstants.DefaultMaxConnectionCount, int waitTimeout = RedisConstants.DefaultWaitTimeout,
            int waitRetryCount = RedisConstants.DefaultWaitRetryCount, int idleTimeout = RedisConstants.DefaultIdleTimeout,
            int readBufferSize = 0, int writeBufferSize = 0)
        {
            Host = host;
            Port = port;
            Password = password;
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
        public string Password { get; private set; }
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
