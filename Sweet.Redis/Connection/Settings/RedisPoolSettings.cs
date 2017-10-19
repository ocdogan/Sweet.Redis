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
using System.Net.Security;

namespace Sweet.Redis
{
    public class RedisPoolSettings : RedisConnectionSettings
    {
        #region Static Members

        public new static readonly RedisPoolSettings Default = new RedisPoolSettings((RedisEndPoint[])null);

        #endregion Static Members

        #region .Ctors

        public RedisPoolSettings(string host = RedisConstants.LocalHost, int port = RedisConstants.DefaultPort,
            string masterName = null, string password = null, string clientName = null, int connectionTimeout = RedisConstants.DefaultConnectionTimeout,
            int receiveTimeout = RedisConstants.DefaultReceiveTimeout, int sendTimeout = RedisConstants.DefaultSendTimeout, 
            int maxConnectionCount = RedisConstants.DefaultMaxConnectionCount, int connectionWaitTimeout = RedisConstants.DefaultWaitTimeout,
            int connectionIdleTimeout = RedisConstants.DefaultIdleTimeout, int readBufferSize = 0, int writeBufferSize = 0,
            bool useAsyncCompleter = true, bool useSsl = false,
            LocalCertificateSelectionCallback sslCertificateSelection = null,
            RemoteCertificateValidationCallback sslCertificateValidation = null)
            : this(new[] { new RedisEndPoint(host, port) }, masterName, password, clientName, connectionTimeout, receiveTimeout,
                sendTimeout, maxConnectionCount, connectionWaitTimeout, connectionIdleTimeout, readBufferSize, writeBufferSize,
                useAsyncCompleter, useSsl, sslCertificateSelection, sslCertificateValidation)
        { }

        public RedisPoolSettings(RedisEndPoint[] endPoints = null,
            string masterName = null, string password = null, string clientName = null, int connectionTimeout = RedisConstants.DefaultConnectionTimeout,
            int receiveTimeout = RedisConstants.DefaultReceiveTimeout, int sendTimeout = RedisConstants.DefaultSendTimeout,
            int maxConnectionCount = RedisConstants.DefaultMaxConnectionCount, int connectionWaitTimeout = RedisConstants.DefaultWaitTimeout,
            int connectionIdleTimeout = RedisConstants.DefaultIdleTimeout, int readBufferSize = 0, int writeBufferSize = 0,
            bool useAsyncCompleter = true, bool useSsl = false,
            LocalCertificateSelectionCallback sslCertificateSelection = null,
            RemoteCertificateValidationCallback sslCertificateValidation = null)
            : base(endPoints, masterName, password, clientName, connectionTimeout, receiveTimeout, sendTimeout, 
                   readBufferSize, writeBufferSize, useSsl, sslCertificateSelection, sslCertificateValidation)
        {
            UseAsyncCompleter = useAsyncCompleter;
            ConnectionIdleTimeout = connectionIdleTimeout <= 0 ? 0 : Math.Max(RedisConstants.MinIdleTimeout, Math.Min(RedisConstants.MaxIdleTimeout, connectionIdleTimeout));
            MaxConnectionCount = Math.Max(Math.Min(maxConnectionCount, RedisConstants.MaxConnectionCount), RedisConstants.MinConnectionCount);
            ConnectionWaitTimeout = Math.Max(RedisConstants.MinWaitTimeout, Math.Min(RedisConstants.MaxWaitTimeout, connectionWaitTimeout));
        }

        #endregion .Ctors

        #region Properties

        public int ConnectionIdleTimeout { get; private set; }
        public int ConnectionWaitTimeout { get; private set; }
        public int MaxConnectionCount { get; private set; }
        public bool UseAsyncCompleter { get; private set; }

        # endregion Properties

        #region Methods

        public override RedisConnectionSettings Clone(string host = null, int port = -1)
        {
            return new RedisPoolSettings(host ?? RedisConstants.LocalHost, port < 1 ? RedisConstants.DefaultPort : port,
                MasterName, Password, ClientName, ConnectionTimeout, ReceiveTimeout, SendTimeout,
                MaxConnectionCount, ConnectionWaitTimeout, ConnectionIdleTimeout, ReadBufferSize, WriteBufferSize,
                UseAsyncCompleter, UseSsl, SslCertificateSelection, SslCertificateValidation);
        }

        #endregion Methods
    }
}
