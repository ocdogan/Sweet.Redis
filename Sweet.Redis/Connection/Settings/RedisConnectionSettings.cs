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
using System.Collections.Generic;
using System.Linq;
using System.Net.Security;

namespace Sweet.Redis
{
    public class RedisConnectionSettings
    {
        #region Static Members

        public static readonly RedisConnectionSettings Default = new RedisConnectionSettings((RedisEndPoint[])null);

        #endregion Static Members

        #region .Ctors

        public RedisConnectionSettings(string host = RedisConstants.LocalHost,
            int port = RedisConstants.DefaultPort,
            string masterName = null, string password = null, string clientName = null,
            int connectionTimeout = RedisConstants.DefaultConnectionTimeout,
            int receiveTimeout = RedisConstants.DefaultReceiveTimeout,
            int sendTimeout = RedisConstants.DefaultSendTimeout,
            int connectionWaitTimeout = RedisConstants.DefaultWaitTimeout,
            int readBufferSize = 0, int writeBufferSize = 0,
            bool useSsl = false,
            LocalCertificateSelectionCallback sslCertificateSelection = null,
            RemoteCertificateValidationCallback sslCertificateValidation = null)
            : this(new[] { new RedisEndPoint(host, port) }, masterName, password, clientName, connectionTimeout, receiveTimeout,
                sendTimeout, connectionWaitTimeout, readBufferSize, writeBufferSize, useSsl, sslCertificateSelection, sslCertificateValidation)
        { }

        public RedisConnectionSettings(HashSet<RedisEndPoint> endPoints = null,
            string masterName = null, string password = null, string clientName = null,
            int connectionTimeout = RedisConstants.DefaultConnectionTimeout,
            int receiveTimeout = RedisConstants.DefaultReceiveTimeout,
            int sendTimeout = RedisConstants.DefaultSendTimeout,
            int connectionWaitTimeout = RedisConstants.DefaultWaitTimeout,
            int readBufferSize = 0, int writeBufferSize = 0,
            bool useSsl = false,
            LocalCertificateSelectionCallback sslCertificateSelection = null,
            RemoteCertificateValidationCallback sslCertificateValidation = null)
            : this(ToEndPointList(endPoints), masterName, password, clientName, connectionTimeout, receiveTimeout,
                sendTimeout, connectionWaitTimeout, readBufferSize, writeBufferSize, useSsl, sslCertificateSelection, sslCertificateValidation)
        { }

        public RedisConnectionSettings(RedisEndPoint[] endPoints = null,
            string masterName = null, string password = null, string clientName = null,
            int connectionTimeout = RedisConstants.DefaultConnectionTimeout,
            int receiveTimeout = RedisConstants.DefaultReceiveTimeout,
            int sendTimeout = RedisConstants.DefaultSendTimeout,
            int connectionWaitTimeout = RedisConstants.DefaultWaitTimeout,
            int readBufferSize = 0, int writeBufferSize = 0,
            bool useSsl = false,
            LocalCertificateSelectionCallback sslCertificateSelection = null,
            RemoteCertificateValidationCallback sslCertificateValidation = null)
        {
            EndPoints = (endPoints != null && endPoints.Length > 0) ? endPoints :
                new[] { new RedisEndPoint(RedisConstants.LocalHost, RedisConstants.DefaultPort) };
            UseSsl = useSsl;
            Password = password;
            ClientName = clientName;
            MasterName = masterName;
            SslCertificateSelection = sslCertificateSelection;
            SslCertificateValidation = sslCertificateValidation;
            ConnectionTimeout = Math.Max(RedisConstants.MinConnectionTimeout, Math.Min(RedisConstants.MaxConnectionTimeout, connectionTimeout));
            ConnectionWaitTimeout = Math.Max(RedisConstants.MinWaitTimeout, Math.Min(RedisConstants.MaxWaitTimeout, connectionWaitTimeout));
            ReceiveTimeout = Math.Max(RedisConstants.MinReceiveTimeout, Math.Min(RedisConstants.MaxReceiveTimeout, receiveTimeout));
            SendTimeout = Math.Max(RedisConstants.MinSendTimeout, Math.Min(RedisConstants.MaxSendTimeout, sendTimeout));
            ReadBufferSize = Math.Max(0, readBufferSize);
            WriteBufferSize = Math.Max(0, writeBufferSize);
        }

        #endregion .Ctors

        #region Properties

        public string ClientName { get; private set; }

        public int ConnectionTimeout { get; private set; }

        public int ConnectionWaitTimeout { get; private set; }

        public RedisEndPoint[] EndPoints { get; private set; }

        public string MasterName { get; private set; }

        public string Password { get; private set; }

        public int ReadBufferSize { get; private set; }

        public int ReceiveTimeout { get; private set; }

        public int SendTimeout { get; private set; }

        public LocalCertificateSelectionCallback SslCertificateSelection { get; private set; }

        public RemoteCertificateValidationCallback SslCertificateValidation { get; private set; }

        public int WriteBufferSize { get; private set; }

        public bool UseSsl { get; private set; }

        #endregion Properties

        #region Methods

        protected static RedisEndPoint[] ToEndPointList(HashSet<RedisEndPoint> endPoints, int defaultPort = RedisConstants.DefaultPort)
        {
            if (endPoints != null)
            {
                var count = endPoints.Count;
                if (count > 0)
                {
                    var result = endPoints.Where(ep => ep != null && !ep.IsEmpty).ToArray();
                    if (result != null && result.Length > 0)
                        return result;
                }
            }
            return new[] { new RedisEndPoint(RedisConstants.LocalHost, defaultPort) };
        }

        public virtual RedisConnectionSettings Clone(string host = null, int port = -1)
        {
            return new RedisConnectionSettings(host ?? RedisConstants.LocalHost, port < 1 ? RedisConstants.DefaultPort : port,
                MasterName, Password, ClientName, ConnectionTimeout, ReceiveTimeout, SendTimeout, ConnectionWaitTimeout, ReadBufferSize, 
                WriteBufferSize, UseSsl, SslCertificateSelection, SslCertificateValidation);
        }

        #endregion Methods
    }
}
