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
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Sockets;

namespace Sweet.Redis
{
    public class RedisEndPoint
    {
        #region RedisIPAddressEntry

        private class RedisIPAddressEntry
        {
            #region .Ctors

            public RedisIPAddressEntry(string host, IPAddress[] ipAddresses)
            {
                Host = host;
                IPAddresses = ipAddresses;
                CreationDate = DateTime.UtcNow;
            }

            #endregion .Ctors

            #region Properties

            public string Host { get; private set; }

            public IPAddress[] IPAddresses { get; private set; }

            public DateTime CreationDate { get; private set; }

            public bool Expired
            {
                get { return (DateTime.UtcNow - CreationDate).TotalSeconds >= 30d; }
            }

            #endregion Properties

            #region Methods

            public void SetIPAddresses(IPAddress[] ipAddresses)
            {
                IPAddresses = ipAddresses;
                CreationDate = DateTime.UtcNow;
            }

            #endregion Methods
        }

        #endregion RedisIPAddressEntry

        #region Static Members

        private static readonly IPAddress[] EmptyAddresses = new IPAddress[0];

        private static readonly ConcurrentDictionary<string, RedisIPAddressEntry> s_DnsEntries =
            new ConcurrentDictionary<string, RedisIPAddressEntry>();

        #endregion Static Members

        #region Field Members

        private RedisIPAddressEntry m_Entry;

        #endregion Field Members

        #region .Ctors

        public RedisEndPoint(string host, int port)
        {
            Host = host;
            Port = port;
        }

        #endregion .Ctors

        #region Properties

        public string Host { get; private set; }

        public int Port { get; private set; }

        public bool IsEmpty
        {
            get { return String.IsNullOrEmpty(Host) || Port < 1; }
        }

        #endregion Properties

        #region Methods

        public IPAddress[] ResolveHost()
        {
            var entry = m_Entry;
            if (entry == null || entry.Expired)
                entry = m_Entry = GetEntry(Host);

            return (entry == null) ? EmptyAddresses :
                (entry.IPAddresses ?? EmptyAddresses);
        }

        private static RedisIPAddressEntry GetEntry(string host)
        {
            if (!String.IsNullOrEmpty(host))
            {
                RedisIPAddressEntry entry;
                if (!s_DnsEntries.TryGetValue(host, out entry) || entry.Expired)
                {
                    lock (s_DnsEntries)
                    {
                        if (!s_DnsEntries.TryGetValue(host, out entry) || entry.Expired)
                        {
                            IPAddress[] ipAddresses;
                            if (host.Equals(RedisConstants.LocalHost, StringComparison.OrdinalIgnoreCase))
                            {
                                var list = new List<IPAddress>();
                                if (RedisSocket.OSSupportsIPv4)
                                    list.Add(IPAddress.Parse(RedisConstants.IP4Loopback));
                                else if (RedisSocket.OSSupportsIPv6)
                                    list.Add(IPAddress.Parse(RedisConstants.IP6Loopback));

                                ipAddresses = list.ToArray();
                            }
                            else
                            {
                                ipAddresses = RedisAsyncEx.GetHostAddressesAsync(host).Result;
                                if (ipAddresses != null && ipAddresses.Length > 1)
                                {
                                    ipAddresses = ipAddresses
                                        .OrderBy((addr) =>
                                        { return addr.AddressFamily == AddressFamily.InterNetwork ? -1 : 1; })
                                        .ToArray();
                                }
                            }

                            if (entry != null)
                                entry.SetIPAddresses(ipAddresses);
                            else
                                s_DnsEntries[host] = entry = new RedisIPAddressEntry(host, ipAddresses);
                        }
                    }
                }
                return entry;
            }
            return null;
        }

        #endregion Methods
    }
}
