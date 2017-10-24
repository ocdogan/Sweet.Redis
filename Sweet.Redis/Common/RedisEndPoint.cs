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
    public class RedisEndPoint : IEquatable<RedisEndPoint>, ICloneable
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

        public static readonly RedisEndPoint Empty = new RedisEndPoint(null, -1);

        public static readonly RedisEndPoint LocalHostEndPoint = new RedisEndPoint(RedisConstants.LocalHost, RedisConstants.DefaultPort);
        public static readonly RedisEndPoint IP4LoopbackEndPoint = new RedisEndPoint(RedisConstants.IP4Loopback, RedisConstants.DefaultPort);
        public static readonly RedisEndPoint IP6LoopbackEndPoint = new RedisEndPoint(RedisConstants.IP6Loopback, RedisConstants.DefaultPort);

        public static readonly RedisEndPoint SentinelLocalHostEndPoint = new RedisEndPoint(RedisConstants.LocalHost, RedisConstants.DefaultSentinelPort);
        public static readonly RedisEndPoint SentinelIP4LoopbackEndPoint = new RedisEndPoint(RedisConstants.IP4Loopback, RedisConstants.DefaultSentinelPort);
        public static readonly RedisEndPoint SentinelIP6LoopbackEndPoint = new RedisEndPoint(RedisConstants.IP6Loopback, RedisConstants.DefaultSentinelPort);

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

        #region Overrides

        public override string ToString()
        {
            return String.Format("[RedisEndPoint: Host={0}, Port={1}, IsEmpty={2}]", Host, Port, IsEmpty);
        }

        public override int GetHashCode()
        {
            var hash = 13;
            hash = (hash * 7) + (Host ?? String.Empty).GetHashCode();
            hash = (hash * 7) + Port.GetHashCode();
            return hash;
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(obj, null))
                return false;

            if (ReferenceEquals(obj, this))
                return true;

            var other = obj as RedisEndPoint;
            if (!ReferenceEquals(obj, null))
                return Port == other.Port &&
                     String.Equals(Host, other.Host, StringComparison.OrdinalIgnoreCase);

            return false;
        }


        public bool Equals(RedisEndPoint other)
        {
            if (ReferenceEquals(other, null))
                return false;

            if (ReferenceEquals(other, this))
                return true;

            return Port == other.Port &&
                 String.Equals(Host, other.Host, StringComparison.OrdinalIgnoreCase);
        }

        #endregion Overrides

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

        public static HashSet<IPEndPoint> ToIPEndPoints(RedisEndPoint[] endPoints)
        {
            if (endPoints != null && endPoints.Length > 0)
            {
                var ipEPList = new HashSet<IPEndPoint>();
                foreach (var ep in endPoints)
                {
                    if (ep != null && !ep.IsEmpty)
                    {
                        try
                        {
                            var ipAddresses = ep.ResolveHost();
                            if (ipAddresses != null)
                            {
                                var length = ipAddresses.Length;
                                if (length > 0)
                                {
                                    for (var i = 0; i < length; i++)
                                        ipEPList.Add(new IPEndPoint(ipAddresses[i], ep.Port));
                                }
                            }
                        }
                        catch (Exception)
                        { }
                    }
                }

                return ipEPList;
            }
            return null;
        }

        public object Clone()
        {
            if (ReferenceEquals(this, Empty))
                return this;
            return new RedisEndPoint(Host, Port);
        }

        #endregion Methods
    }
}
