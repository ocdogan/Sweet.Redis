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
using System.Globalization;

namespace Sweet.Redis
{
    /*
    name : mymaster
    ip : 127.0.0.1
    port : 6379
    runid : f790ed3ab5f8fa33fa1ea3eb64e3c17103d795c7
    flags : master
    link-pending-commands : 0
    link-refcount : 1
    last-ping-sent : 0
    last-ok-ping-reply : 65
    last-ping-reply : 65
    down-after-milliseconds : 30000
    info-refresh : 8181
    role-reported : master
    role-reported-time : 108592
    */
    public class RedisSentinelNodeInfo : RedisInfoBase
    {
        #region .Ctors

        internal RedisSentinelNodeInfo(string[] infoLines = null)
        {
            if (infoLines != null)
            {
                var length = infoLines.Length;
                if (length > 0)
                {
                    for (var i = 0; i < length; i += 2)
                    {
                        var name = infoLines[i];
                        if (name != null)
                            base[name] = i < length - 1 ? infoLines[i + 1] : null;
                    }
                }
            }
        }

        #endregion .Ctors

        #region Properties

        public string Name { get { return Get("name"); } } // mymaster

        public string Ip { get { return Get("port"); } } // 127.0.0.1

        public long? Port { get { return GetInteger("port"); } } // 6379

        public string RunId { get { return Get("runid"); } } // f790ed3ab5f8fa33fa1ea3eb64e3c17103d795c7

        public string Flags { get { return Get("flags"); } } // master

        public long? LinkPendingCommands { get { return GetInteger("link-pending-commands"); } } // 0

        public long? LinkRefCount { get { return GetInteger("link-refcount"); } } // 1

        public long? LastPingSent { get { return GetInteger("last-ping-sent"); } } // 0

        public long? LastOKPingReply { get { return GetInteger("last-ok-ping-reply"); } } // 65

        public long? LastPingReply { get { return GetInteger("last-ping-reply"); } } // 65

        public long? DownAfterMilliseconds { get { return GetInteger("down-after-milliseconds"); } } // 30000

        public long? InfoRefresh { get { return GetInteger("info-refresh"); } } // 8181

        public string RoleReported { get { return Get("role-reported"); } } // master

        public long? RoleReportedTime { get { return GetInteger("role-reported-time"); } } // 108592

        #endregion Properties
    }
}
