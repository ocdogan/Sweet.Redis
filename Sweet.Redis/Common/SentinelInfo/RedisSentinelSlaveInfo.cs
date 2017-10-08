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
    master-link-down-time : 0
    master-link-status : ok
    master-host : 127.0.0.1
    master-port : 6379
    slave-priority : 100
    slave-repl-offset : 2216974
    */
    public class RedisSentinelSlaveInfo : RedisSentinelNodeInfo
    {
        #region .Ctors

        internal RedisSentinelSlaveInfo(string[] infoLines = null)
            : base(infoLines)
        { }

        #endregion .Ctors

        #region Properties

        public long? MasterLinkDownTime { get { return GetInteger("master-link-down-time"); } } // 0

        public bool? MasterLinkStatus { get { return GetOK("master-link-status"); } } // ok

        public string MasterHost { get { return Get("master-host"); } } // 127.0.0.1

        public long? MasterPort { get { return GetInteger("master-port"); } } // 6379

        public long? SlavePriority { get { return GetInteger("slave-priority"); } } // 100

        public long? SlaveReplOffset { get { return GetInteger("slave-repl-offset"); } } // 2216974

        #endregion Properties
    }
}
