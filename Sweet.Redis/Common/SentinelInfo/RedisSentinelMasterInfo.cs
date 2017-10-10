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
    config-epoch : 4
    num-slaves : 2
    num-other-sentinels : 0
    quorum : 2
    failover-timeout : 180000
    parallel-syncs : 1
    */
    public class RedisSentinelMasterInfo : RedisSentinelInfoBase
    {
        #region .Ctors

        internal RedisSentinelMasterInfo(string[] infoLines = null)
            : base(infoLines)
        { }

        internal RedisSentinelMasterInfo(RedisRawObject rawObject)
            : base(rawObject)
        { }

        #endregion .Ctors

        #region Properties

        public long? ConfigEpoch { get { return GetInteger("config-epoch"); } } // 4

        public long? NumberOfSlaves { get { return GetInteger("num-slaves"); } } // 2

        public long? NumberOfOtherSentinels { get { return GetInteger("num-other-sentinels"); } } // 0

        public long? Quorum { get { return GetInteger("quorum"); } } // 2

        public long? FailoverTimeOut { get { return GetInteger("failover-timeout"); } } // 180000

        public long? ParallelSyncs { get { return GetInteger("parallel-syncs"); } } // 1

        #endregion Properties
    }
}
