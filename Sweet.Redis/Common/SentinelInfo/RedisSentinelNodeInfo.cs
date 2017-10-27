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

using System.Collections.Generic;

namespace Sweet.Redis
{
    /*
    name : mymaster
    ip : 127.0.0.1
    port : 6379
    runid : f790ed3ab5f8fa33fa1ea3eb64e3c17103d795c7
    flags : master
    pending-commands : 0
    last-ping-sent : 0
    last-ok-ping-reply : 65
    last-ping-reply : 65
    down-after-milliseconds : 30000
    last-hello-message : 399
    voted-leader : ?
    voted-leader-epoch : 0
    */
    public class RedisSentinelNodeInfo : RedisSentinelInfoBase
    {
        #region .Ctors

        internal RedisSentinelNodeInfo(string[] infoLines = null)
            : base(infoLines)
        { }

        internal RedisSentinelNodeInfo(RedisRawObject rawObject)
            : base(rawObject)
        { }

        #endregion .Ctors

        #region Properties

        public long? PendingCommands { get { return GetInteger("pending-commands"); } } // 0

        public long? LastHelloMessage { get { return GetInteger("last-hello-message"); } } // 399

        public long? InfoRefresh { get { return GetInteger("info-refresh"); } } // 8181

        public string VotedLeader { get { return Get("voted-leader"); } } // ?

        public long? VotedLeaderEpoch { get { return GetInteger("voted-leader-epoch"); } } // 0

        #endregion Properties

        #region Methods

        public static RedisSentinelNodeInfo[] ParseInfoResponse(RedisRawObject rawObject)
        {
            if (!ReferenceEquals(rawObject, null) &&
                rawObject.Type == RedisRawObjectType.Array)
            {
                var items = rawObject.Items;
                if (items != null)
                {
                    var count = items.Count;
                    if (count > 0)
                    {
                        var isInfoArray = true;
                        for (var i = 0; i < count; i++)
                        {
                            var item = items[i];
                            if (!ReferenceEquals(item, null) &&
                                item.Type != RedisRawObjectType.Array)
                            {
                                isInfoArray = false;
                                break;
                            }
                        }

                        if (!isInfoArray)
                        {
                            var info = new RedisSentinelNodeInfo(rawObject);
                            return new[] { info };
                        }

                        var list = new List<RedisSentinelNodeInfo>(count);
                        for (var i = 0; i < count; i++)
                        {
                            var item = items[i];
                            if (!ReferenceEquals(item, null) && item.Type == RedisRawObjectType.Array)
                            {
                                var info = new RedisSentinelNodeInfo(item);
                                list.Add(info);
                            }
                        }

                        if (list.Count > 0)
                            return list.ToArray();
                    }
                }
            }
            return null;
        }

        #endregion Methods    
    }
}
