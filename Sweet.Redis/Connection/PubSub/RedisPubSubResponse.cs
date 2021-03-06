﻿#region License
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
using System.Text;

namespace Sweet.Redis
{
    internal struct RedisPubSubResponse
    {
        #region Static Members

        public static RedisPubSubResponse Empty = new RedisPubSubResponse(-1);

        #endregion Static Members

        #region .Ctors

        public RedisPubSubResponse(int dummy = -1)
            : this()
        {
            Channel = "";
            Pattern = "";
            Data = null;
            Type = RedisPubSubResponseType.Undefined;
            TypeStr = "";
            IsEmpty = true;
        }

        public RedisPubSubResponse(RedisPubSubResponseType type, string typeStr, string channel, string pattern, object data)
            : this()
        {
            Channel = channel;
            Pattern = pattern ?? "";
            Data = data;
            Type = type;
            TypeStr = typeStr;
        }

        #endregion .Ctors

        #region Properties

        public string Channel { get; private set; }

        public object Data { get; private set; }

        public bool IsEmpty { get; private set; }

        public string Pattern { get; private set; }

        public RedisPubSubResponseType Type { get; private set; }

        public string TypeStr { get; private set; }

        #endregion Properties

        #region Methods

        public static RedisPubSubResponse ToPubSubResponse(IRedisRawResponse response)
        {
            if (response != null &&
                response.Type == RedisRawObjectType.Array)
            {
                var items = response.Items;
                if (items != null && items.Count >= 3)
                {
                    var index = 0;
                    var typeItem = items[index++];

                    if (typeItem != null && typeItem.Type == RedisRawObjectType.BulkString)
                    {
                        var data = typeItem.Data;
                        if (data != null)
                        {
                            var typeStr = data.ToUTF8String().ToLowerInvariant();
                            if (!typeStr.IsEmpty())
                            {
                                var type = RedisPubSubResponseType.Undefined;
                                switch (typeStr)
                                {
                                    case "message":
                                        type = RedisPubSubResponseType.Message;
                                        break;
                                    case "pmessage":
                                        type = RedisPubSubResponseType.PMessage;
                                        break;
                                    case "subscribe":
                                        type = RedisPubSubResponseType.Subscribe;
                                        break;
                                    case "psubscribe":
                                        type = RedisPubSubResponseType.PSubscribe;
                                        break;
                                    case "unsubscribe":
                                        type = RedisPubSubResponseType.Unsubscribe;
                                        break;
                                    case "punsubscribe":
                                        type = RedisPubSubResponseType.PUnsubscribe;
                                        break;
                                    default:
                                        break;
                                }

                                if (type != RedisPubSubResponseType.Undefined)
                                {
                                    if (type == RedisPubSubResponseType.PMessage && items.Count < 4)
                                        return RedisPubSubResponse.Empty;

                                    var channelItem = items[index++];
                                    if (channelItem != null && channelItem.Type == RedisRawObjectType.BulkString)
                                    {
                                        data = channelItem.Data;
                                        if (data != null)
                                        {
                                            var channel = data.ToUTF8String();
                                            if (!channel.IsEmpty())
                                            {
                                                var pattern = String.Empty;
                                                switch (type)
                                                {
                                                    case RedisPubSubResponseType.PMessage:
                                                        {
                                                            var patternItem = items[index++];
                                                            if (patternItem != null)
                                                            {
                                                                data = patternItem.Data;
                                                                if (data != null)
                                                                    pattern = data.ToUTF8String();
                                                            }

                                                            var tmp = channel;
                                                            channel = pattern;
                                                            pattern = tmp;
                                                        }
                                                        break;
                                                    case RedisPubSubResponseType.PSubscribe:
                                                    case RedisPubSubResponseType.PUnsubscribe:
                                                        {
                                                            pattern = channel;
                                                            channel = String.Empty;
                                                        }
                                                        break;
                                                    default:
                                                        break;
                                                }

                                                var dataItem = items[index++];
                                                if (dataItem != null)
                                                {
                                                    data = dataItem.Data;
                                                    switch (dataItem.Type)
                                                    {
                                                        case RedisRawObjectType.Integer:
                                                            {
                                                                var value = -1L;
                                                                if (data != null)
                                                                    long.TryParse(data.ToUTF8String(), out value);

                                                                return new RedisPubSubResponse(type, typeStr, channel, pattern, value);
                                                            }
                                                        case RedisRawObjectType.BulkString:
                                                            return new RedisPubSubResponse(type, typeStr, channel, pattern, dataItem.Data);
                                                        default:
                                                            break;
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
            return RedisPubSubResponse.Empty;
        }

        #endregion Methods
    }
}
