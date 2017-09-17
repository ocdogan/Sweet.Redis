using System;
using System.Text;

namespace Sweet.Redis
{
    public struct RedisPubSubMessage
    {
        #region Static Members

        public static RedisPubSubMessage Empty = new RedisPubSubMessage(-1);

        #endregion Static Members

        #region .Ctors

        public RedisPubSubMessage(int dummy = -1)
            : this()
        {
            Channel = "";
            Pattern = "";
            Data = null;
            Type = RedisPubSubType.Undefined;
            TypeStr = "";
            IsEmpty = true;
        }

        public RedisPubSubMessage(RedisPubSubType type, string typeStr, string channel, string pattern, object data)
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

        public RedisPubSubType Type { get; private set; }

        public string TypeStr { get; private set; }

        #endregion Properties

        public static RedisPubSubMessage ToPubSubMessage(RedisResponse response)
        {
            if (response != null &&
                response.Type == RedisObjectType.Array)
            {
                var items = response.Items;
                if (items != null && items.Count >= 3)
                {
                    var index = 0;
                    var typeItem = items[index++];

                    if (typeItem != null && typeItem.Type == RedisObjectType.BulkString)
                    {
                        var data = typeItem.Data;
                        if (data != null)
                        {
                            var typeStr = Encoding.UTF8.GetString(data).ToLowerInvariant();
                            if (!String.IsNullOrEmpty(typeStr))
                            {
                                var type = RedisPubSubType.Undefined;
                                switch (typeStr)
                                {
                                    case "subscribe":
                                        type = RedisPubSubType.Subscription;
                                        break;
                                    case "psubscribe":
                                        type = RedisPubSubType.PSubscription;
                                        break;
                                    case "pmessage":
                                        type = RedisPubSubType.PMessage;
                                        break;
                                    case "message":
                                        type = RedisPubSubType.SMessage;
                                        break;
                                }

                                if (type != RedisPubSubType.Undefined)
                                {
                                    if (type == RedisPubSubType.PMessage && items.Count < 4)
                                        return RedisPubSubMessage.Empty;

                                    var channelItem = items[index++];
                                    if (channelItem != null && channelItem.Type == RedisObjectType.BulkString)
                                    {
                                        data = channelItem.Data;
                                        if (data != null)
                                        {
                                            var channel = Encoding.UTF8.GetString(data);
                                            if (!String.IsNullOrEmpty(channel))
                                            {
                                                var pattern = String.Empty;
                                                if (type == RedisPubSubType.PMessage)
                                                {
                                                    var patternItem = items[index++];
                                                    if (patternItem != null)
                                                    {
                                                        data = patternItem.Data;
                                                        if (data != null)
                                                            pattern = Encoding.UTF8.GetString(data);
                                                    }

                                                    var tmp = channel;
                                                    channel = pattern;
                                                    pattern = tmp;
                                                }

                                                var dataItem = items[index++];
                                                if (dataItem != null)
                                                {
                                                    data = dataItem.Data;
                                                    switch (dataItem.Type)
                                                    {
                                                        case RedisObjectType.Integer:
                                                            {
                                                                var value = -1L;
                                                                if (data != null)
                                                                    long.TryParse(Encoding.UTF8.GetString(data), out value);

                                                                return new RedisPubSubMessage(type, typeStr, channel, pattern, value);
                                                            }
                                                        case RedisObjectType.BulkString:
                                                            return new RedisPubSubMessage(type, typeStr, channel, pattern, dataItem.Data);
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
            return RedisPubSubMessage.Empty;
        }
    }
}
