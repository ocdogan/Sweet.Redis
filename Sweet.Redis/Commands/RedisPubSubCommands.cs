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

namespace Sweet.Redis
{
    internal class RedisPubSubCommands : RedisCommandSet, IRedisPubSubCommands
    {
        #region .Ctors

        public RedisPubSubCommands(IRedisDb db)
            : base(db)
        { }

        #endregion .Ctors

        #region Methods

        public long Publish(string channel, string message)
        {
            if (channel == null)
                throw new ArgumentNullException("channel");

            if (message == null)
                throw new ArgumentNullException("message");

            return ExpectInteger(RedisCommands.Publish, channel.ToBytes(), message.ToBytes());
        }

        public long Publish(string channel, byte[] message)
        {
            if (channel == null)
                throw new ArgumentNullException("channel");

            if (message == null)
                throw new ArgumentNullException("message");

            return ExpectInteger(RedisCommands.Publish, channel.ToBytes(), message);
        }

        public string[] PubSubChannels(string pattern = null)
        {
            RedisRawObj response;
            if (!String.IsNullOrEmpty(pattern))
                response = ExpectArray(RedisCommands.PubSub, RedisCommands.Channels, pattern.ToBytes());
            else
                response = ExpectArray(RedisCommands.PubSub, RedisCommands.Channels);

            if (response != null && response.Type == RedisRawObjType.Array)
            {
                var items = response.Items;
                if (items != null)
                {
                    var itemCount = items.Count;
                    var result = new string[itemCount];

                    if (itemCount > 0)
                    {
                        for (var i = 0; i < itemCount; i++)
                        {
                            var item = items[i];

                            if (item != null &&
                                (item.Type == RedisRawObjType.BulkString ||
                                 item.Type == RedisRawObjType.SimpleString))
                                result[i] = item.Data as string ?? String.Empty;
                        }
                    }

                    return result;
                }
            }

            return new string[0];
        }

        public RedisKeyValue<string, long>[] PubSubNumerOfSubscribers(params string[] channels)
        {
            RedisRawObj response;
            if (channels.Length > 0)
                response = ExpectArray(RedisCommands.PubSub, RedisCommands.NumSub.Join(channels.ToBytesArray()));
            else
                response = ExpectArray(RedisCommands.PubSub, RedisCommands.NumSub);

            if (response != null && response.Type == RedisRawObjType.Array)
            {
                var items = response.Items;
                if (items != null)
                {
                    var itemCount = items.Count;
                    var result = new RedisKeyValue<string, long>[itemCount / 2];

                    if (itemCount > 0)
                    {
                        for (int i = 0, index = 0; i < itemCount; index++)
                        {
                            var nameItem = items[i++];
                            var countItem = items[i++];

                            var name = String.Empty;
                            var count = RedisConstants.Zero;

                            if (nameItem != null &&
                                (nameItem.Type == RedisRawObjType.BulkString ||
                                 nameItem.Type == RedisRawObjType.SimpleString))
                                name = nameItem.Data as string ?? String.Empty;

                            if (countItem != null &&
                                countItem.Type == RedisRawObjType.Integer)
                                count = (long)countItem.Data;

                            result[index] = new RedisKeyValue<string, long>(name, count);
                        }
                    }

                    return result;
                }
            }
            return new RedisKeyValue<string, long>[0];
        }

        public long PubSubNumerOfSubscriptionsToPatterns()
        {
            return ExpectInteger(RedisCommands.PubSub, RedisCommands.NumPat);
        }

        #endregion Methods
    }
}
