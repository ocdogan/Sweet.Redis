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
	PSUBSCRIBE pattern [pattern ...]
	summary: Listen for messages published to channels matching the given patterns
	since: 2.0.0

	PUBLISH channel message
	summary: Post a message to a channel
	since: 2.0.0

	PUBSUB subcommand [argument [argument ...]]
	summary: Inspect the state of the Pub/Sub subsystem
	since: 2.8.0

	PUNSUBSCRIBE [pattern [pattern ...]]
	summary: Stop listening for messages posted to channels matching the given patterns
	since: 2.0.0

	SUBSCRIBE channel [channel ...]
	summary: Listen for messages published to the given channels
	since: 2.0.0

	UNSUBSCRIBE [channel [channel ...]]
	summary: Stop listening for messages posted to the given channels
	since: 2.0.0
	*/
    public interface IRedisPubSubCommands
    {
        RedisInteger Publish(RedisParam channel, RedisParam message);
        RedisMultiString PubSubChannels(RedisParam? pattern = null);
        RedisResult<RedisKeyValue<string, long>[]> PubSubNumerOfSubscribers(params RedisParam[] channels);
        RedisInteger PubSubNumerOfSubscriptionsToPatterns();
    }
}
