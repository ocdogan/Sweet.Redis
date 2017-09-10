﻿namespace Sweet.Redis
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
    }
}
