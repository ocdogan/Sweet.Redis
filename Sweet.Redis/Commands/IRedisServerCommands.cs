﻿using System;
using System.Collections.Generic;

namespace Sweet.Redis
{
    /*
    BGREWRITEAOF -
    summary: Asynchronously rewrite the append-only file
    since: 1.0.0

    BGSAVE -
    summary: Asynchronously save the dataset to disk
    since: 1.0.0

    CLIENT GETNAME -
    summary: Get the current connection name
    since: 2.6.9

    CLIENT KILL [ip:port] [ID client-id] [TYPE normal|master|slave|pubsub] [ADDR ip:port] [SKIPME yes/no]
    summary: Kill the connection of a client
    since: 2.4.0

    CLIENT LIST -
    summary: Get the list of client connections
    since: 2.4.0

    CLIENT PAUSE timeout
    summary: Stop processing commands from clients for some time
    since: 2.9.50

    CLIENT REPLY ON|OFF|SKIP
    summary: Instruct the server whether to reply to commands
    since: 3.2

    CLIENT SETNAME connection-name
    summary: Set the current connection name
    since: 2.6.9

    COMMAND -
    summary: Get array of Redis command details
    since: 2.8.13

    COMMAND COUNT -
    summary: Get total number of Redis commands
    since: 2.8.13

    COMMAND GETKEYS -
    summary: Extract keys given a full Redis command
    since: 2.8.13

    COMMAND INFO command-name [command-name ...]
    summary: Get array of specific Redis command details
    since: 2.8.13

    CONFIG GET parameter
    summary: Get the value of a configuration parameter
    since: 2.0.0

    CONFIG RESETSTAT -
    summary: Reset the stats returned by INFO
    since: 2.0.0

    CONFIG REWRITE -
    summary: Rewrite the configuration file with the in memory configuration
    since: 2.8.0

    CONFIG SET parameter value
    summary: Set a configuration parameter to the given value
    since: 2.0.0

    DBSIZE -
    summary: Return the number of keys in the selected database
    since: 1.0.0

    DEBUG OBJECT key
    summary: Get debugging information about a key
    since: 1.0.0

    DEBUG SEGFAULT -
    summary: Make the server crash
    since: 1.0.0

    FLUSHALL -
    summary: Remove all keys from all databases
    since: 1.0.0

    FLUSHDB -
    summary: Remove all keys from the current database
    since: 1.0.0

    INFO [section]
    summary: Get information and statistics about the server
    since: 1.0.0

    LASTSAVE -
    summary: Get the UNIX time stamp of the last successful save to disk
    since: 1.0.0

    MONITOR -
    summary: Listen for all requests received by the server in real time
    since: 1.0.0

    ROLE -
    summary: Return the role of the instance in the context of replication
    since: 2.8.12

    SAVE -
    summary: Synchronously save the dataset to disk
    since: 1.0.0

    SHUTDOWN [NOSAVE|SAVE]
    summary: Synchronously save the dataset to disk and then shut down the server
    since: 1.0.0

    SLAVEOF host port
    summary: Make the server a slave of another instance, or promote it as master
    since: 1.0.0

    SLOWLOG subcommand [argument]
    summary: Manages the Redis slow queries log
    since: 2.2.12

    SYNC -
    summary: Internal command used for replication
    since: 1.0.0

    TIME -
    summary: Return the current server time
    since: 2.6.0
     */
    public interface IRedisServerCommands
    {
        bool BGRewriteAOF();
        bool BGSave();

        string ClientGetName();
        long ClientKill(string ip = null, int port = -1, string clientId = null, string type = null, bool skipMe = true);
        RedisClientInfo[] ClientList();
        IDictionary<string, string>[] ClientListDictionary();
        bool ClientPause(int timeout);
        bool ClientReplyOn();
        bool ClientReplyOff();
        bool ClientReplySkip();
        bool ClientSetName(string connectionName);

        IDictionary<string, string> ConfigGet(string parameter);
        bool ConfigResetStat();
        bool ConfigRewrite();
        bool ConfigSet(string parameter, string value);

        long DbSize();

        bool FlushAll();
        bool FlushAllAsync();
        bool FlushDb();
        bool FlushDbAsync();

        string[] Info(string section);
        DateTime LastSave();
        bool Save();
        void ShutDown();
        void ShutDownSave();
        bool SlaveOf(string host, int port);
        bool SlaveOfNoOne();
        DateTime Time();
    }
}
