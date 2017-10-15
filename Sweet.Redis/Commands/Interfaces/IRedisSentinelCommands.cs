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
    Command	Description
    PING : This command simply returns PONG.
    sentinel masters : Shows a list of all monitored masters + state.
    sentinel master <master name> : Shows the state of a specified master.
    sentinel slaves <master name> : Shows the slaves of the master + state.
    sentinel sentinels <master name> : Shows the Sentinel instances for this master + state
    sentinel get-master-addr-by-name <master name> : Returns the ip and port number of the master with that name. 
         * If a failover is in progress or terminated successfully for this master it returns the address and port of the promoted slave.
    sentinel reset <pattern> : Resets the masters with a matching name. Clears previous state for the master, 
         * removes every slave and sentinel discovered. A fresh discovery is started.
    sentinel failover <master name> : Forces a failover, as if the master was not reachable
    sentinel ckquorum <master name> : Checkes if the current Sentinel configuration is able to reach quorum and majority.
         * Replies:
             * +OK 5 usable Sentinels. Quorum and failover authorization can be reached.
             * -NOQUORUM 1 usable Sentinels. Not enough available Sentinels to reach the specified quorum for this master. Not enough available Sentinels to reach the majority and authorize a failover
    sentinel flushconfig : Forces Sentinel to rewrite it’s configuration on disk
    sentinel monitor <master name> <ip> <port> <quorum> : This command tells the Sentinel to start monitoring a new master 
        * with the specified name, ip, port, and quorum. It is identical to the sentinel monitor configuration directive in sentinel.conf configuration file, 
        * with the difference that you can't use an hostname in as ip, but you need to provide an IPv4 or IPv6 address.
    sentinel remove <master name> : Removes a specified master from monitoring.
    sentinel set <master name> <option> <value> : Similar to config set with Redis, you can change Sentinel options. 
        * All options at the Sentinel configuration file can also be set here
    sentinel is-master-down-by-addr <ip> <port> : Gets feedback from other Sentinels. 
        * This command replies true if the specified address is the one of a master instance, and the master is in SDOWN state.
        * Returns a two elements multi bulk reply where the first is 0 or 1 (0 if the master with that address is known and is in SDOWN state, 1 otherwise). 
            * The second element of the reply is the subjective leader for this master, that is, the runid of the Redis Sentinel instance that should perform 
            * the failover accordingly to the queried instance.
    */
    public interface IRedisSentinelCommands
    {
        RedisBool Ping();
        RedisString CheckQuorum(string masterName);
        RedisBool Failover(string masterName);
        RedisBool FlushConfig();
        RedisResult<RedisEndPoint> GetMasterAddrByName(string masterName);
        RedisResult<RedisServerInfo> Info(RedisParam? section = null);
        RedisResult<RedisIsMasterDownInfo> IsMasterDownByAddr(string ipAddress, int port, string runId);
        RedisResult<RedisSentinelMasterInfo> Master(string masterName);
        RedisResult<RedisSentinelMasterInfo[]> Masters();
        RedisBool Monitor(string masterName, string ipAddress, int port, int quorum);
        RedisBool Remove(string masterName);
        RedisInteger Reset(RedisParam pattern);
        RedisResult<RedisRoleInfo> Role();
        RedisResult<RedisSentinelNodeInfo[]> Sentinels(string masterName);
        RedisBool Set(string masterName, RedisParam parameter, RedisParam value);
        RedisResult<RedisSentinelSlaveInfo[]> Slaves(string masterName);
    }
}
