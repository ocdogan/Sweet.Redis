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

namespace Sweet.Redis
{
    /*
    # Server
    redis_version:4.0.1
    redis_git_sha1:00000000
    redis_git_dirty:0
    redis_build_id:6288fb96ae78c08d
    redis_mode:sentinel
    os:Darwin 15.6.0 x86_64
    arch_bits:64
    multiplexing_api:kqueue
    atomicvar_api:atomic-builtin
    gcc_version:4.2.1
    process_id:64301
    run_id:c5eb8d22af31ac8a3b90251c42c57eff2433f599
    tcp_port:26379
    uptime_in_seconds:97252
    uptime_in_days:1
    hz:19
    lru_clock:13694659
    executable:/usr/local/redis-4.0.1/bin/redis-sentinel
    config_file:/usr/local/redis-4.0.1/sentinel.conf

    # Clients
    connected_clients:1
    client_longest_output_list:0
    client_biggest_input_buf:0
    blocked_clients:0

    # CPU
    used_cpu_sys:20.68
    used_cpu_user:17.70
    used_cpu_sys_children:0.00
    used_cpu_user_children:0.00

    # Memory
    used_memory:2737232
    used_memory_human:2.61M
    used_memory_rss:1884160
    used_memory_rss_human:1.80M
    used_memory_peak:6327552
    used_memory_peak_human:6.03M
    used_memory_peak_perc:43.26%
    used_memory_overhead:2113076
    used_memory_startup:963616
    used_memory_dataset:624156
    used_memory_dataset_perc:35.19%
    total_system_memory:8589934592
    total_system_memory_human:8.00G
    used_memory_lua:39936
    used_memory_lua_human:39.00K
    maxmemory:0
    maxmemory_human:0B
    maxmemory_policy:noeviction
    mem_fragmentation_ratio:0.69
    mem_allocator:libc
    active_defrag_running:0
    lazyfree_pending_objects:0

    # Persistence
    loading:0
    rdb_changes_since_last_save:0
    rdb_bgsave_in_progress:0
    rdb_last_save_time:1506768844
    rdb_last_bgsave_status:ok
    rdb_last_bgsave_time_sec:0
    rdb_current_bgsave_time_sec:-1
    rdb_last_cow_size:0
    aof_enabled:0
    aof_rewrite_in_progress:0
    aof_rewrite_scheduled:0
    aof_last_rewrite_time_sec:-1
    aof_current_rewrite_time_sec:-1
    aof_last_bgrewrite_status:ok
    aof_last_write_status:ok
    aof_last_cow_size:0

    # Stats
    total_connections_received:1
    total_commands_processed:5
    instantaneous_ops_per_sec:0
    total_net_input_bytes:124
    total_net_output_bytes:1985
    instantaneous_input_kbps:0.00
    instantaneous_output_kbps:0.00
    rejected_connections:0
    sync_full:0
    sync_partial_ok:0
    sync_partial_err:0
    expired_keys:0
    evicted_keys:0
    keyspace_hits:0
    keyspace_misses:0
    pubsub_channels:0
    pubsub_patterns:0
    latest_fork_usec:0
    migrate_cached_sockets:0
    slave_expires_tracked_keys:0
    active_defrag_hits:0
    active_defrag_misses:0
    active_defrag_key_hits:0
    active_defrag_key_misses:0

    # Replication
    role:master
    connected_slaves:1
    slave0:ip=127.0.0.1,port=6381,state=online,offset=1748378,lag=0
    master_replid:c11020e01bc557109082cb298de257f7c04e4914
    master_replid2:0000000000000000000000000000000000000000
    master_repl_offset:1748511
    second_repl_offset:-1
    repl_backlog_active:1
    repl_backlog_size:1048576
    repl_backlog_first_byte_offset:699936
    repl_backlog_histlen:1048576

    # Cluster
    cluster_enabled:0

    # Keyspace
    db0:keys=12,expires=0,avg_ttl=0
    db2:keys=1,expires=0,avg_ttl=0

    # Sentinel
    sentinel_masters:1
    sentinel_tilt:0
    sentinel_running_scripts:0
    sentinel_scripts_queue_length:0
    sentinel_simulate_failure_flags:0
    master0:name=mymaster,status=ok,address=127.0.0.1:6379,slaves=2,sentinels=1
    */
    public class RedisServerInfo : Dictionary<string, RedisServerInfoSection>
    {
        #region .Ctors

        internal RedisServerInfo()
        { }

        #endregion .Ctors

        #region Properties

        public RedisServerInfoClientsSection Clients 
        { 
            get 
            { 
                RedisServerInfoSection section;
                if (TryGetValue("clients", out section))
                    return section as RedisServerInfoClientsSection;
                return null;
            }
        }

        public RedisServerInfoClusterSection Cluster 
        { 
            get 
            { 
                RedisServerInfoSection section;
                if (TryGetValue("cluster", out section))
                    return section as RedisServerInfoClusterSection;
                return null;
            }
        }

        public RedisServerInfoCpuSection Cpu 
        { 
            get 
            { 
                RedisServerInfoSection section;
                if (TryGetValue("cpu", out section))
                    return section as RedisServerInfoCpuSection;
                return null;
            }
        }

        public RedisServerInfoKeyspaceSection Keyspace 
        { 
            get 
            { 
                RedisServerInfoSection section;
                if (TryGetValue("keyspace", out section))
                    return section as RedisServerInfoKeyspaceSection;
                return null;
            }
        }

        public RedisServerInfoMemorySection Memory 
        { 
            get 
            { 
                RedisServerInfoSection section;
                if (TryGetValue("memory", out section))
                    return section as RedisServerInfoMemorySection;
                return null;
            }
        }

        public RedisServerInfoPersistenceSection Persistence 
        { 
            get 
            { 
                RedisServerInfoSection section;
                if (TryGetValue("persistence", out section))
                    return section as RedisServerInfoPersistenceSection;
                return null;
            }
        }

        public RedisServerInfoSentinelSection Sentinel 
        { 
            get 
            { 
                RedisServerInfoSection section;
                if (TryGetValue("sentinel", out section))
                    return section as RedisServerInfoSentinelSection;
                return null;
            }
        }

        public RedisServerInfoServerSection Server 
        { 
            get 
            { 
                RedisServerInfoSection section;
                if (TryGetValue("server", out section))
                    return section as RedisServerInfoServerSection;
                return null;
            }
        }

        public RedisServerInfoStatsSection Stats 
        { 
            get 
            { 
                RedisServerInfoSection section;
                if (TryGetValue("stats", out section))
                    return section as RedisServerInfoStatsSection;
                return null;
            }
        }

        public RedisServerInfoReplicationSection Replication 
        { 
            get 
            { 
                RedisServerInfoSection section;
                if (TryGetValue("replication", out section))
                    return section as RedisServerInfoReplicationSection;
                return null;
            }
        }

        #endregion Properties

        #region Methods

        public RedisServerInfoSection Get(string sectionName)
        {
            if (!String.IsNullOrEmpty(sectionName))
            {
                RedisServerInfoSection result;
                TryGetValue(sectionName, out result);

                return result;
            }
            return null;
        }
        
        #region Static Methods

        public static RedisServerInfo Parse(string info)
        {
            if (!String.IsNullOrEmpty(info))
            {
                var lines = info.Split(new[] { RedisConstants.CRLF }, StringSplitOptions.RemoveEmptyEntries);
                if (lines != null)
                {
                    var length = lines.Length;
                    if (length > 0)
                    {
                        var result = new RedisServerInfo();

                        for (var i = 0; i < length; i++)
                        {
                            var line = (lines[i] ?? String.Empty).TrimStart();
                            if (line[0] == '#')
                            {
                                i++;

                                var section = RedisServerInfoSection.ParseSection(line.TrimStart('#').Trim(), lines, ref i);
                                if (section != null)
                                    result[section.SectionName] = section;
                            }
                        }

                        return result;
                    }
                }
            }
            return null;
        }
        
        #endregion Static Methods

        #endregion Methods
    }
}
