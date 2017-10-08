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
using System.Globalization;

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
    */
    public class RedisServerInfoServerSection : RedisServerInfoSection
    {
        #region .Ctors

        internal RedisServerInfoServerSection(string sectionName)
            : base(sectionName)
        { }

        #endregion .Ctors

        #region Properties

        public string RedisVersion { get { return Get("redis_version"); } } // 4.0.1
        
        public string RedisGitSha1 { get { return Get("redis_git_sha1"); } } // 00000000
        
        public long? RedisGitDirty { get { return GetInteger("redis_git_dirty"); } } // 0
        
        public string RedisBuildId { get { return Get("redis_build_id"); } } // 6288fb96ae78c08d
        
        public string RedisMode { get { return Get("redis_mode"); } } // sentinel
        
        public string OS { get { return Get("os"); } } // Darwin 15.6.0 x86_64
        
        public long? ArchBits { get { return GetInteger("arch_bits"); } } // 64
        
        public string MultiplexingApi { get { return Get("multiplexing_api"); } } // kqueue
        
        public string AtomicvarApi { get { return Get("atomicvar_api"); } } // atomic-builtin
        
        public string GccVersion { get { return Get("gcc_version"); } } // 4.2.1
        
        public long? ProcessId { get { return GetInteger("process_id"); } } // 64301
        
        public string RunId { get { return Get("run_id"); } } // c5eb8d22af31ac8a3b90251c42c57eff2433f599
        
        public long? TcpPort { get { return GetInteger("tcp_port"); } } // 26379
        
        public long? UptimeInSeconds { get { return GetInteger("uptime_in_seconds"); } } // 97252
        
        public long? UptimeInDays { get { return GetInteger("uptime_in_days"); } } // 1
        
        public long? Hz { get { return GetInteger("hz"); } } // 19
        
        public long? LRUClock { get { return GetInteger("lru_clock"); } } // 13694659
        
        public string Executable { get { return Get("executable"); } } // /usr/local/redis-4.0.1/bin/redis-sentinel
        
        public string ConfigFile { get { return Get("config_file"); } } // /usr/local/redis-4.0.1/sentinel.conf

        #endregion Properties
    }
}
