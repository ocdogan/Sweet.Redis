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
    */
    public class RedisServerInfoStatsSection : RedisServerInfoSection
    {
        #region .Ctors

        internal RedisServerInfoStatsSection(string sectionName)
            : base(sectionName)
        { }

        #endregion .Ctors

        #region Properties

        public long? TotalConnectionsReceived { get { return GetInteger("total_connections_received"); } } // 1

        public long? TotalCommandsProcessed { get { return GetInteger("total_commands_processed"); } } // 5

        public long? InstantaneousOpsPerSec { get { return GetInteger("instantaneous_ops_per_sec"); } } // 0

        public long? TotalNetInputBytes { get { return GetInteger("total_net_input_bytes"); } } // 124

        public long? TotalNetOutputBytes { get { return GetInteger("total_net_output_bytes"); } } // 1985

        public double? InstantaneousInputKbps { get { return GetDouble("instantaneous_input_kbps"); } } // 0.00

        public double? InstantaneousOutputKbps { get { return GetDouble("instantaneous_output_kbps"); } } // 0.00

        public long? RejectedConnections { get { return GetInteger("rejected_connections"); } } // 0

        public long? SyncFull { get { return GetInteger("sync_full"); } } // 0

        public long? SyncPartialOK { get { return GetInteger("sync_partial_ok"); } } // 0

        public long? SyncPartialError { get { return GetInteger("sync_partial_err"); } } // 0

        public long? ExpiredKeys { get { return GetInteger("expired_keys"); } } // 0

        public long? EvictedKeys { get { return GetInteger("evicted_keys"); } } // 0

        public long? KeyspaceHits { get { return GetInteger("keyspace_hits"); } } // 0

        public long? KeyspaceMisses { get { return GetInteger("keyspace_misses"); } } // 0

        public long? PubsubChannels { get { return GetInteger("pubsub_channels"); } } // 0

        public long? PubsubPatterns { get { return GetInteger("pubsub_patterns"); } } // 0

        public long? LatestForkUsec { get { return GetInteger("latest_fork_usec"); } } // 0

        public long? MigrateCachedSockets { get { return GetInteger("migrate_cached_sockets"); } } // 0

        public long? SlaveExpiresTrackedKeys { get { return GetInteger("slave_expires_tracked_keys"); } } // 0

        public long? ActiveDefragHits { get { return GetInteger("active_defrag_hits"); } } // 0

        public long? ActiveDefragMisses { get { return GetInteger("active_defrag_misses"); } } // 0

        public long? ActiveDefragKeyHits { get { return GetInteger("active_defrag_key_hits"); } } // 0

        public long? ActiveDefragKeyMisses { get { return GetInteger("active_defrag_key_misses"); } } // 0

        #endregion Properties
    }
}
