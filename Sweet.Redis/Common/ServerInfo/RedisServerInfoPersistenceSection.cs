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
    /*
    # Persistence
    loading:0
    rdb_changes_sinceLast_save:0
    rdbBgsave_in_progress:0
    rdbLast_save_time:1506768844
    rdbLastBgsave_status:ok
    rdbLastBgsave_time_sec:0
    rdb_currentBgsave_time_sec:-1
    rdbLast_cow_size:0
    aof_enabled:0
    aof_rewrite_in_progress:0
    aof_rewrite_scheduled:0
    aofLast_rewrite_time_sec:-1
    aof_current_rewrite_time_sec:-1
    aofLast_bgrewrite_status:ok
    aofLast_write_status:ok
    aofLast_cow_size:0
    */
    public class RedisServerInfoPersistenceSection : RedisServerInfoSection
    {
        #region .Ctors

        internal RedisServerInfoPersistenceSection(string sectionName)
            : base(sectionName)
        { }

        #endregion .Ctors

        #region Properties

        public long? Loading { get { return GetInteger("loading"); } } // 0

        public long? RdbChangesSinceLastSave { get { return GetInteger("rdb_changes_sinceL_last_save"); } } // 0

        public long? RdbBgsaveInProgress { get { return GetInteger("rdb_bgsave_in_progress"); } } // 0

        public DateTime? RdbLastSaveTime { get { return GetDate("rdb_last_save_time"); } } // 1506768844

        public bool RdbLastBgsaveStatus { get { return GetOK("rdbLast_bgsave_status"); } } // ok

        public long? RdbLastBgsaveTimeSec { get { return GetInteger("rdbLast_bgsave_time_sec"); } } // 0

        public long? Rdb_currentBgsaveTimeSec { get { return GetInteger("rdb_current_bgsave_time_sec"); } } // -1

        public long? RdbLastCowSize { get { return GetInteger("rdb_last_cow_size"); } } // 0

        public bool AofEnabled { get { return GetInteger("aof_enabled") > 0; } } // 0

        public long? AofRewriteInProgress { get { return GetInteger("aof_rewrite_in_progress"); } } // 0

        public long? AofRewriteScheduled { get { return GetInteger("aof_rewrite_scheduled"); } } // 0

        public long? AofLastRewriteTimeSec { get { return GetInteger("aof_last_rewrite_time_sec"); } } // -1

        public long? AofCurrentRewriteTimeSec { get { return GetInteger("aof_current_rewrite_time_sec"); } } // -1

        public bool AofLastBgrewriteStatus { get { return GetOK("aof_last_bgrewrite_status"); } } // ok

        public bool AofLastWriteStatus { get { return GetOK("aof_last_write_status"); } } // ok

        public long? AofLastCowSize { get { return GetInteger("aof_last_cow_size"); } } // 0

        #endregion Properties
    }
}