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
    */
    public class RedisServerInfoMemorySection : RedisServerInfoSection
    {
        #region .Ctors

        internal RedisServerInfoMemorySection(string sectionName)
            : base(sectionName)
        { }

        #endregion .Ctors

        #region Properties

        public long? UsedMemory { get { return GetInteger("used_memory"); } } // 2737232

        public string UsedMemoryHuman { get { return Get("used_memory_human"); } } // 2.61M

        public long? UsedMemoryRss { get { return GetInteger("used_memory_rss"); } } // 1884160

        public string UsedMemoryRssHuman { get { return Get("used_memory_rss_human"); } } // 1.80M

        public long? UsedMemoryPeak { get { return GetInteger("used_memory_peak"); } } // 6327552

        public string UsedMemoryPeakHuman { get { return Get("used_memory_peak_human"); } } // 6.03M

        public double? UsedMemoryPeakPercentage { get { return GetDouble("used_memory_peak_perc"); } } // 43.26%

        public long? UsedMemoryOverhead { get { return GetInteger("used_memory_overhead"); } } // 2113076

        public long? UsedMemoryStartup { get { return GetInteger("used_memory_startup"); } } // 963616

        public long? UsedMemoryDataset { get { return GetInteger("used_memory_dataset"); } } // 624156

        public double? UsedMemoryDatasetPercentage { get { return GetDouble("used_memory_dataset_perc"); } } // 35.19%

        public long? TotalSystemMemory { get { return GetInteger("total_system_memory"); } } // 8589934592

        public string TotalSystemMemoryHuman { get { return Get("total_system_memory_human"); } } // 8.00G

        public long? UsedMemoryLua { get { return GetInteger("used_memory_lua"); } } // 39936

        public string UsedMemoryLuaHuman { get { return Get("used_memory_lua_human"); } } // 39.00K

        public long? MaxMemory { get { return GetInteger("maxmemory"); } } // 0

        public string MaxMemoryHuman { get { return Get("maxmemory_human"); } } // 0B

        public string MaxMemoryPolicy { get { return Get("maxmemory_policy"); } } // noeviction

        public double? MemFragmentationRatio { get { return GetDouble("mem_fragmentation_ratio"); } } // 0.69

        public string MemAllocator { get { return Get("mem_allocator"); } } // libc

        public long? ActiveDefragRunning { get { return GetInteger("active_defrag_running"); } } // 0

        public long? LazyFreePendingObjects { get { return GetInteger("lazyfree_pending_objects"); } } // 0

        #endregion Properties
    }
}
