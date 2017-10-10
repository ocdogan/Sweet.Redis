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
    # Sentinel
    sentinel_masters:1
    sentinel_tilt:0
    sentinel_running_scripts:0
    sentinel_scripts_queue_length:0
    sentinel_simulate_failure_flags:0
    master0:name=mymaster,status=ok,address=127.0.0.1:6379,slaves=2,sentinels=1
    */
    public class RedisServerInfoSentinelSection : RedisServerInfoSection
    {
        #region .Ctors

        internal RedisServerInfoSentinelSection(string sectionName)
            : base(sectionName)
        { }

        #endregion .Ctors

        #region Properties

        public long? SentinelMasters { get { return GetInteger("sentinel_masters"); } } // 1

        public long? SentinelTilt { get { return GetInteger("sentinel_tilt"); } } // 0

        public long? SentinelRunningScripts { get { return GetInteger("sentinel_running_scripts"); } } // 0

        public long? SentinelScriptsQueueLength { get { return GetInteger("sentinel_scripts_queue_length"); } } // 0

        public long? SentinelSimulateFailureFlags { get { return GetInteger("sentinel_simulate_failure_flags"); } } // 0

        public IDictionary<string, string> Master0 { get { return GetAttributes("master0"); } } // master0:name=mymaster,status=ok,address=127.0.0.1:6379,slaves=2,sentinels=1

        public IDictionary<string, string> Master1 { get { return GetAttributes("master1"); } } // master0:name=mymaster,status=ok,address=127.0.0.1:6379,slaves=2,sentinels=1

        public IDictionary<string, string> Master2 { get { return GetAttributes("master2"); } } // master0:name=mymaster,status=ok,address=127.0.0.1:6379,slaves=2,sentinels=1

        public IDictionary<string, string> Master3 { get { return GetAttributes("master3"); } } // master0:name=mymaster,status=ok,address=127.0.0.1:6379,slaves=2,sentinels=1

        public IDictionary<string, string> Master4 { get { return GetAttributes("master4"); } } // master0:name=mymaster,status=ok,address=127.0.0.1:6379,slaves=2,sentinels=1

        public IDictionary<string, string> Master5 { get { return GetAttributes("master5"); } } // master0:name=mymaster,status=ok,address=127.0.0.1:6379,slaves=2,sentinels=1

        public IDictionary<string, string> Master6 { get { return GetAttributes("master6"); } } // master0:name=mymaster,status=ok,address=127.0.0.1:6379,slaves=2,sentinels=1

        public IDictionary<string, string> Master7 { get { return GetAttributes("master7"); } } // master0:name=mymaster,status=ok,address=127.0.0.1:6379,slaves=2,sentinels=1

        public IDictionary<string, string> Master8 { get { return GetAttributes("master8"); } } // master0:name=mymaster,status=ok,address=127.0.0.1:6379,slaves=2,sentinels=1

        public IDictionary<string, string> Master9 { get { return GetAttributes("master9"); } } // master0:name=mymaster,status=ok,address=127.0.0.1:6379,slaves=2,sentinels=1
        
        #endregion Properties
    }
}
