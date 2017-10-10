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
    */
    public class RedisServerInfoReplicationSection : RedisServerInfoSection
    {
        #region .Ctors

        internal RedisServerInfoReplicationSection(string sectionName)
            : base(sectionName)
        { }

        #endregion .Ctors

        #region Properties

        public long? Role { get { return GetInteger("role"); } } // master

        public long? ConnectedSlaves { get { return GetInteger("connected_slaves"); } } // 1

        public IDictionary<string, string> Slave0 { get { return GetAttributes("slave0"); } } // ip=127.0.0.1,port=6381,state=online,offset=1748378,lag=0

        public IDictionary<string, string> Slave1 { get { return GetAttributes("slave1"); } } // ip=127.0.0.1,port=6381,state=online,offset=1748378,lag=0

        public IDictionary<string, string> Slave2 { get { return GetAttributes("slave2"); } } // ip=127.0.0.1,port=6381,state=online,offset=1748378,lag=0

        public IDictionary<string, string> Slave3 { get { return GetAttributes("slave3"); } } // ip=127.0.0.1,port=6381,state=online,offset=1748378,lag=0

        public IDictionary<string, string> Slave4 { get { return GetAttributes("slave4"); } } // ip=127.0.0.1,port=6381,state=online,offset=1748378,lag=0

        public IDictionary<string, string> Slave5 { get { return GetAttributes("slave5"); } } // ip=127.0.0.1,port=6381,state=online,offset=1748378,lag=0

        public IDictionary<string, string> Slave6 { get { return GetAttributes("slave6"); } } // ip=127.0.0.1,port=6381,state=online,offset=1748378,lag=0

        public IDictionary<string, string> Slave7 { get { return GetAttributes("slave7"); } } // ip=127.0.0.1,port=6381,state=online,offset=1748378,lag=0

        public IDictionary<string, string> Slave8 { get { return GetAttributes("slave8"); } } // ip=127.0.0.1,port=6381,state=online,offset=1748378,lag=0
        
        public IDictionary<string, string> Slave9 { get { return GetAttributes("slave9"); } } // ip=127.0.0.1,port=6381,state=online,offset=1748378,lag=0

        public string MasterReplId { get { return Get("master_replid"); } } // c11020e01bc557109082cb298de257f7c04e4914

        public string MasterReplId2 { get { return Get("master_replid2"); } } // 0000000000000000000000000000000000000000

        public string MasterReplId3 { get { return Get("master_replid3"); } } // 0000000000000000000000000000000000000000

        public string MasterReplId4 { get { return Get("master_replid4"); } } // 0000000000000000000000000000000000000000

        public string MasterReplId5 { get { return Get("master_replid5"); } } // 0000000000000000000000000000000000000000

        public string MasterReplId6 { get { return Get("master_replid6"); } } // 0000000000000000000000000000000000000000

        public string MasterReplId7 { get { return Get("master_replid7"); } } // 0000000000000000000000000000000000000000

        public string MasterReplId8 { get { return Get("master_replid8"); } } // 0000000000000000000000000000000000000000

        public string MasterReplId9 { get { return Get("master_replid9"); } } // 0000000000000000000000000000000000000000
        
        public long? MasterReplOffset { get { return GetInteger("master_repl_offset"); } } // 1748511

        public long? SecondReplOffset { get { return GetInteger("second_repl_offset"); } } // -1

        public bool ReplBacklogActive { get { return GetInteger("repl_backlog_active") > 0; } } // 1

        public long? ReplBacklogSize { get { return GetInteger("repl_backlog_size"); } } // 1048576

        public long? ReplBacklogFirstByteOfset { get { return GetInteger("repl_backlog_first_byte_offset"); } } // 699936

        public long? ReplBacklogHistLen { get { return GetInteger("repl_backlog_histlen"); } } // 1048576

        #endregion Properties
    }
}
