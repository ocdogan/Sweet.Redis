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
using System.Threading;

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
        #region Field Members

        private RedisServerSlaveInfo[] m_Slaves;
        private List<RedisServerSlaveInfo> m_SlavesList = new List<RedisServerSlaveInfo>();

        #endregion Field Members

        #region .Ctors

        internal RedisServerInfoReplicationSection(string sectionName)
            : base(sectionName)
        { }

        #endregion .Ctors

        #region Properties

        public string Role { get { return Get("role"); } } // master

        public long? ConnectedSlaves { get { return GetInteger("connected_slaves"); } } // 1

        public RedisServerSlaveInfo[] Slaves // ip=127.0.0.1,port=6381,state=online,offset=1748378,lag=0
        {
            get 
            {
                if (m_Slaves == null)
                {
                    var list = Interlocked.Exchange(ref m_SlavesList, null);
                    m_Slaves = list != null ? list.ToArray() : new RedisServerSlaveInfo[0];
                }
                return m_Slaves;
            }
        } 

        public string MasterReplId { get { return Get("master_replid"); } } // c11020e01bc557109082cb298de257f7c04e4914

        public long? MasterReplOffset { get { return GetInteger("master_repl_offset"); } } // 1748511

        public long? SecondReplOffset { get { return GetInteger("second_repl_offset"); } } // -1

        public bool ReplBacklogActive { get { return GetInteger("repl_backlog_active") > 0; } } // 1

        public long? ReplBacklogSize { get { return GetInteger("repl_backlog_size"); } } // 1048576

        public long? ReplBacklogFirstByteOfset { get { return GetInteger("repl_backlog_first_byte_offset"); } } // 699936

        public long? ReplBacklogHistLen { get { return GetInteger("repl_backlog_histlen"); } } // 1048576

        #endregion Properties

        #region Methods

        protected override string OnSetValue(string name, string value)
        {
            if (!String.IsNullOrEmpty(name))
            {
                var slaveLength = "slave".Length;
                if ((name.Length > slaveLength) && name.StartsWith("slave", StringComparison.OrdinalIgnoreCase))
                {
                    var indexStr = name.Substring(slaveLength);
                    if (!String.IsNullOrEmpty(indexStr))
                    {
                        int index;
                        if (int.TryParse(indexStr, out index))
                            m_SlavesList.Add(new RedisServerSlaveInfo(index, value));
                    }
                }
            }
            return base.OnSetValue(name, value);
        }

        #endregion Methods
    }
}
