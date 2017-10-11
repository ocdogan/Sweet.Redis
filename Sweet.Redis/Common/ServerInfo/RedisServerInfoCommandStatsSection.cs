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
    # Commandstats
    cmdstat_get:calls=903883,usec=1439965,usec_per_call=1.59
    cmdstat_set:calls=228883,usec=537015,usec_per_call=2.35
    cmdstat_select:calls=118,usec=263,usec_per_call=2.23
    cmdstat_ping:calls=68521,usec=120832,usec_per_call=1.76
    cmdstat_multi:calls=688,usec=1121,usec_per_call=1.63
    cmdstat_exec:calls=399686,usec=1155672,usec_per_call=2.89
    cmdstat_discard:calls=3,usec=6,usec_per_call=2.00
    cmdstat_psync:calls=4,usec=5026,usec_per_call=1256.50
    cmdstat_replconf:calls=59145,usec=85452,usec_per_call=1.44
    cmdstat_info:calls=6158,usec=659946,usec_per_call=107.17
    cmdstat_monitor:calls=12,usec=25,usec_per_call=2.08
    cmdstat_role:calls=2,usec=61,usec_per_call=30.50
    cmdstat_subscribe:calls=1,usec=4,usec_per_call=4.00
    cmdstat_publish:calls=29902,usec=227726,usec_per_call=7.62
    cmdstat_watch:calls=62,usec=433,usec_per_call=6.98
    cmdstat_client:calls=2,usec=213,usec_per_call=106.50
    */
    public class RedisServerInfoCommandStatsSection : RedisServerInfoSection
    {
        #region .Ctors

        internal RedisServerInfoCommandStatsSection(string sectionName)
            : base(sectionName)
        { }

        #endregion .Ctors

        #region Methods

        protected override string ToItemName(string name)
        {
            if (name != null && name.StartsWith("cmdstat_", StringComparison.OrdinalIgnoreCase))
                return (name.Substring("cmdstat_".Length) ?? String.Empty).TrimStart();
            return name;
        }

        #endregion Methods
    }
}
