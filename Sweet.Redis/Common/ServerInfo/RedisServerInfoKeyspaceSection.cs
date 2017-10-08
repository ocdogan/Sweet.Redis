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
     # Keyspace
    db0:keys=12,expires=0,avg_ttl=0
    db2:keys=1,expires=0,avg_ttl=0
    */
    public class RedisServerInfoKeyspaceSection : RedisServerInfoSection
    {
        #region .Ctors

        internal RedisServerInfoKeyspaceSection(string sectionName)
            : base(sectionName)
        { }

        #endregion .Ctors

        #region Properties

        public IDictionary<string, string> db0 { get { return GetItems("db0", ',', '='); } } // db0:keys=12,expires=0,avg_ttl=0

        public IDictionary<string, string> db1 { get { return GetItems("db1", ',', '='); } } // db0:keys=12,expires=0,avg_ttl=0

        public IDictionary<string, string> db2 { get { return GetItems("db2", ',', '='); } } // db0:keys=12,expires=0,avg_ttl=0

        public IDictionary<string, string> db3 { get { return GetItems("db3", ',', '='); } } // db0:keys=12,expires=0,avg_ttl=0

        public IDictionary<string, string> db4 { get { return GetItems("db4", ',', '='); } } // db0:keys=12,expires=0,avg_ttl=0

        public IDictionary<string, string> db5 { get { return GetItems("db5", ',', '='); } } // db0:keys=12,expires=0,avg_ttl=0

        public IDictionary<string, string> db6 { get { return GetItems("db6", ',', '='); } } // db0:keys=12,expires=0,avg_ttl=0

        public IDictionary<string, string> db7 { get { return GetItems("db7", ',', '='); } } // db0:keys=12,expires=0,avg_ttl=0

        public IDictionary<string, string> db8 { get { return GetItems("db8", ',', '='); } } // db0:keys=12,expires=0,avg_ttl=0

        public IDictionary<string, string> db9 { get { return GetItems("db9", ',', '='); } } // db0:keys=12,expires=0,avg_ttl=0

        public IDictionary<string, string> db10 { get { return GetItems("db10", ',', '='); } } // db0:keys=12,expires=0,avg_ttl=0

        public IDictionary<string, string> db11 { get { return GetItems("db11", ',', '='); } } // db0:keys=12,expires=0,avg_ttl=0

        public IDictionary<string, string> db12 { get { return GetItems("db12", ',', '='); } } // db0:keys=12,expires=0,avg_ttl=0

        public IDictionary<string, string> db13 { get { return GetItems("db13", ',', '='); } } // db0:keys=12,expires=0,avg_ttl=0

        public IDictionary<string, string> db14 { get { return GetItems("db14", ',', '='); } } // db0:keys=12,expires=0,avg_ttl=0

        public IDictionary<string, string> db15 { get { return GetItems("db15", ',', '='); } } // db0:keys=12,expires=0,avg_ttl=0

        #endregion Properties
    }
}
