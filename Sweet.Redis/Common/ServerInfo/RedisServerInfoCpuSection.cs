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
    # CPU
    used_cpu_sys:20.68
    used_cpu_user:17.70
    used_cpu_sys_children:0.00
    used_cpu_user_children:0.00
    */
    public class RedisServerInfoCpuSection : RedisServerInfoSection
    {
        #region .Ctors

        internal RedisServerInfoCpuSection(string sectionName)
            : base(sectionName)
        { }

        #endregion .Ctors

        #region Properties

        public double? UsedCpuSys { get { return GetDouble("used_cpu_sys"); } } // 20.68

        public double? UsedCpuUser { get { return GetDouble("used_cpu_user"); } } // 17.70

        public double? UsedCpuSysChildren { get { return GetDouble("used_cpu_sys_children"); } } // 0.00

        public double? UsedCpuUserChildren { get { return GetDouble("used_cpu_user_children"); } } // 0.00

        #endregion Properties
    }
}
