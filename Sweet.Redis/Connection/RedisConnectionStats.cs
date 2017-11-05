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

using System.Threading;

namespace Sweet.Redis
{
    public static class RedisConnectionStats
    {
        #region Static Members

        private static long s_InUseConnections;

        #endregion Static Members

        #region Properties

        public static long InUseConnections
        {
            get { return Interlocked.Read(ref s_InUseConnections); }
        }

        #endregion Properties

        #region Methods

        public static void IncrInUseConnections()
        {
            var count = Interlocked.Add(ref s_InUseConnections, RedisConstants.One);
            if (count > int.MaxValue)
                Interlocked.Exchange(ref s_InUseConnections, int.MaxValue);
        }

        public static void DecrInUseConnections()
        {
            var count = Interlocked.Add(ref s_InUseConnections, RedisConstants.MinusOne);
            if (count < int.MinValue)
                Interlocked.Exchange(ref s_InUseConnections, int.MinValue);
        }

        #endregion Methods
    }
}
