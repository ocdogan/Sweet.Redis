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
    public class RedisConnectionRetryEventArgs
    {
        #region .Ctors

        public RedisConnectionRetryEventArgs(int retryCountLimit, int spinStepTimeoutMs, int connectionTimeout, int remainingTime)
        {
            StartTime = DateTime.UtcNow;
            RetryCountLimit = retryCountLimit;
            SpinStepTimeoutMs = spinStepTimeoutMs <= 0 ? RedisConstants.MaxConnectionTimeout : spinStepTimeoutMs;
            ConnectionTimeout = connectionTimeout <= 0 ? RedisConstants.MaxConnectionTimeout : connectionTimeout;
            RemainingTime = remainingTime;

            ContinueToSpin = true;
            ThrowError = true;
        }

        #endregion .Ctors

        #region ConnectionRetryInfo

        public DateTime StartTime { get; private set; }

        public int RetryCountLimit { get; private set; }

        public int CurrentRetryCount { get; private set; }

        public int SpinStepTimeoutMs { get; private set; }

        public int ConnectionTimeout { get; private set; }

        public int RemainingTime { get; private set; }

        public bool ContinueToSpin { get; set; }

        public bool ThrowError { get; set; }

        #endregion Properties

        #region Methods

        internal void Entered()
        {
            CurrentRetryCount++;
            RemainingTime = ConnectionTimeout - (int)(DateTime.UtcNow - StartTime).TotalMilliseconds;
        }

        #endregion Methods
    }
}
