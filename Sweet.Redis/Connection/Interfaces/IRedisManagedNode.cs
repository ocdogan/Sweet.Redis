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
    internal interface IRedisManagedNode
    {
        object Seed { get; }
        bool OwnsSeed { get; }

        RedisRole Role { get; }
        RedisEndPoint EndPoint { get; }
        RedisManagedNodeStatus Status { get; set; }

        bool Disposed { get; }
        bool IsClosed { get; set; }
        bool IsHalfClosed { get; set; }
        bool IsOpen { get; set; }
        bool IsSeedAlive { get; }
        bool IsSeedDown { get; }

        RedisManagerSettings Settings { get; }

        RedisNodeInfo GetNodeInfo();
        object ExchangeSeed(object seed);
        bool Ping();

        void SetOnPulseStateChange(Action<object, RedisCardioPulseStatus> onPulseStateChange);
    }
}
