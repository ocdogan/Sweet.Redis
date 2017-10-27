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
    public struct RedisPubSubMessage
    {
        #region Static Members

        public static readonly RedisPubSubMessage Empty = new RedisPubSubMessage(-1);

        #endregion Static Members

        #region .Ctors

        public RedisPubSubMessage(int dummy = -1)
            : this()
        {
            Channel = "";
            Pattern = "";
            Data = null;
            Type = RedisPubSubMessageType.Undefined;
            IsEmpty = true;
        }

        public RedisPubSubMessage(RedisPubSubMessageType type, string channel, string pattern, byte[] data)
            : this()
        {
            Channel = channel;
            Pattern = pattern ?? "";
            Data = data;
            Type = type;
        }

        #endregion .Ctors

        #region Properties

        public string Channel { get; private set; }

        public byte[] Data { get; private set; }

        public bool IsEmpty { get; private set; }

        public string Pattern { get; private set; }

        public RedisPubSubMessageType Type { get; private set; }

        #endregion Properties
    }
}
