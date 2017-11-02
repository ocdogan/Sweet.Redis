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
    public class RedisScanBytesData : RedisScanData<RedisByteArray>
    {
        #region .Ctors

        public RedisScanBytesData(ulong cursor, RedisByteArray[] data)
            : base(cursor, data)
        { }

        #endregion .Ctors    

        #region Conversion Methods

        public static implicit operator RedisScanBytesData(byte[][] value)  // implicit RedisScanData conversion operator
        {
            if (value.IsEmpty())
                return new RedisScanBytesData(0, null);

            var length = value.Length;
            var data = new RedisByteArray[length];

            for (var i = 0; i < length; i++)
                data[i] = value[i];

            return new RedisScanBytesData(0, data);
        }

        public static implicit operator byte[][] (RedisScanBytesData value)  // implicit RedisScanData conversion operator
        {
            if (ReferenceEquals(value, null))
                return null;

            var count = value.Count;
            if (count == 0)
                return new byte[][] { };

            var data = value.Data;

            var result = new byte[count][];
            for (var i = 0; i < count; i++)
                result[i] = data[i];

            return result;
        }

        #endregion Conversion Methods
    }
}
