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
    public struct RedisGeospatialItem : IRedisNamedObject
    {
        #region Static Members

        public static readonly RedisGeospatialItem Empty = new RedisGeospatialItem(0, 0, null);

        #endregion Static Members

        #region .Ctors

        public RedisGeospatialItem(double longitude, double latitude, string name)
            : this()
        {
            Longitude = longitude;
            Latitude = latitude;
            Name = name;
        }

        #endregion .Ctors

        #region Properties

        public bool IsEmpty
        {
            get
            {
                return Longitude.Equals(0d) && Latitude.Equals(0d) &&
                                string.IsNullOrEmpty(Name);
            }
        }

        public double Longitude { get; private set; }

        public double Latitude { get; private set; }

        public string Name { get; private set; }

        #endregion Properties

        #region Methods

        public override string ToString()
        {
            if (IsEmpty)
                return "(nil)";
            return String.Format("[Longitude={0}, Latitude={1}, Name={2}]",
                                 Longitude.ToString("G17"), Latitude.ToString("G17"), Name ?? "(nil)");
        }

        #endregion Methods
    }
}
