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
    public class RedisInfoBase : Dictionary<string, string>
    {
        #region .Ctors

        internal RedisInfoBase()
        { }

        #endregion .Ctors

        #region Methods

        #region Get Methods

        public string Get(string key)
        {
            if (!String.IsNullOrEmpty(key))
            {
                string result;
                TryGetValue(key, out result);

                return result;
            }
            return null;
        }

        public DateTime? GetDate(string key)
        {
            var ticks = GetInteger(key);
            if (ticks.HasValue)
                return ticks.Value.FromUnixTimeStamp();
            return null;
        }

        public double? GetDouble(string key)
        {
            if (!String.IsNullOrEmpty(key))
            {
                string value;
                if (TryGetValue(key, out value) && !String.IsNullOrEmpty(value))
                {
                    if (value.EndsWith("%", StringComparison.OrdinalIgnoreCase))
                        value = value.Substring(0, value.Length - 1);

                    if (!String.IsNullOrEmpty(value))
                    {
                        if (value.StartsWith("%", StringComparison.OrdinalIgnoreCase))
                            value = value.Substring(1, value.Length - 1);

                        if (!String.IsNullOrEmpty(value))
                        {
                            double result;
                            if (double.TryParse(value, NumberStyles.AllowDecimalPoint | NumberStyles.AllowLeadingSign,
                                RedisConstants.InvariantCulture, out result))
                                return result;
                        }
                    }
                }
            }
            return null;
        }

        public long? GetInteger(string key)
        {
            if (!String.IsNullOrEmpty(key))
            {
                string value;
                if (TryGetValue(key, out value) && !String.IsNullOrEmpty(value))
                {
                    long result;
                    if (long.TryParse(value, out result))
                        return result;
                }
            }
            return null;
        }

        public IDictionary<string, string> GetAttributes(string key, char itemSeparator = ',', char valueSeparator = '=')
        {
            if (!String.IsNullOrEmpty(key))
            {
                string value;
                if (TryGetValue(key, out value) && !String.IsNullOrEmpty(value))
                {
                    var result = new Dictionary<string, string>();

                    var items = value.Split(new[] { itemSeparator }, StringSplitOptions.RemoveEmptyEntries);
                    if (items != null)
                    {
                        foreach (var item in items)
                        {
                            if (!String.IsNullOrEmpty(item))
                            {
                                var pos = item.IndexOf(valueSeparator);
                                if (pos == -1)
                                    result[item] = null;
                                else
                                {
                                    var name = (item.Substring(0, pos) ?? String.Empty).TrimEnd();
                                    if (pos == item.Length - 1)
                                        result[name] = null;
                                    else
                                    {
                                        var itemValue = (item.Substring(pos + 1, item.Length - pos - 1) ?? String.Empty).TrimEnd();
                                        result[name] = itemValue;
                                    }
                                }
                            }
                        }
                    }
                    return result;
                }
            }
            return null;
        }

        public string[] GetItems(string key, char separator = ',')
        {
            if (!String.IsNullOrEmpty(key))
            {
                string value;
                if (TryGetValue(key, out value) && !String.IsNullOrEmpty(value))
                    return value.Split(new[] { separator }, StringSplitOptions.RemoveEmptyEntries);
            }
            return null;
        }

        public bool GetOK(string key)
        {
            if (!String.IsNullOrEmpty(key))
            {
                string value;
                if (TryGetValue(key, out value) && (value != null))
                    return value.ToLowerInvariant() == "ok";
            }
            return false;
        }

        #endregion Get Methods

        #endregion Methods
    }
}
