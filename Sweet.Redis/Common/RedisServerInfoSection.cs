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
    public class RedisServerInfoSection : Dictionary<string, string>
    {
        #region .Ctors

        internal RedisServerInfoSection(string sectionName)
        {
            SectionName = sectionName;
        }

        #endregion .Ctors

        #region Properties

        public string SectionName { get; private set; }

        #endregion Properties

        #region Methods

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

        public IDictionary<string, string> GetItems(string key, char itemSeparator, char valueSeparator)
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

        #region Static Methods

        internal static RedisServerInfoSection ParseSection(string sectionName, string[] lines, ref int index)
        {
            RedisServerInfoSection result;

            var section = (sectionName ?? String.Empty).Trim().ToLowerInvariant();
            switch (section)
            {
                case "clients":
                    result = new RedisServerInfoClientsSection(sectionName);
                    break;
                case "cluster":
                    result = new RedisServerInfoClusterSection(sectionName);
                    break;
                case "cpu":
                    result = new RedisServerInfoCpuSection(sectionName);
                    break;
                case "keyspace":
                    result = new RedisServerInfoKeyspaceSection(sectionName);
                    break;
                case "memory":
                    result = new RedisServerInfoMemorySection(sectionName);
                    break;
                case "persistence":
                    result = new RedisServerInfoPersistenceSection(sectionName);
                    break;
                case "sentinel":
                    result = new RedisServerInfoSentinelSection(sectionName);
                    break;
                case "server":
                    result = new RedisServerInfoServerSection(sectionName);
                    break;
                case "stats":
                    result = new RedisServerInfoStatsSection(sectionName);
                    break;
                case "replication":
                    result = new RedisServerInfoReplicationSection(sectionName);
                    break;
                default:
                    result = new RedisServerInfoSection(sectionName);
                    break;
            }

            var length = lines.Length;
            for (; index < length; index++)
            {
                var line = (lines[index] ?? String.Empty).TrimStart();
                if (!String.IsNullOrEmpty(line))
                {
                    if (line[0] == '#')
                    {
                        index--;
                        return result;
                    }

                    int pos = line.IndexOf(':');
                    if (pos == -1)
                        result[line.TrimEnd()] = null;
                    else
                    {
                        var name = (line.Substring(0, pos) ?? String.Empty).TrimEnd();
                        if (pos == line.Length - 1)
                            result[name] = null;
                        else
                        {
                            var value = (line.Substring(pos + 1, line.Length - pos - 1) ?? String.Empty).TrimEnd();
                            result[name] = value;
                        }
                    }
                }
            }
            return result;
        }

        #endregion Static Methods
        #endregion Methods
    }
}
