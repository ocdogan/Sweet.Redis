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

namespace Sweet.Redis
{
    public class RedisServerInfoSection : RedisInfoBase
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

        protected virtual string ToItemName(string name)
        {
            return name;
        }

        protected internal virtual void Parse(IList<string> lines)
        {
            if (lines != null)
            {
                var length = lines.Count;
                for (var index = 0; index < length; index++)
                {
                    var line = (lines[index] ?? String.Empty);
                    if (!String.IsNullOrEmpty(line))
                    {
                        var pos = line.IndexOf(':');
                        if (pos == -1)
                        {
                            var name = (ToItemName(line) ?? String.Empty).TrimEnd();
                            if (!String.IsNullOrEmpty(name))
                                this[name] = OnSetValue(name, null);
                        }
                        else
                        {
                            var name = (ToItemName(line.Substring(0, pos)) ?? String.Empty).TrimEnd();
                            if (!String.IsNullOrEmpty(name))
                            {
                                if (pos == line.Length - 1)
                                    this[name] = OnSetValue(name, null);
                                else
                                {
                                    var value = (line.Substring(pos + 1, line.Length - pos - 1) ?? String.Empty).TrimEnd();
                                    this[name] = OnSetValue(name, value);
                                }
                            }
                        }
                    }
                }
            }
        }

        protected virtual string OnSetValue(string name, string value)
        {
            return value;
        }

        #endregion Methods
    }
}
