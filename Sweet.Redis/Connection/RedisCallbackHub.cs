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
    internal class RedisCallbackHub<T>
    {
        #region Field Members

        private readonly object m_SyncObj = new object();
        private Dictionary<string, RedisActionBag<T>> m_Subscriptions = new Dictionary<string, RedisActionBag<T>>();

        #endregion Field Members

        #region Methods

        public RedisActionBag<T> CallbacksOf(string keyword)
        {
            if (!String.IsNullOrEmpty(keyword))
            {
                lock (m_SyncObj)
                {
                    RedisActionBag<T> callbacks;
                    if (m_Subscriptions.TryGetValue(keyword, out callbacks) &&
                        callbacks != null && callbacks.Count > 0)
                        return new RedisActionBag<T>(callbacks);
                }
            }
            return null;
        }

        public IDictionary<string, RedisActionBag<T>> Subscriptions()
        {
            lock (m_SyncObj)
            {
                if (m_Subscriptions.Count > 0)
                {
                    var result = new Dictionary<string, RedisActionBag<T>>();
                    foreach (var kvp in m_Subscriptions)
                        result[kvp.Key] = new RedisActionBag<T>(kvp.Value);

                    return result;
                }
            }
            return null;
        }

        public bool Exists(string keyword, Action<T> callback)
        {
            if (!String.IsNullOrEmpty(keyword) && callback != null)
            {
                RedisActionBag<T> callbacks;
                lock (m_SyncObj)
                {
                    if (m_Subscriptions.TryGetValue(keyword, out callbacks) &&
                        callbacks != null && callbacks.Count > 0)
                    {
                        var minfo = callback.Method;
                        return callbacks.FindIndex(c => c.Method == minfo) > -1;
                    }
                }
            }
            return false;
        }

        public bool Exists(string keyword)
        {
            if (!String.IsNullOrEmpty(keyword))
            {
                lock (m_SyncObj)
                {
                    return m_Subscriptions.ContainsKey(keyword);
                }
            }
            return false;
        }

        public bool HasCallbacks(string keyword)
        {
            if (!String.IsNullOrEmpty(keyword))
            {
                RedisActionBag<T> callbacks;
                lock (m_SyncObj)
                {
                    if (m_Subscriptions.TryGetValue(keyword, out callbacks))
                        return (callbacks != null && callbacks.Count > 0);
                }
            }
            return false;
        }

        public bool IsEmpty()
        {
            lock (m_SyncObj)
            {
                foreach (var kvp in m_Subscriptions)
                {
                    var callbacks = kvp.Value;
                    if (callbacks != null && callbacks.Count > 0)
                        return false;
                }
            }
            return true;
        }

        public bool Register(string keyword, Action<T> callback)
        {
            if (String.IsNullOrEmpty(keyword))
                return false;

            var result = false;

            RedisActionBag<T> bag;
            if (!m_Subscriptions.TryGetValue(keyword, out bag))
            {
                lock (m_SyncObj)
                {
                    if (!m_Subscriptions.TryGetValue(keyword, out bag))
                    {
                        bag = new RedisActionBag<T>();
                        bag.Add(callback);
                        m_Subscriptions[keyword] = bag;
                        result = true;
                    }
                }
            }

            if (!result)
            {
                lock (m_SyncObj)
                {
                    var minfo = callback.Method;

                    var index = bag.FindIndex(c => c.Method == minfo);
                    if (index == -1)
                    {
                        bag.Add(callback);
                        result = true;
                    }
                }
            }

            return result;
        }

        public bool Register(string keyword, RedisActionBag<T> callbacks)
        {
            if (String.IsNullOrEmpty(keyword) || callbacks == null ||
                callbacks.Count == 0)
                return false;

            var result = false;

            RedisActionBag<T> bag;
            var processed = false;
            if (!m_Subscriptions.TryGetValue(keyword, out bag))
            {
                lock (m_SyncObj)
                {
                    if (!m_Subscriptions.TryGetValue(keyword, out bag))
                    {
                        processed = true;
                        bag = new RedisActionBag<T>();

                        foreach (var callback in callbacks)
                        {
                            if (callback != null)
                            {
                                var minfo = callback.Method;

                                var index = bag.FindIndex(c => c.Method == minfo);
                                if (index == -1)
                                {
                                    bag.Add(callback);
                                    result = true;
                                }
                            }
                        }

                        if (result)
                            m_Subscriptions[keyword] = bag;
                    }
                }
            }

            if (!processed)
            {
                lock (m_SyncObj)
                {
                    foreach (var callback in callbacks)
                    {
                        if (callback != null)
                        {
                            var minfo = callback.Method;

                            var index = bag.FindIndex(c => c.Method == minfo);
                            if (index == -1)
                            {
                                bag.Add(callback);
                                result = true;
                            }
                        }
                    }
                }
            }

            return result;
        }

        public RedisActionBag<T> Drop(string keyword)
        {
            if (!String.IsNullOrEmpty(keyword))
            {
                lock (m_SyncObj)
                {
                    RedisActionBag<T> callbacks;
                    if (m_Subscriptions.TryGetValue(keyword, out callbacks))
                        m_Subscriptions.Remove(keyword);
                    return callbacks;
                }
            }
            return null;
        }

        public bool Unregister(string keyword)
        {
            if (!String.IsNullOrEmpty(keyword))
            {
                lock (m_SyncObj)
                {
                    RedisActionBag<T> callbacks;
                    if (m_Subscriptions.TryGetValue(keyword, out callbacks))
                    {
                        m_Subscriptions.Remove(keyword);
                        if (callbacks != null && callbacks.Count > 0)
                            callbacks.Clear();
                        return true;
                    }
                }
            }
            return false;
        }

        public bool Unregister(string keyword, Action<T> callback)
        {
            if (callback != null && !String.IsNullOrEmpty(keyword))
            {
                lock (m_SyncObj)
                {
                    RedisActionBag<T> callbacks;
                    if (m_Subscriptions.TryGetValue(keyword, out callbacks) &&
                        callbacks != null && callbacks.Count > 0)
                    {
                        var minfo = callback.Method;

                        var index = callbacks.FindIndex(c => c.Method == minfo);
                        if (index > -1)
                        {
                            callbacks.RemoveAt(index);
                            return true;
                        }
                    }
                }
            }
            return false;
        }

        public bool Unregister(Action<T> callback)
        {
            if (callback == null)
                return false;

            var result = false;
            var minfo = callback.Method;

            lock (m_SyncObj)
            {
                foreach (var kvp in m_Subscriptions)
                {
                    var callbacks = kvp.Value;
                    var count = callbacks.Count;

                    for (var i = count - 1; i > -1; i--)
                    {
                        if (callbacks[i].Method == minfo)
                        {
                            callbacks.RemoveAt(i);
                            result = true;
                        }
                    }
                }
            }
            return result;
        }

        public void UnregisterAll()
        {
            lock (m_SyncObj)
            {
                foreach (var kvp in m_Subscriptions)
                    if (kvp.Value != null)
                        kvp.Value.Clear();

                m_Subscriptions.Clear();
            }
        }

        public virtual void Invoke(T msg)
        { }

        #endregion Methods
    }
}
