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
using System.Collections.ObjectModel;
using System.Threading;

namespace Sweet.Redis
{
    public class RedisRawResponse : RedisDisposable, IRedisRawResponse
    {
        #region Field Members

        private long m_HasData;
        private byte[] m_Data;
        private long m_Ready;
        private int m_TypeByte = -1;
        private RedisRawObjType? m_Type;
        private long m_Length = int.MinValue;
        private IRedisRawResponse m_Parent;
        private IList<IRedisRawResponse> m_List;
        private IList<IRedisRawResponse> m_ReadOnlyList;

        #endregion Field Members

        #region .Ctors

        public RedisRawResponse(IRedisRawResponse parent = null, RedisRawObjType type = RedisRawObjType.Undefined)
        {
            m_Parent = parent;
            if (type != RedisRawObjType.Undefined)
                SetType(type);
        }

        #endregion .Ctors

        #region Destructors

        protected override void OnDispose(bool disposing)
        {
            Interlocked.Exchange(ref m_Parent, null);
            ClearInternal();
        }

        #endregion Destructors

        #region Properties

        public virtual int ChildCount
        {
            get
            {
                if (!m_Type.HasValue || m_Type != RedisRawObjType.Array)
                    return -1;

                var list = m_List;
                return (list != null) ? list.Count : 0;
            }
        }

        public virtual byte[] Data
        {
            get { return m_Data; }
        }

        public virtual bool HasChild
        {
            get
            {
                if (m_Type != RedisRawObjType.Array)
                    return false;

                var list = m_List;
                return (list != null) && list.Count > 0;
            }
        }

        public virtual bool HasData
        {
            get
            {
                if (Interlocked.Read(ref m_HasData) == RedisConstants.True)
                    return true;

                if (!m_Type.HasValue || m_Type == RedisRawObjType.Array)
                    return false;

                var data = m_Data;
                var result = (data != null) && data.Length > 0;
                if (result)
                    Interlocked.Exchange(ref m_HasData, RedisConstants.True);

                return result;
            }
        }

        public virtual bool IsVoid
        {
            get { return false; }
        }

        public virtual IList<IRedisRawResponse> Items
        {
            get { return m_ReadOnlyList; }
        }

        public virtual int Length
        {
            get { return (int)Interlocked.Read(ref m_Length); }
        }

        public virtual IRedisRawResponse Parent
        {
            get { return m_Parent; }
        }

        public virtual bool Ready
        {
            get { return Interlocked.Read(ref m_Ready) != RedisConstants.False; }
        }

        public virtual RedisRawObjType Type
        {
            get { return m_Type.HasValue ? m_Type.Value : RedisRawObjType.Undefined; }
        }

        public virtual int TypeByte
        {
            get { return m_TypeByte; }
        }

        #endregion Properties

        #region Methods

        internal void SetData(byte[] value)
        {
            m_Data = value;
            Interlocked.Exchange(ref m_HasData, RedisConstants.True);

            if (m_Type.HasValue && m_Type != RedisRawObjType.Array)
                SetReady(true);
        }

        internal void SetLength(int value)
        {
            value = Math.Max(-1, value);
            Interlocked.Exchange(ref m_Length, value);

            if (m_Type == RedisRawObjType.Array)
            {
                InitializeList(value);
                if (value < 1)
                    SetReady(true);
                else
                {
                    var items = m_ReadOnlyList;
                    SetReady(items.Count == value);
                }
            }
        }

        internal void SetParent(IRedisRawResponse value)
        {
            var parent = Interlocked.Exchange(ref m_Parent, value);
            if (parent != null && value != parent)
            {
                var response = parent as RedisRawResponse;
                if (response != null)
                    response.Remove(this);
            }
        }

        internal void SetReady(bool value)
        {
            Interlocked.Exchange(ref m_Ready, value ? RedisConstants.True : RedisConstants.False);
        }

        internal void SetType(RedisRawObjType value)
        {
            if (!m_Type.HasValue)
            {
                m_Type = value;
                m_TypeByte = value.ResponseTypeByte();

                if (value == RedisRawObjType.Array)
                    NewArrayList();

                if (Interlocked.Read(ref m_HasData) == RedisConstants.True)
                    SetReady(true);
            }
        }

        internal void SetTypeByte(int value)
        {
            if (m_TypeByte < 0 && value > -1 && value < 256)
            {
                m_TypeByte = value;
                SetType(((byte)value).ResponseType());
            }
        }

        public virtual byte[] ReleaseData()
        {
            var data = Interlocked.Exchange(ref m_Data, null);
            if (m_Type != RedisRawObjType.Array)
                SetReady(Length > -1);
            return data;
        }

        internal void Add(IRedisRawResponse item)
        {
            ValidateNotDisposed();

            if (m_Type != RedisRawObjType.Array)
                throw new ArgumentException("Can not add item to " + Type.ToString("F") + " type", "item");

            if (item == null)
                throw new ArgumentNullException("item");

            if (item == this)
                throw new ArgumentException("Circular reference", "item");

            var response = item as RedisRawResponse;
            if (response != null)
                response.SetParent(this);

            var list = GetArrayList();
            list.Add(item);

            SetReady(list.Count >= Length);
        }

        internal void Remove(IRedisRawResponse item)
        {
            ValidateNotDisposed();

            if (m_Type != RedisRawObjType.Array)
                throw new ArgumentException("Can not add/remove item to/from " + Type.ToString("F") + " type", "item");

            if (item == null)
                throw new ArgumentNullException("item");

            if (item == this)
                throw new ArgumentException("Circular reference", "item");

            if (item.Parent != this)
                throw new ArgumentException("Item does not belong to this response", "item");

            var response = item as RedisRawResponse;
            if (response != null)
                response.SetParent(null);

            var list = GetArrayList();
            list.Remove(item);

            SetReady(list.Count >= Length);
        }

        protected void ClearInternal()
        {
            Interlocked.Exchange(ref m_Data, null);
            if (m_Type != RedisRawObjType.Array)
                SetReady(Length > -1);

            var arrayItems = Interlocked.Exchange(ref m_List, null);
            if (arrayItems != null)
            {
                Interlocked.Exchange(ref m_ReadOnlyList, null);
                foreach (var item in arrayItems)
                    item.Dispose();
                arrayItems.Clear();
            }

            if (m_Type == RedisRawObjType.Array)
                SetReady(Length == -1);
        }

        public virtual void Clear()
        {
            ValidateNotDisposed();
            ClearInternal();
        }

        private void InitializeList(int value)
        {
            if (value < 1)
            {
                var list = (value < 0) ? null : new List<IRedisRawResponse>();

                var oldList = Interlocked.Exchange(ref m_List, list);
                Interlocked.Exchange(ref m_ReadOnlyList, list == null ? null : new ReadOnlyCollection<IRedisRawResponse>(list));

                if (oldList != null)
                {
                    for (var i = oldList.Count - 1; i > -1; i--)
                    {
                        var response = oldList[i];
                        response.Dispose();
                    }
                    oldList.Clear();
                }
            }
            else
            {
                var list = GetArrayList();

                var count = 0;
                while ((count = list.Count) > value)
                {
                    var response = list[count - 1];
                    list.RemoveAt(count - 1);
                    response.Dispose();
                }
            }
        }

        private IList<IRedisRawResponse> GetArrayList()
        {
            return m_List ?? NewArrayList();
        }

        private IList<IRedisRawResponse> NewArrayList()
        {
            ValidateNotDisposed();

            var list = (m_Type == RedisRawObjType.Array) ? new List<IRedisRawResponse>() : null;

            Interlocked.Exchange(ref m_List, list);
            Interlocked.Exchange(ref m_ReadOnlyList, new ReadOnlyCollection<IRedisRawResponse>(list));

            return list;
        }

        #endregion Methods
    }
}
