using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Threading;

namespace Sweet.Redis
{
    public class RedisResponse : RedisDisposable, IRedisResponse
    {
        #region Field Members

        private byte[] m_Data;
        private long m_Ready;
        private int m_TypeByte = -1;
        private RedisObjectType? m_Type;
        private long m_Length = int.MinValue;
        private IRedisResponse m_Parent;
        private IList<IRedisResponse> m_List;
        private IList<IRedisResponse> m_ReadOnlyList;

        #endregion Field Members

        #region .Ctors

        public RedisResponse(IRedisResponse parent = null)
        {
            m_Parent = parent;
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

        public int ChildCount
        {
            get
            {
                if (m_Type != RedisObjectType.Array)
                    return -1;

                var list = m_List;
                return (list != null) ? list.Count : 0;
            }
        }

        public byte[] Data
        {
            get { return m_Data; }
            internal set
            {
                m_Data = value;
                if (m_Type != RedisObjectType.Array)
                    Ready = true;
            }
        }

        public bool HasChild
        {
            get
            {
                if (m_Type != RedisObjectType.Array)
                    return false;

                var list = m_List;
                return (list != null) && list.Count > 0;
            }
        }

        public bool HasData
        {
            get
            {
                if (m_Type == RedisObjectType.Array)
                    return false;

                var data = m_Data;
                return (data != null) && data.Length > 0;
            }
        }

        public IList<IRedisResponse> Items
        {
            get { return m_ReadOnlyList; }
        }

        public int Length
        {
            get { return (int)Interlocked.Read(ref m_Length); }
            internal set
            {
                value = Math.Max(-1, value);
                Interlocked.Exchange(ref m_Length, value);

                if (m_Type == RedisObjectType.Array)
                {
                    InitializeList(value);
                    if (value < 1)
                        Ready = true;
                    else
                    {
                        var items = m_ReadOnlyList;
                        Ready = items.Count == value;
                    }
                }
            }
        }

        public IRedisResponse Parent
        {
            get { return m_Parent; }
            internal set
            {
                var parent = Interlocked.Exchange(ref m_Parent, value);
                if (parent != null && value != parent)
                {
                    var response = parent as RedisResponse;
                    if (response != null)
                        response.Remove(this);
                }
            }
        }

        public bool Ready
        {
            get { return Interlocked.Read(ref m_Ready) != 0L; }
            internal set
            {
                Interlocked.Exchange(ref m_Ready, value ? 1L : 0L);
            }
        }

        public RedisObjectType Type
        {
            get { return m_Type.HasValue ? m_Type.Value : RedisObjectType.Undefined; }
            internal set
            {
                if (!m_Type.HasValue)
                {
                    m_Type = value;
                    m_TypeByte = value.ResponseTypeByte();

                    if (value == RedisObjectType.Array)
                    {
                        NewArrayList();
                    }
                }
            }
        }

        public int TypeByte
        {
            get { return m_TypeByte; }
            internal set
            {
                if (m_TypeByte < 0 && value > -1 && value < 256)
                {
                    m_TypeByte = value;
                    Type = ((byte)value).ResponseType();
                }
            }
        }

        #endregion Properties

        #region Methods

        public byte[] ReleaseData()
        {
            var data = Interlocked.Exchange(ref m_Data, null);
            if (m_Type != RedisObjectType.Array)
                Ready = Length > -1;
            return data;
        }

        internal void Add(IRedisResponse item)
        {
            ValidateNotDisposed();

            if (m_Type != RedisObjectType.Array)
                throw new ArgumentException("Can not add item to " + Type.ToString("F") + " type", "item");

            if (item == null)
                throw new ArgumentNullException("item");

            if (item == this)
                throw new ArgumentException("Circular reference", "item");

            var response = item as RedisResponse;
            if (response != null)
                response.Parent = this;

            var list = GetArrayList();
            list.Add(item);

            Ready = list.Count >= Length;
        }

        internal void Remove(IRedisResponse item)
        {
            ValidateNotDisposed();

            if (m_Type != RedisObjectType.Array)
                throw new ArgumentException("Can not add/remove item to/from " + Type.ToString("F") + " type", "item");

            if (item == null)
                throw new ArgumentNullException("item");

            if (item == this)
                throw new ArgumentException("Circular reference", "item");

            if (item.Parent != this)
                throw new ArgumentException("Item does not belong to this response", "item");

            var response = item as RedisResponse;
            if (response != null)
                response.Parent = null;

            var list = GetArrayList();
            list.Remove(item);

            Ready = list.Count >= Length;
        }

        protected void ClearInternal()
        {
            Interlocked.Exchange(ref m_Data, null);
            if (m_Type != RedisObjectType.Array)
                Ready = Length > -1;

            var arrayItems = Interlocked.Exchange(ref m_List, null);
            if (arrayItems != null)
            {
                Interlocked.Exchange(ref m_ReadOnlyList, null);
                foreach (var item in arrayItems)
                    item.Dispose();
                arrayItems.Clear();
            }

            if (m_Type == RedisObjectType.Array)
                Ready = Length == -1;
        }

        public void Clear()
        {
            ValidateNotDisposed();
            ClearInternal();
        }

        private void InitializeList(int value)
        {
            if (value < 1)
            {
                var list = (value < 0) ? null : new List<IRedisResponse>();
                var oldList = Interlocked.Exchange(ref m_List, list);
                Interlocked.Exchange(ref m_ReadOnlyList, new ReadOnlyCollection<IRedisResponse>(list));

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

        private IList<IRedisResponse> GetArrayList()
        {
            return m_List ?? NewArrayList();
        }

        private IList<IRedisResponse> NewArrayList()
        {
            ValidateNotDisposed();

            var list = (m_Type == RedisObjectType.Array) ? new List<IRedisResponse>() : null;

            Interlocked.Exchange(ref m_List, list);
            Interlocked.Exchange(ref m_ReadOnlyList, new ReadOnlyCollection<IRedisResponse>(list));

            return list;
        }

        #endregion Methods
    }
}
