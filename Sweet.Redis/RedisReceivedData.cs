using System;

namespace Sweet.Redis
{
    public struct RedisReceivedData
    {
        #region Static Members

        public static readonly RedisReceivedData Empty = new RedisReceivedData(0);

        #endregion Static Members

        #region .Ctors

        public RedisReceivedData(byte dummy = 0)
            : this()
        {
            Available = 0;
            Data = null;
            Offset = 0;
            Length = 0;
            IsEmpty = true;
        }

        public RedisReceivedData(byte[] data, int offset = 0, int length = -1, int available = 0)
            : this()
        {
            offset = Math.Max(0, data == null ? 0 : Math.Min(offset, data.Length));

            var maxLength = data.Length - offset;
            length = Math.Max(0, data == null ? 0 : (length < 0 ? maxLength : Math.Min(length, maxLength)));

            available = Math.Max(0, available);
            var isEmpty = data == null || data.Length == 0 ||
                        length == 0 || offset == data.Length;

            Available = available;
            Data = data;
            Offset = offset;
            Length = length;
            IsEmpty = isEmpty;
        }

        #endregion .Ctors

        #region Properties

        public int Available { get; private set; }

        public byte[] Data { get; private set; }

        public bool IsEmpty { get; private set; }

        public int Offset { get; private set; }

        public int Length { get; private set; }

        #endregion Properties
    }
}
