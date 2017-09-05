using System;

namespace Sweet.Redis
{
    public struct RedisReceivedData
    {
        #region Static Members

        public static readonly RedisReceivedData Empty = new RedisReceivedData(new byte[0], 0, 0);

        #endregion Static Members

        #region .Ctors

        public RedisReceivedData(byte[] data, int offset = 0, int length = -1)
            : this()
        {
            offset = Math.Max(0, data == null ? 0 : Math.Min(offset, data.Length));

            var maxLength = data.Length - offset;
            length = Math.Max(0, data == null ? 0 : (length < 0 ? maxLength : Math.Min(length, maxLength)));

            var isEmpty = data == null || data.Length == 0 ||
                        length == 0 || offset == data.Length;

            Data = data;
            Offset = offset;
            Length = length;
            IsEmpty = isEmpty;
        }

        #endregion .Ctors

        #region Properties

        public byte[] Data { get; private set; }

        public bool IsEmpty { get; private set; }

        public int Offset { get; private set; }

        public int Length { get; private set; }

        #endregion Properties
    }
}
