namespace Sweet.Redis
{
    public struct RedisPubSubMessage
    {
        #region .Ctors

        public RedisPubSubMessage(RedisPubSubType type, string typeStr, string channel, byte[] data)
            : this()
        {
            Channel = channel;
            Data = data;
            Type = type;
            TypeStr = typeStr;
        }

        #endregion .Ctors

        #region Properties

        public string Channel { get; private set; }

        public byte[] Data { get; private set; }

        public RedisPubSubType Type { get; private set; }

        public string TypeStr { get; private set; }

        #endregion Properties
    }
}
