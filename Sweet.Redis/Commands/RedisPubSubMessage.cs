namespace Sweet.Redis
{
    public struct RedisPubSubMessage
    {
        #region .Ctors

        public RedisPubSubMessage(bool isPSub, string channel, byte[] data)
            : this()
        {
            Channel = channel;
            Data = data;
            IsPSub = isPSub;
        }

        #endregion .Ctors

        #region Properties

        public string Channel { get; private set; }

        public byte[] Data { get; private set; }

        public bool IsPSub { get; private set; }

        #endregion Properties
    }
}
