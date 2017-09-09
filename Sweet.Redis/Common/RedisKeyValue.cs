namespace Sweet.Redis
{
    public struct RedisKeyValue<T, K>
    {
        #region .Ctors

        public RedisKeyValue(T key, K value)
            : this()
        {
            Key = key;
            Value = value;
        }

        #endregion .Ctors

        #region Properties

        public T Key { get; private set; }

        public K Value { get; private set; }

        #endregion Properties
    }
}
