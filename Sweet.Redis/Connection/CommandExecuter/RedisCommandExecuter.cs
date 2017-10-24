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

namespace Sweet.Redis
{
    public abstract class RedisCommandExecuter : RedisDisposable, IRedisCommandExecuter
    {
        #region Field Members

        private long m_Id;
        private RedisConnectionSettings m_Settings;

        #endregion Field Members

        #region .Ctors

        public RedisCommandExecuter(RedisConnectionSettings settings)
        {
            if (settings == null)
                throw new RedisFatalException(new ArgumentNullException("settings"), RedisErrorCode.MissingParameter);

            m_Settings = settings;
            m_Id = RedisIDGenerator<RedisCommandExecuter>.NextId();
        }

        #endregion .Ctors

        #region Properties

        public virtual RedisEndPoint EndPoint
        {
            get
            {
                var settings = Settings;
                if (settings != null)
                {
                    var endPoints = settings.EndPoints;
                    if (endPoints != null && endPoints.Length > 0)
                    {
                        foreach (var ep in endPoints)
                            if (ep != null)
                                return (RedisEndPoint)ep.Clone();
                    }
                }

                return RedisEndPoint.Empty;
            }
        }

        public long Id { get { return m_Id; } }

        public RedisConnectionSettings Settings { get { return m_Settings; } }

        #endregion Properties

        #region Methods

        IRedisConnection IRedisConnectionProvider.Connect(int dbIndex, RedisRole expectedRole)
        {
            return this.Connect(dbIndex, expectedRole);
        }

        protected internal abstract IRedisConnection Connect(int dbIndex, RedisRole expectedRole);

        #region IRedisCommandExecuter Methods

        protected internal virtual RedisResponse Execute(RedisCommand command, bool throwException = true)
        {
            if (command == null)
                throw new ArgumentNullException("command");

            ValidateNotDisposed();

            using (var connection = Connect(command.DbIndex, command.Role))
            {
                return command.Execute(connection, throwException);
            }
        }

        protected internal virtual RedisRaw ExpectArray(RedisCommand command, bool throwException = true)
        {
            if (command == null)
                throw new ArgumentNullException("command");

            ValidateNotDisposed();

            using (var connection = Connect(command.DbIndex, command.Role))
            {
                return command.ExpectArray(connection, throwException);
            }
        }

        protected internal virtual RedisString ExpectBulkString(RedisCommand command, bool throwException = true)
        {
            if (command == null)
                throw new ArgumentNullException("command");

            ValidateNotDisposed();

            using (var connection = Connect(command.DbIndex, command.Role))
            {
                return command.ExpectBulkString(connection, throwException);
            }
        }

        protected internal virtual RedisBytes ExpectBulkStringBytes(RedisCommand command, bool throwException = true)
        {
            if (command == null)
                throw new ArgumentNullException("command");

            ValidateNotDisposed();

            using (var connection = Connect(command.DbIndex, command.Role))
            {
                return command.ExpectBulkStringBytes(connection, throwException);
            }
        }

        protected internal virtual RedisDouble ExpectDouble(RedisCommand command, bool throwException = true)
        {
            if (command == null)
                throw new ArgumentNullException("command");

            ValidateNotDisposed();

            using (var connection = Connect(command.DbIndex, command.Role))
            {
                return command.ExpectDouble(connection, throwException);
            }
        }

        protected internal virtual RedisBool ExpectGreaterThanZero(RedisCommand command, bool throwException = true)
        {
            if (command == null)
                throw new ArgumentNullException("command");

            ValidateNotDisposed();

            using (var connection = Connect(command.DbIndex, command.Role))
            {
                return command.ExpectInteger(connection, throwException) > RedisConstants.Zero;
            }
        }

        protected internal virtual RedisInteger ExpectInteger(RedisCommand command, bool throwException = true)
        {
            if (command == null)
                throw new ArgumentNullException("command");

            ValidateNotDisposed();

            using (var connection = Connect(command.DbIndex, command.Role))
            {
                return command.ExpectInteger(connection, throwException);
            }
        }

        protected internal virtual RedisMultiBytes ExpectMultiDataBytes(RedisCommand command, bool throwException = true)
        {
            if (command == null)
                throw new ArgumentNullException("command");

            ValidateNotDisposed();

            using (var connection = Connect(command.DbIndex, command.Role))
            {
                return command.ExpectMultiDataBytes(connection, throwException);
            }
        }

        protected internal virtual RedisMultiString ExpectMultiDataStrings(RedisCommand command, bool throwException = true)
        {
            if (command == null)
                throw new ArgumentNullException("command");

            ValidateNotDisposed();

            using (var connection = Connect(command.DbIndex, command.Role))
            {
                return command.ExpectMultiDataStrings(connection, throwException);
            }
        }

        protected internal virtual RedisVoid ExpectNothing(RedisCommand command, bool throwException = true)
        {
            if (command == null)
                throw new ArgumentNullException("command");

            ValidateNotDisposed();

            using (var connection = Connect(command.DbIndex, command.Role))
            {
                return command.ExpectNothing(connection, throwException);
            }
        }

        protected internal virtual RedisNullableDouble ExpectNullableDouble(RedisCommand command, bool throwException = true)
        {
            if (command == null)
                throw new ArgumentNullException("command");

            ValidateNotDisposed();

            using (var connection = Connect(command.DbIndex, command.Role))
            {
                return command.ExpectNullableDouble(connection, throwException);
            }
        }

        protected internal virtual RedisNullableInteger ExpectNullableInteger(RedisCommand command, bool throwException = true)
        {
            if (command == null)
                throw new ArgumentNullException("command");

            ValidateNotDisposed();

            using (var connection = Connect(command.DbIndex, command.Role))
            {
                return command.ExpectNullableInteger(connection, throwException);
            }
        }

        protected internal virtual RedisBool ExpectOK(RedisCommand command, bool throwException = true)
        {
            if (command == null)
                throw new ArgumentNullException("command");

            ValidateNotDisposed();

            using (var connection = Connect(command.DbIndex, command.Role))
            {
                return command.ExpectSimpleString(connection, "OK", throwException);
            }
        }

        protected internal virtual RedisBool ExpectOne(RedisCommand command, bool throwException = true)
        {
            if (command == null)
                throw new ArgumentNullException("command");

            ValidateNotDisposed();

            using (var connection = Connect(command.DbIndex, command.Role))
            {
                return command.ExpectOne(connection, throwException);
            }
        }

        protected internal virtual RedisBool ExpectSimpleString(RedisCommand command, string expectedResult, bool throwException = true)
        {
            if (command == null)
                throw new ArgumentNullException("command");

            ValidateNotDisposed();

            using (var connection = Connect(command.DbIndex, command.Role))
            {
                return command.ExpectSimpleString(connection, expectedResult, throwException);
            }
        }

        protected internal virtual RedisString ExpectSimpleString(RedisCommand command, bool throwException = true)
        {
            if (command == null)
                throw new ArgumentNullException("command");

            ValidateNotDisposed();

            using (var connection = Connect(command.DbIndex, command.Role))
            {
                return command.ExpectSimpleString(connection, throwException);
            }
        }

        protected internal virtual RedisBool ExpectSimpleStringBytes(RedisCommand command, byte[] expectedResult, bool throwException = true)
        {
            if (command == null)
                throw new ArgumentNullException("command");

            ValidateNotDisposed();

            using (var connection = Connect(command.DbIndex, command.Role))
            {
                return command.ExpectSimpleStringBytes(connection, expectedResult, throwException);
            }
        }

        protected internal virtual RedisBytes ExpectSimpleStringBytes(RedisCommand command, bool throwException = true)
        {
            if (command == null)
                throw new ArgumentNullException("command");

            ValidateNotDisposed();

            using (var connection = Connect(command.DbIndex, command.Role))
            {
                return command.ExpectSimpleStringBytes(connection, throwException);
            }
        }

        #endregion IRedisCommandExecuter Methods

        #region IRedisCommandExecuter Methods

        RedisResponse IRedisCommandExecuter.Execute(RedisCommand command, bool throwException)
        {
            return this.Execute(command, throwException);
        }

        RedisRaw IRedisCommandExecuter.ExpectArray(RedisCommand command, bool throwException)
        {
            return this.ExpectArray(command, throwException);
        }

        RedisString IRedisCommandExecuter.ExpectBulkString(RedisCommand command, bool throwException)
        {
            return this.ExpectBulkString(command, throwException);
        }

        RedisBytes IRedisCommandExecuter.ExpectBulkStringBytes(RedisCommand command, bool throwException)
        {
            return this.ExpectBulkStringBytes(command, throwException);
        }

        RedisDouble IRedisCommandExecuter.ExpectDouble(RedisCommand command, bool throwException)
        {
            return this.ExpectDouble(command, throwException);
        }

        RedisBool IRedisCommandExecuter.ExpectGreaterThanZero(RedisCommand command, bool throwException)
        {
            return this.ExpectGreaterThanZero(command, throwException);
        }

        RedisInteger IRedisCommandExecuter.ExpectInteger(RedisCommand command, bool throwException)
        {
            return this.ExpectInteger(command, throwException);
        }

        RedisMultiBytes IRedisCommandExecuter.ExpectMultiDataBytes(RedisCommand command, bool throwException)
        {
            return this.ExpectMultiDataBytes(command, throwException);
        }

        RedisMultiString IRedisCommandExecuter.ExpectMultiDataStrings(RedisCommand command, bool throwException)
        {
            return this.ExpectMultiDataStrings(command, throwException);
        }

        RedisVoid IRedisCommandExecuter.ExpectNothing(RedisCommand command, bool throwException)
        {
            return this.ExpectNothing(command, throwException);
        }

        RedisNullableDouble IRedisCommandExecuter.ExpectNullableDouble(RedisCommand command, bool throwException)
        {
            return this.ExpectNullableDouble(command, throwException);
        }

        RedisNullableInteger IRedisCommandExecuter.ExpectNullableInteger(RedisCommand command, bool throwException)
        {
            return this.ExpectNullableInteger(command, throwException);
        }

        RedisBool IRedisCommandExecuter.ExpectOK(RedisCommand command, bool throwException)
        {
            return this.ExpectOK(command, throwException);
        }

        RedisBool IRedisCommandExecuter.ExpectOne(RedisCommand command, bool throwException)
        {
            return this.ExpectOne(command, throwException);
        }

        RedisBool IRedisCommandExecuter.ExpectSimpleString(RedisCommand command, string expectedResult, bool throwException)
        {
            return this.ExpectSimpleString(command, expectedResult, throwException);
        }

        RedisString IRedisCommandExecuter.ExpectSimpleString(RedisCommand command, bool throwException)
        {
            return this.ExpectSimpleString(command, throwException);
        }

        RedisBool IRedisCommandExecuter.ExpectSimpleStringBytes(RedisCommand command, byte[] expectedResult, bool throwException)
        {
            return this.ExpectSimpleStringBytes(command, expectedResult, throwException);
        }

        RedisBytes IRedisCommandExecuter.ExpectSimpleStringBytes(RedisCommand command, bool throwException)
        {
            return this.ExpectSimpleStringBytes(command, throwException);
        }

        #endregion IRedisCommandExecuter Methods

        #endregion Methods
    }
}
