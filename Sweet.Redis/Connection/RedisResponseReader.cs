﻿#region License
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
using System.Text;

namespace Sweet.Redis
{
    internal class RedisResponseReader : RedisDisposable
    {
        #region Methods

        protected override void OnDispose(bool disposing)
        {
            base.OnDispose(disposing);
        }

        public IRedisResponse Execute(RedisSocket socket)
        {
            using (var buffer = new RedisByteBuffer())
            {
                var item = ReadThrough(socket, buffer, true);

                var available = 0;
                if (buffer.Position < buffer.Length || (available = socket.Available) > 0)
                {
                    var siblings = new List<IRedisResponse>();
                    do
                    {
                        var sibling = ReadThrough(socket, buffer, available > 0);
                        if (sibling != null)
                            siblings.Add(sibling);
                    }
                    while (buffer.Position < buffer.Length || (available = socket.Available) > 0);

                    var siblingCount = siblings.Count;
                    if (siblingCount > 0)
                    {
                        var parent = new RedisResponse(type: RedisRawObjType.Array);
                        parent.Length = siblingCount + 1;

                        parent.Add(item);
                        for (var i = 0; i < siblingCount; i++)
                            parent.Add(siblings[i]);

                        return parent;
                    }
                }
                return item;
            }
        }

        private IRedisResponse ReadThrough(RedisSocket socket, RedisByteBuffer buffer, bool receiveMore = false)
        {
            var item = new RedisResponse();
            while (receiveMore || !item.Ready)
            {
                if (!receiveMore)
                    receiveMore = NeedToReceiveMore(item, buffer);

                if (receiveMore)
                {
                    receiveMore = false;
                    Receive(socket, buffer);
                }

                if (item.Length < -1)
                {
                    ReadObjectType(item, buffer);
                    if (ReadHeader(item, buffer, out receiveMore))
                        return item;

                    if (receiveMore)
                        continue;
                }

                if (ReadBody(item, socket, buffer, out receiveMore))
                    return item;
            }
            return null;
        }

        private static bool NeedToReceiveMore(RedisResponse item, RedisByteBuffer buffer)
        {
            return (buffer.Length == 0) ||
                (item.Type != RedisRawObjType.Array &&
                 (item.Length > buffer.Length - buffer.Position + RedisConstants.CRLFLength));
        }

        private void Receive(RedisSocket socket, RedisByteBuffer buffer)
        {
            if (socket == null || !socket.IsConnected())
                throw new RedisException("Can not establish socket to complete redis response read");

            var offset = 0;
            var length = 0;
            var data = (byte[])null;

            var remainingLength = socket.Available;
            do
            {
                if (length == 0)
                {
                    offset = 0;
                    length = (remainingLength == 0) ?
                        RedisConstants.ReadBufferSize :
                                      remainingLength;

                    data = new byte[length];
                }

                var receivedLength = socket.ReceiveAsync(data, offset, length).Result;

                if (receivedLength > 0)
                    offset += receivedLength;

                if (receivedLength < length)
                {
                    if (offset > 0)
                    {
                        if (offset == data.Length)
                            buffer.Put(data);
                        else
                        {
                            var tmp = new byte[offset];
                            Buffer.BlockCopy(data, 0, tmp, 0, offset);

                            buffer.Put(tmp);
                        }
                    }

                    break;
                }

                length -= receivedLength;
                if (length == 0)
                {
                    buffer.Put(data);

                    data = null;
                    offset = 0;
                }
            }
            while ((remainingLength = socket.Available) > 0);
        }

        private bool ReadHeader(RedisResponse item, RedisByteBuffer buffer, out bool receiveMore)
        {
            receiveMore = false;

            var header = buffer.ReadLine();

            receiveMore = (header == null);
            if (receiveMore)
                return false;

            switch (item.Type)
            {
                case RedisRawObjType.SimpleString:
                case RedisRawObjType.Error:
                case RedisRawObjType.Integer:
                    {
                        item.Data = header;
                        SetReady(item);

                        return true;
                    }
                default:
                    {
                        var lenStr = Encoding.UTF8.GetString(header);
                        if (String.IsNullOrEmpty(lenStr))
                            throw new RedisException("Corrupted redis response, empty length string");

                        int msgLength;
                        if (!int.TryParse(lenStr, out msgLength))
                            throw new RedisException("Corrupted redis response, not an integer value");

                        msgLength = Math.Max(-1, msgLength);
                        item.Length = msgLength;

                        if (msgLength == -1)
                        {
                            item.Data = null;
                            SetReady(item);

                            return true;
                        }

                        if (msgLength == 0)
                        {
                            if (item.Type == RedisRawObjType.BulkString)
                            {
                                item.Data = new byte[0];
                                SetReady(item);

                                receiveMore = !buffer.EatCRLF();
                                return !receiveMore;
                            }

                            SetReady(item);
                            return true;
                        }

                        receiveMore = ((item.Type == RedisRawObjType.BulkString) &&
                                       (buffer.Length < buffer.Position + msgLength + RedisConstants.CRLFLength));
                    }
                    break;
            }

            return false;
        }

        private bool ReadBody(RedisResponse item, RedisSocket socket, RedisByteBuffer buffer, out bool receiveMore)
        {
            receiveMore = false;
            if (socket == null || !socket.IsConnected())
                throw new RedisException("Can not establish socket to complete redis response read");

            switch (item.Type)
            {
                case RedisRawObjType.BulkString:
                    {
                        if (item.Length > 0)
                        {
                            receiveMore = (buffer.Length < buffer.Position + item.Length + RedisConstants.CRLFLength);
                            if (receiveMore)
                                return false;

                            var data = buffer.Read(item.Length);

                            receiveMore = (data == null);
                            if (receiveMore)
                                return false;

                            item.Data = data;
                            SetReady(item);

                            receiveMore = !buffer.EatCRLF();

                            return !receiveMore;
                        }

                        if (item.Length == 0)
                        {
                            receiveMore = !buffer.EatCRLF();
                            return !receiveMore;
                        }

                        if (item.Length == -1)
                            return true;
                    }
                    break;
                case RedisRawObjType.Array:
                    {
                        for (var i = 0; i < item.Length; i++)
                        {
                            var child = ReadThrough(socket, buffer);
                            if (child == null)
                                throw new RedisException("Unexpected response data, not valid data for array item");

                            item.Add(child);
                        }
                        return true;
                    }
            }
            return false;
        }

        private static void ReadObjectType(RedisResponse item, RedisByteBuffer buffer)
        {
            if (item.TypeByte < 0)
            {
                var b = buffer.ReadByte();
                if (b < 0)
                    throw new RedisException("Unexpected byte for redis response type");

                item.TypeByte = b;
                if (item.Type == RedisRawObjType.Undefined)
                    throw new RedisException("Undefined redis response type");
            }
        }

        private static void SetReady(RedisResponse child)
        {
            child.Ready = true;

            var parent = child.Parent as RedisResponse;
            if (parent != null)
            {
                var count = parent.ChildCount;
                if (count == 0 || count == parent.Length)
                    SetReady(parent);
            }
        }

        #endregion Methods
    }
}
