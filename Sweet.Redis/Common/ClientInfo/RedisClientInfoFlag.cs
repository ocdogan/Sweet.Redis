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
    [Flags]
    public enum RedisClientInfoFlag : long
    {
        /*
        N: no specific flag set
        O: the client is a slave in MONITOR mode
        S: the client is a normal slave server
        M: the client is a master
        x: the client is in a MULTI/EXEC context
        b: the client is waiting in a blocking operation
        i: the client is waiting for a VM I/O (deprecated)
        d: a watched keys has been modified - EXEC will fail
        c: connection to be closed after writing entire reply
        u: the client is unblocked
        U: the client is connected via a Unix domain socket
        r: the client is in readonly mode against a cluster node
        A: connection to be closed ASAP
        */
        None = 0, // N
        MonitoringSlave = 1 << 0, // O
        Slave = 1 << 1, // S
        Master = 1 << 2, // M
        MutiExecContext = 1 << 3, // x
        WaitingBlockingOp = 1 << 4, // b
        WaitingIO = 1 << 5, // i
        WatchedKeysModified = 1 << 6, // d
        ClosingAfterReply = 1 << 7, // c
        Unblocked = 1 << 8, // u
        UnixSocket = 1 << 9, // U
        ReadOnlyClusterNode = 1 << 10, // r
        ClosingASAP = 1 << 11, // A
    }
}
