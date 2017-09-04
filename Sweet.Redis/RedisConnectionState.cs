using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Sweet.Redis
{
    public enum RedisConnectionState : int
    {
        Idle = 0,
        Connecting = 1,
        Connected = 2,
        Failed = 3,
        Disposed = 4
    }
}
