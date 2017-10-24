using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Sweet.Redis
{
    public class RedisMasterSwitchedMessage
    {
        #region .Ctors

        public RedisMasterSwitchedMessage(string masterName, RedisEndPoint oldEndPoint, RedisEndPoint newEndPoint)
        {
            MasterName = masterName;
            OldEndPoint = oldEndPoint;
            NewEndPoint = newEndPoint;
        }

        #endregion .Ctors

        #region Properties

        public string MasterName { get; private set; }

        public RedisEndPoint OldEndPoint { get; private set; }

        public RedisEndPoint NewEndPoint { get; private set; }

        #endregion Properties
    }
}
