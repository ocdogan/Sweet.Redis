using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Text;
using System.Threading;

using Sweet.Redis;

namespace Sweet.Redis.ConsoleTest
{
    class Program
    {
        static void Main(string[] args)
        {
            // PerformanceTest1();
            // PerformanceTest2();

            // ScriptingNoArgsEvalTest();
            // ScriptingShaNoArgsEvalTest();
            // ScriptingWithArgsEvalTest();
            // ScriptingShaWithArgsEvalTest();

            // PubSubTest1();
            // PubSubTest2();
            // PubSubTest3();
            // PubSubTest4();
            // PubSubTest5();
            // PubSubTest6();
            // PubSubTest7();
            // PubSubTest8();
            // PubSubTest9();
            // PubSubTest10();
            // PubSubTest11();
            // PubSubTest12();

            // MultiThreading1();
            // MultiThreading2a();
            // MultiThreading2b();
            // MultiThreading3();
            // MultiThreading4();
            // MultiThreading5();
            // MultiThreading6();
            // MultiThreading7a();           
            // MultiThreading7b();
            // MultiThreading8();

            // MonitorTest1();
            // MonitorTest2();
            // MonitorTest3();

            // GeoTest1();

            // SlowLogTest1();

            // InfoTest1();

            // ShutdownTest1();

            // TransactionTest1();
            // TransactionTest2();
            // TransactionTest3();
            // TransactionTest4();
            // TransactionTest5();

            // PipelineTest1();
            // PipelineTest2();
            // PipelineTest3();
            // PipelineTest4();
            // PipelineTest5();

            // SentinelTest1();
            // SentinelTest2();
            // SentinelTest3();
            // SentinelTest4();
            // SentinelTest5();
            // SentinelTest6();
            // SentinelTest7();
            // SentinelTest8();
            // SentinelTest9();

            // ManagerTest1();
            // ManagerTest2();
            // ManagerTest3();
            // ManagerTest4();
            // ManagerTest5();
            // ManagerTest6();
            // ManagerTest7();
            // ManagerTest8();
            // ManagerTest9();
            ManagerTest10();
        }

        #region Manager

        static void ManagerTest10()
        {
            try
            {
                var i = 0;
                using (var manager = new RedisManager("My Manager", new RedisManagerSettings(
                    new[] { new RedisEndPoint("127.0.0.1", RedisConstants.DefaultSentinelPort) },
                    masterName: "mymaster")))
                {
                    Console.Clear();

                    Console.WriteLine();
                    Console.WriteLine("Press any key to continue, ESC to escape ...");

                    var modKey = 0;
                    do
                    {
                        var ch = (char)('0' + (i++ % 10));
                        var text = i.ToString() + "-" + new string(ch, 10);

                        try
                        {
                            using (var db = manager.GetDb())
                            {
                                Ping(db, false);
                                SetGet(db, "tinytext", text, false);
                            }

                            using (var db = manager.GetDb(true))
                            {
                                Ping(db, false);
                                Get(db, "tinytext", false);
                            }
                        }
                        catch (Exception)
                        {
                            /* Console.WriteLine(e);
                            Console.WriteLine();
                            Console.WriteLine("Press any key to continue, ESC to escape ..."); */
                        }

                        modKey = (modKey + 1) % 100;
                        if (modKey == 99 && WaitForConsoleKey(50).Key == ConsoleKey.Escape)
                            return;
                    }
                    while (true);
                }
            }
            catch (Exception e)
            {
                Console.WriteLine(e);
            }
        }

        static ConsoleKeyInfo WaitForConsoleKey(int milliSecond)
        {
            var delay = 0;
            while (delay < milliSecond)
            {
                if (Console.KeyAvailable)
                    return Console.ReadKey();

                Thread.Sleep(50);
                delay += 50;
            }
            return new ConsoleKeyInfo((char)0, (ConsoleKey)0, false, false, false);
        }

        static void ManagerTest9()
        {
            var i = 0;
            var sw = new Stopwatch();

            using (var manager = new RedisManager("My Manager", new RedisManagerSettings(
                new[] { new RedisEndPoint("127.0.0.1", RedisConstants.DefaultSentinelPort) },
                masterName: "mymaster")))
            {
                do
                {
                    try
                    {
                        Console.Clear();

                        for (var j = 0; j < 100; j++)
                        {
                            var ch = (char)('0' + (i++ % 10));
                            var text = i.ToString() + "-" + new string(ch, 10);

                            sw.Restart();
                            using (var db = manager.GetDb())
                            {
                                Ping(db);
                                SetGet(db, "tinytext", text);
                            }

                            using (var db = manager.GetDb(true))
                            {
                                Ping(db);
                                Get(db, "tinytext");
                            }
                        }
                    }
                    catch (Exception e)
                    {
                        Console.WriteLine(e);
                    }

                    sw.Stop();

                    Console.WriteLine();
                    Console.WriteLine("Total ticks: " + sw.ElapsedTicks);
                    Console.WriteLine("Total millisecs: " + sw.ElapsedMilliseconds);

                    Console.WriteLine();
                    Console.WriteLine("Press any key to continue, ESC to escape ...");
                }
                while (Console.ReadKey(true).Key != ConsoleKey.Escape);
            }
        }

        static void ManagerTest8()
        {
            var i = 0;
            var sw = new Stopwatch();
            do
            {
                using (var manager = new RedisManager("My Manager", new RedisManagerSettings(
                    new[] { new RedisEndPoint("127.0.0.1", RedisConstants.DefaultSentinelPort),
                        new RedisEndPoint("127.0.0.1", RedisConstants.DefaultSentinelPort + 1),
                        new RedisEndPoint("127.0.0.1", RedisConstants.DefaultSentinelPort + 2)},
                    masterName: "mymaster")))
                {
                    try
                    {
                        Console.Clear();

                        for (var j = 0; j < 100; j++)
                        {
                            var ch = (char)('0' + (i++ % 10));
                            var text = i.ToString() + "-" + new string(ch, 10);

                            sw.Restart();
                            using (var db = manager.GetDb())
                            {
                                Ping(db);
                                if (j % 10 == 3)
                                    manager.Refresh();

                                SetGet(db, "tinytext", text);
                                if (j % 10 == 9)
                                    manager.Refresh();
                            }

                            using (var db = manager.GetDb(true))
                            {
                                Get(db, "tinytext");
                                if (j % 10 == 6)
                                    manager.Refresh();
                            }
                        }
                    }
                    catch (Exception e)
                    {
                        Console.WriteLine(e);
                    }
                }

                sw.Stop();

                Console.WriteLine();
                Console.WriteLine("Total ticks: " + sw.ElapsedTicks);
                Console.WriteLine("Total millisecs: " + sw.ElapsedMilliseconds);

                Console.WriteLine();
                Console.WriteLine("Press any key to continue, ESC to escape ...");
            }
            while (Console.ReadKey(true).Key != ConsoleKey.Escape);
        }

        static void ManagerTest7()
        {
            var i = 0;
            var sw = new Stopwatch();
            do
            {
                using (var manager = new RedisManager("My Manager", new RedisManagerSettings(
                    new[] { RedisEndPoint.SentinelLocalHostEndPoint },
                    masterName: "mymaster")))
                {
                    try
                    {
                        Console.Clear();

                        for (var j = 0; j < 100; j++)
                        {
                            var ch = (char)('0' + (i++ % 10));
                            var text = i.ToString() + "-" + new string(ch, 10);

                            sw.Restart();

                            try
                            {
                                using (var db = manager.GetDb())
                                {
                                    Ping(db);
                                    if (j % 10 == 3)
                                        manager.Refresh();

                                    SetGet(db, "tinytext", text);
                                    if (j % 10 == 9)
                                        manager.Refresh();
                                }

                                using (var db = manager.GetDb(true))
                                {
                                    Get(db, "tinytext");
                                    if (j % 10 == 6)
                                        manager.Refresh();
                                }
                            }
                            catch (Exception e)
                            {
                                Console.WriteLine(e);
                            }
                        }
                    }
                    catch (Exception e)
                    {
                        Console.WriteLine(e);
                    }
                }

                sw.Stop();

                Console.WriteLine();
                Console.WriteLine("Total ticks: " + sw.ElapsedTicks);
                Console.WriteLine("Total millisecs: " + sw.ElapsedMilliseconds);

                Console.WriteLine();
                Console.WriteLine("Press any key to continue, ESC to escape ...");
            }
            while (Console.ReadKey(true).Key != ConsoleKey.Escape);
        }

        static void ManagerTest6()
        {
            var i = 0;
            var sw = new Stopwatch();
            do
            {
                using (var manager = new RedisManager("My Manager", new RedisManagerSettings(
                    new[] { RedisEndPoint.SentinelLocalHostEndPoint },
                    masterName: "mymaster")))
                {
                    try
                    {
                        Console.Clear();

                        var ch = (char)('0' + (i++ % 10));
                        var text = i.ToString() + "-" + new string(ch, 10);

                        sw.Restart();
                        using (var db = manager.GetDb())
                        {
                            Ping(db);
                            SetGet(db, "tinytext", text);
                        }

                        using (var db = manager.GetDb(true))
                        {
                            Get(db, "tinytext");
                        }
                    }
                    catch (Exception e)
                    {
                        Console.WriteLine(e);
                    }
                }

                sw.Stop();

                Console.WriteLine();
                Console.WriteLine("Total ticks: " + sw.ElapsedTicks);
                Console.WriteLine("Total millisecs: " + sw.ElapsedMilliseconds);

                Console.WriteLine();
                Console.WriteLine("Press any key to continue, ESC to escape ...");
            }
            while (Console.ReadKey(true).Key != ConsoleKey.Escape);
        }

        static void ManagerTest5()
        {
            using (var manager = new RedisManager("My Manager", new RedisManagerSettings(
                new[] { RedisEndPoint.SentinelLocalHostEndPoint },
                masterName: "mymaster")))
            {
                var i = 0;
                var sw = new Stopwatch();
                do
                {
                    try
                    {
                        Console.Clear();

                        var ch = (char)('0' + (i++ % 10));
                        var text = new string(ch, 10);

                        sw.Restart();
                        using (var db = manager.GetDb(true))
                        {
                            Ping(db);
                            SetGet(db, "tinytext", text);
                        }
                    }
                    catch (Exception e)
                    {
                        Console.WriteLine(e);
                    }

                    sw.Stop();

                    Console.WriteLine();
                    Console.WriteLine("Total ticks: " + sw.ElapsedTicks);
                    Console.WriteLine("Total millisecs: " + sw.ElapsedMilliseconds);

                    Console.WriteLine();
                    Console.WriteLine("Press any key to continue, ESC to escape ...");
                }
                while (Console.ReadKey(true).Key != ConsoleKey.Escape);
            }
        }

        static void ManagerTest4()
        {
            using (var manager = new RedisManager("My Manager", new RedisManagerSettings(
                new[] { RedisEndPoint.SentinelLocalHostEndPoint },
                masterName: "mymaster")))
            {
                var i = 0;
                var sw = new Stopwatch();
                do
                {
                    try
                    {
                        Console.Clear();

                        var ch = (char)('0' + (i++ % 10));
                        var text = new string(ch, 10);

                        sw.Restart();
                        using (var db = manager.GetDb())
                        {
                            Ping(db);
                            SetGet(db, "tinytext", text);
                        }
                    }
                    catch (Exception e)
                    {
                        Console.WriteLine(e);
                    }

                    sw.Stop();

                    Console.WriteLine();
                    Console.WriteLine("Total ticks: " + sw.ElapsedTicks);
                    Console.WriteLine("Total millisecs: " + sw.ElapsedMilliseconds);

                    Console.WriteLine();
                    Console.WriteLine("Press any key to continue, ESC to escape ...");
                }
                while (Console.ReadKey(true).Key != ConsoleKey.Escape);
            }
        }

        static void Ping(IRedisDb db, bool writeLog = true)
        {
            var result = db.Connection.Ping();
            if (writeLog)
            {
                if (result == (string)null)
                    Console.WriteLine("(nil)");
                else
                {
                    var value = result.Value;
                    if (value == null)
                        Console.WriteLine("(nil)");
                    else
                    {
                        Console.WriteLine("Response: " + value);
                        Console.WriteLine();
                    }
                }
            }
        }

        static void SetGet(IRedisDb db, string key, string value, bool writeLog = true)
        {
            var result = db.Strings.Set(key, value);
            if (result)
                Get(db, key, writeLog);
        }

        static void Get(IRedisDb db, string key, bool writeLog = true)
        {
            var result = db.Strings.Get(key);
            if (writeLog)
            {
                if (result == (string)null)
                    Console.WriteLine("(nil)");
                else
                {
                    var value = result.Value;
                    if (value == null)
                        Console.WriteLine("(nil)");
                    else
                    {
                        Console.WriteLine("Response: " + Encoding.UTF8.GetString(value));
                        Console.WriteLine();
                    }
                }
            }
        }

        static void ManagerTest3()
        {
            using (var manager = new RedisManager("My Manager", new RedisManagerSettings(
                new[] { RedisEndPoint.SentinelLocalHostEndPoint },
                masterName: "mymaster")))
            {
                var sw = new Stopwatch();
                do
                {
                    try
                    {
                        Console.Clear();

                        var runId = String.Empty;

                        sw.Restart();
                        using (var db = manager.GetDb(true))
                        {
                            Ping(db);
                        }
                    }
                    catch (Exception e)
                    {
                        Console.WriteLine(e);
                    }

                    sw.Stop();

                    Console.WriteLine();
                    Console.WriteLine("Total ticks: " + sw.ElapsedTicks);
                    Console.WriteLine("Total millisecs: " + sw.ElapsedMilliseconds);

                    Console.WriteLine();
                    Console.WriteLine("Press any key to continue, ESC to escape ...");
                }
                while (Console.ReadKey(true).Key != ConsoleKey.Escape);
            }
        }

        static void ManagerTest2()
        {
            using (var manager = new RedisManager("My Manager", new RedisManagerSettings(
                new[] { RedisEndPoint.LocalHostEndPoint, new RedisEndPoint("localhost", 6380) },
                masterName: "mymaster")))
            {
                var sw = new Stopwatch();
                do
                {
                    try
                    {
                        Console.Clear();

                        var runId = String.Empty;

                        sw.Restart();
                        using (var db = manager.GetDb(true))
                        {
                            Ping(db);
                        }
                    }
                    catch (Exception e)
                    {
                        Console.WriteLine(e);
                    }

                    sw.Stop();

                    Console.WriteLine();
                    Console.WriteLine("Total ticks: " + sw.ElapsedTicks);
                    Console.WriteLine("Total millisecs: " + sw.ElapsedMilliseconds);

                    Console.WriteLine();
                    Console.WriteLine("Press any key to continue, ESC to escape ...");
                }
                while (Console.ReadKey(true).Key != ConsoleKey.Escape);
            }
        }

        static void ManagerTest1()
        {
            using (var manager = new RedisManager("My Manager",
                new RedisManagerSettings(new[] { RedisEndPoint.LocalHostEndPoint }, masterName: "mymaster")))
            {
                var sw = new Stopwatch();
                do
                {
                    try
                    {
                        Console.Clear();

                        var runId = String.Empty;

                        sw.Restart();
                        using (var db = manager.GetDb(true))
                        {
                            Ping(db);
                        }
                    }
                    catch (Exception e)
                    {
                        Console.WriteLine(e);
                    }

                    sw.Stop();

                    Console.WriteLine();
                    Console.WriteLine("Total ticks: " + sw.ElapsedTicks);
                    Console.WriteLine("Total millisecs: " + sw.ElapsedMilliseconds);

                    Console.WriteLine();
                    Console.WriteLine("Press any key to continue, ESC to escape ...");
                }
                while (Console.ReadKey(true).Key != ConsoleKey.Escape);
            }
        }

        #endregion Manager

        #region Sentinel

        static void SentinelTest9()
        {
            using (var sentinel = RedisSentinel.NewSentinel(new RedisSentinelSettings("127.0.0.1",
                       RedisConstants.DefaultSentinelPort, masterName: "mymaster")))
            {
                var sw = new Stopwatch();
                do
                {
                    try
                    {
                        Console.Clear();

                        sw.Restart();

                        var result = sentinel.Commands.Monitor("mymaster2", "127.0.0.1", 6379, 2);
                        if (result == null)
                            Console.WriteLine("(nil)");
                        else
                        {
                            Console.WriteLine("Monitor: " + result.Value);
                            if (result)
                            {
                                result = sentinel.Commands.Remove("mymaster2");
                                if (result == null)
                                    Console.WriteLine("(nil)");
                                else
                                    Console.WriteLine("Remove: " + result.Value);
                            }

                            Console.WriteLine();
                        }
                    }
                    catch (Exception e)
                    {
                        Console.WriteLine(e);
                    }

                    Console.WriteLine();
                    Console.WriteLine("Total ticks: " + sw.ElapsedTicks);
                    Console.WriteLine("Total millisecs: " + sw.ElapsedMilliseconds);

                    Console.WriteLine();
                    Console.WriteLine("Press any key to continue, ESC to escape ...");
                }
                while (Console.ReadKey(true).Key != ConsoleKey.Escape);
            }
        }

        static void SentinelTest8()
        {
            using (var sentinel = RedisSentinel.NewSentinel(new RedisSentinelSettings("127.0.0.1",
                       RedisConstants.DefaultSentinelPort, masterName: "mymaster")))
            {
                var sw = new Stopwatch();
                do
                {
                    try
                    {
                        Console.Clear();

                        sw.Restart();

                        var result = sentinel.Commands.Slaves("mymaster");
                        if (result == null)
                            Console.WriteLine("(nil)");
                        else
                        {
                            var slavesValue = result.Value;

                            if (slavesValue == null)
                                Console.WriteLine("(nil)");
                            else if (slavesValue.Length == 0)
                                Console.WriteLine("(empty)");
                            else
                            {
                                foreach (var slave in slavesValue)
                                {
                                    Console.WriteLine("IP address: " + slave.IPAddress);
                                    Console.WriteLine("Port: " + slave.Port);
                                    Console.WriteLine("RunId: " + slave.RunId);
                                    Console.WriteLine("Master Host: " + slave.MasterHost);
                                    Console.WriteLine("Master Link Down Time: " + slave.MasterLinkDownTime);
                                    Console.WriteLine("Master Link Status: " + slave.MasterLinkStatus);
                                    Console.WriteLine("Master Port: " + slave.MasterPort);
                                    Console.WriteLine("Slave Priority: " + slave.SlavePriority);
                                    Console.WriteLine("Slave Repl Offset: " + slave.SlaveReplOffset);
                                    Console.WriteLine("Down After Milliseconds: " + slave.DownAfterMilliseconds);
                                    Console.WriteLine("Flags: " + slave.Flags);
                                    Console.WriteLine("Last OK Ping Reply: " + slave.LastOKPingReply);
                                    Console.WriteLine("Last Ping Reply: " + slave.LastPingReply);
                                    Console.WriteLine("Last Ping Sent: " + slave.LastPingSent);
                                    Console.WriteLine("Name: " + slave.Name);
                                    Console.WriteLine();
                                }
                            }
                        }
                    }
                    catch (Exception e)
                    {
                        Console.WriteLine(e);
                    }

                    Console.WriteLine();
                    Console.WriteLine("Total ticks: " + sw.ElapsedTicks);
                    Console.WriteLine("Total millisecs: " + sw.ElapsedMilliseconds);

                    Console.WriteLine();
                    Console.WriteLine("Press any key to continue, ESC to escape ...");
                }
                while (Console.ReadKey(true).Key != ConsoleKey.Escape);
            }
        }

        static void SentinelTest7()
        {
            using (var sentinel = RedisSentinel.NewSentinel(new RedisSentinelSettings("127.0.0.1",
                       RedisConstants.DefaultSentinelPort, masterName: "mymaster")))
            {
                var sw = new Stopwatch();
                do
                {
                    try
                    {
                        Console.Clear();

                        sw.Restart();

                        var result = sentinel.Commands.Sentinels("mymaster");
                        if (result == null)
                            Console.WriteLine("(nil)");
                        else
                        {
                            var sentinelsInfo = result.Value;

                            if (sentinelsInfo == null)
                                Console.WriteLine("(nil)");
                            else if (sentinelsInfo.Length == 0)
                                Console.WriteLine("(empty)");
                            else
                            {
                                foreach (var sentinelInfo in sentinelsInfo)
                                {
                                    Console.WriteLine("IP address: " + sentinelInfo.IPAddress);
                                    Console.WriteLine("Port: " + sentinelInfo.Port);
                                    Console.WriteLine("RunId: " + sentinelInfo.RunId);
                                    Console.WriteLine("Info Refresh: " + sentinelInfo.InfoRefresh);
                                    Console.WriteLine("Last Hello Message: " + sentinelInfo.LastHelloMessage);
                                    Console.WriteLine("Pending Commands: " + sentinelInfo.PendingCommands);
                                    Console.WriteLine("Voted Leader: " + sentinelInfo.VotedLeader);
                                    Console.WriteLine("Voted Leader Epoch: " + sentinelInfo.VotedLeaderEpoch);
                                    Console.WriteLine("Down After Milliseconds: " + sentinelInfo.DownAfterMilliseconds);
                                    Console.WriteLine("Flags: " + sentinelInfo.Flags);
                                    Console.WriteLine("Last OK Ping Reply: " + sentinelInfo.LastOKPingReply);
                                    Console.WriteLine("Last Ping Reply: " + sentinelInfo.LastPingReply);
                                    Console.WriteLine("Last Ping Sent: " + sentinelInfo.LastPingSent);
                                    Console.WriteLine("Name: " + sentinelInfo.Name);
                                    Console.WriteLine();
                                }
                            }
                        }
                    }
                    catch (Exception e)
                    {
                        Console.WriteLine(e);
                    }

                    Console.WriteLine();
                    Console.WriteLine("Total ticks: " + sw.ElapsedTicks);
                    Console.WriteLine("Total millisecs: " + sw.ElapsedMilliseconds);

                    Console.WriteLine();
                    Console.WriteLine("Press any key to continue, ESC to escape ...");
                }
                while (Console.ReadKey(true).Key != ConsoleKey.Escape);
            }
        }

        static void SentinelTest6()
        {
            using (var sentinel = RedisSentinel.NewSentinel(new RedisSentinelSettings("127.0.0.1",
                       RedisConstants.DefaultSentinelPort, masterName: "mymaster")))
            {
                var sw = new Stopwatch();
                do
                {
                    try
                    {
                        Console.Clear();

                        var runId = String.Empty;

                        sw.Restart();

                        var result = sentinel.Commands.Role();
                        if (result == null)
                            Console.WriteLine("(nil)");
                        else
                        {
                            var role = result.Value;
                            if (role == null)
                                Console.WriteLine("(nil)");
                            else
                            {
                                Console.WriteLine("Name: " + role.RoleName);
                                Console.WriteLine("Role: " + role.Role);
                                Console.WriteLine();
                            }
                        }
                    }
                    catch (Exception e)
                    {
                        Console.WriteLine(e);
                    }

                    Console.WriteLine();
                    Console.WriteLine("Total ticks: " + sw.ElapsedTicks);
                    Console.WriteLine("Total millisecs: " + sw.ElapsedMilliseconds);

                    Console.WriteLine();
                    Console.WriteLine("Press any key to continue, ESC to escape ...");
                }
                while (Console.ReadKey(true).Key != ConsoleKey.Escape);
            }
        }

        static void SentinelTest5()
        {
            using (var sentinel = RedisSentinel.NewSentinel(new RedisSentinelSettings("127.0.0.1",
                       RedisConstants.DefaultSentinelPort, masterName: "mymaster")))
            {
                var sw = new Stopwatch();
                do
                {
                    try
                    {
                        Console.Clear();

                        var runId = String.Empty;

                        sw.Restart();

                        var result = sentinel.Commands.Ping();
                        if (result == null)
                            Console.WriteLine("(nil)");
                        else
                        {
                            Console.WriteLine("Ping: " + result.Value);
                            Console.WriteLine();
                        }
                    }
                    catch (Exception e)
                    {
                        Console.WriteLine(e);
                    }

                    Console.WriteLine();
                    Console.WriteLine("Total ticks: " + sw.ElapsedTicks);
                    Console.WriteLine("Total millisecs: " + sw.ElapsedMilliseconds);

                    Console.WriteLine();
                    Console.WriteLine("Press any key to continue, ESC to escape ...");
                }
                while (Console.ReadKey(true).Key != ConsoleKey.Escape);
            }
        }

        static void SentinelTest4()
        {
            using (var sentinel = RedisSentinel.NewSentinel(new RedisSentinelSettings("127.0.0.1",
                       RedisConstants.DefaultSentinelPort, masterName: "mymaster")))
            {
                var sw = new Stopwatch();
                do
                {
                    try
                    {
                        Console.Clear();

                        var runId = String.Empty;

                        var masterResult = sentinel.Commands.Master("mymaster");
                        if (masterResult != null)
                        {
                            var master = masterResult.Value;
                            if (master != null)
                                runId = master.RunId;
                        }

                        sw.Restart();

                        var result = sentinel.Commands.IsMasterDownByAddr("127.0.0.1", 6379, runId);
                        if (result == null)
                            Console.WriteLine("(nil)");
                        else
                        {
                            var isMasterDownInfo = result.Value;

                            Console.WriteLine("IsDown: " + isMasterDownInfo.IsDown);
                            Console.WriteLine("Leader: " + isMasterDownInfo.LeaderRunId);
                            Console.WriteLine("Leader Epoch: " + isMasterDownInfo.LeaderEpoch);
                            Console.WriteLine();
                        }
                    }
                    catch (Exception e)
                    {
                        Console.WriteLine(e);
                    }

                    Console.WriteLine();
                    Console.WriteLine("Total ticks: " + sw.ElapsedTicks);
                    Console.WriteLine("Total millisecs: " + sw.ElapsedMilliseconds);

                    Console.WriteLine();
                    Console.WriteLine("Press any key to continue, ESC to escape ...");
                }
                while (Console.ReadKey(true).Key != ConsoleKey.Escape);
            }
        }

        static void SentinelTest3()
        {
            using (var sentinel = RedisSentinel.NewSentinel(new RedisSentinelSettings("127.0.0.1",
                       RedisConstants.DefaultSentinelPort, masterName: "mymaster")))
            {
                var sw = new Stopwatch();
                do
                {
                    try
                    {
                        Console.Clear();

                        sw.Restart();

                        var result = sentinel.Commands.GetMasterAddrByName("mymaster");
                        if (result == null)
                            Console.WriteLine("(nil)");
                        else
                        {
                            var endpoint = result.Value;
                            if (endpoint == null)
                                Console.WriteLine("(nil)");
                            else
                            {
                                Console.WriteLine("IP address: " + endpoint.IPAddress);
                                Console.WriteLine("Port: " + endpoint.Port);
                                Console.WriteLine();
                            }
                        }
                    }
                    catch (Exception e)
                    {
                        Console.WriteLine(e);
                    }

                    Console.WriteLine();
                    Console.WriteLine("Total ticks: " + sw.ElapsedTicks);
                    Console.WriteLine("Total millisecs: " + sw.ElapsedMilliseconds);

                    Console.WriteLine();
                    Console.WriteLine("Press any key to continue, ESC to escape ...");
                }
                while (Console.ReadKey(true).Key != ConsoleKey.Escape);
            }
        }

        static void SentinelTest2()
        {
            using (var sentinel = RedisSentinel.NewSentinel(new RedisSentinelSettings("127.0.0.1",
                       RedisConstants.DefaultSentinelPort, masterName: "mymaster")))
            {
                var sw = new Stopwatch();
                do
                {
                    try
                    {
                        Console.Clear();

                        sw.Restart();

                        var result = sentinel.Commands.Masters();
                        if (result == null)
                            Console.WriteLine("(nil)");
                        else
                        {
                            var mastersValue = result.Value;

                            if (mastersValue == null)
                                Console.WriteLine("(nil)");
                            else if (mastersValue.Length == 0)
                                Console.WriteLine("(empty)");
                            else
                            {
                                foreach (var master in mastersValue)
                                {
                                    Console.WriteLine("IP address: " + master.IPAddress);
                                    Console.WriteLine("Port: " + master.Port);
                                    Console.WriteLine("RunId: " + master.RunId);
                                    Console.WriteLine("Config epoch: " + master.ConfigEpoch);
                                    Console.WriteLine("Down after milliseconds: " + master.DownAfterMilliseconds);
                                    Console.WriteLine("Failover timeout: " + master.FailoverTimeOut);
                                    Console.WriteLine("Flags: " + master.Flags);
                                    Console.WriteLine("Last OK ping reply: " + master.LastOKPingReply);
                                    Console.WriteLine("Last ping reply: " + master.LastPingReply);
                                    Console.WriteLine("Last ping sent: " + master.LastPingSent);
                                    Console.WriteLine("Name: " + master.Name);
                                    Console.WriteLine("Number of other sentinels: " + master.NumberOfOtherSentinels);
                                    Console.WriteLine("Number of slaves: " + master.NumberOfSlaves);
                                    Console.WriteLine("Parallel syncs: " + master.ParallelSyncs);
                                    Console.WriteLine("Quorum: " + master.Quorum);
                                    Console.WriteLine();
                                }
                            }
                        }
                    }
                    catch (Exception e)
                    {
                        Console.WriteLine(e);
                    }

                    Console.WriteLine();
                    Console.WriteLine("Total ticks: " + sw.ElapsedTicks);
                    Console.WriteLine("Total millisecs: " + sw.ElapsedMilliseconds);

                    Console.WriteLine();
                    Console.WriteLine("Press any key to continue, ESC to escape ...");
                }
                while (Console.ReadKey(true).Key != ConsoleKey.Escape);
            }
        }

        static void SentinelTest1()
        {
            using (var sentinel = RedisSentinel.NewSentinel(new RedisSentinelSettings("localhost",
                       RedisConstants.DefaultSentinelPort, masterName: "mymaster")))
            {
                var sw = new Stopwatch();
                do
                {
                    try
                    {
                        Console.Clear();

                        sw.Restart();

                        var result = sentinel.Commands.Master("mymaster");
                        if (result == null)
                            Console.WriteLine("(nil)");
                        else
                        {
                            var master = result.Value;
                            if (master == null)
                                Console.WriteLine("(nil)");
                            else
                            {
                                Console.WriteLine("IP address: " + master.IPAddress);
                                Console.WriteLine("Port: " + master.Port);
                                Console.WriteLine("RunId: " + master.RunId);
                                Console.WriteLine("Config epoch: " + master.ConfigEpoch);
                                Console.WriteLine("Down after milliseconds: " + master.DownAfterMilliseconds);
                                Console.WriteLine("Failover timeout: " + master.FailoverTimeOut);
                                Console.WriteLine("Flags: " + master.Flags);
                                Console.WriteLine("Last OK ping reply: " + master.LastOKPingReply);
                                Console.WriteLine("Last ping reply: " + master.LastPingReply);
                                Console.WriteLine("Last ping sent: " + master.LastPingSent);
                                Console.WriteLine("Name: " + master.Name);
                                Console.WriteLine("Number of other sentinels: " + master.NumberOfOtherSentinels);
                                Console.WriteLine("Number of slaves: " + master.NumberOfSlaves);
                                Console.WriteLine("Parallel syncs: " + master.ParallelSyncs);
                                Console.WriteLine("Quorum: " + master.Quorum);
                                Console.WriteLine();
                            }
                        }
                    }
                    catch (Exception e)
                    {
                        Console.WriteLine(e);
                    }

                    Console.WriteLine();
                    Console.WriteLine("Total ticks: " + sw.ElapsedTicks);
                    Console.WriteLine("Total millisecs: " + sw.ElapsedMilliseconds);

                    Console.WriteLine();
                    Console.WriteLine("Press any key to continue, ESC to escape ...");
                }
                while (Console.ReadKey(true).Key != ConsoleKey.Escape);
            }
        }

        #endregion Sentinel

        #region Pipeline

        static void PipelineTest5()
        {
            using (var pool = new RedisConnectionPool("My redis pool",
                    new RedisPoolSettings("127.0.0.1", 6379, maxConnectionCount: 1)))
            {
                using (var db = pool.GetDb())
                {
                    for (var i = 0; i < 5000; i++)
                        db.Strings.Set("abc_" + i, i);
                }

                using (var pipeline = pool.CreatePipeline())
                {
                    var sw = new Stopwatch();
                    do
                    {
                        try
                        {
                            Console.Clear();

                            sw.Restart();

                            var list = new List<RedisResult>();
                            for (var i = 0; i < 5000; i++)
                            {
                                var result = pipeline.Strings.Get("abc_" + i);
                                list.Add(result);
                            }

                            pipeline.Execute();

                            sw.Stop();

                            var sBuilder = new StringBuilder(20 * 1024);
                            for (var i = 0; i < list.Count; i++)
                            {
                                sBuilder.Append(list[i]);
                                sBuilder.AppendLine();
                            }

                            Console.WriteLine(sBuilder.ToString());
                        }
                        catch (Exception e)
                        {
                            Console.WriteLine(e);
                        }

                        Console.WriteLine();
                        Console.WriteLine("Total ticks: " + sw.ElapsedTicks);
                        Console.WriteLine("Total millisecs: " + sw.ElapsedMilliseconds);

                        Console.WriteLine();
                        Console.WriteLine("Press any key to continue, ESC to escape ...");
                    }
                    while (Console.ReadKey(true).Key != ConsoleKey.Escape);
                }
            }
        }

        static void PipelineTest4()
        {
            using (var pool = new RedisConnectionPool("My redis pool",
                    new RedisPoolSettings("127.0.0.1", 6379, maxConnectionCount: 1)))
            {
                using (var pipeline = pool.CreatePipeline())
                {
                    var sw = new Stopwatch();
                    do
                    {
                        try
                        {
                            Console.Clear();

                            sw.Restart();

                            var list = new List<RedisResult>();
                            for (var i = 0; i < 100; i++)
                            {
                                pipeline.Strings.Set("abc_" + i, i);
                                var result = pipeline.Strings.Get("abc_" + i);

                                list.Add(result);
                            }

                            pipeline.Execute();

                            sw.Stop();

                            sw.Stop();

                            for (var i = 0; i < list.Count; i++)
                                Console.WriteLine(list[i]);
                        }
                        catch (Exception e)
                        {
                            Console.WriteLine(e);
                        }

                        Console.WriteLine();
                        Console.WriteLine("Total ticks: " + sw.ElapsedTicks);
                        Console.WriteLine("Total millisecs: " + sw.ElapsedMilliseconds);

                        Console.WriteLine();
                        Console.WriteLine("Press any key to continue, ESC to escape ...");
                    }
                    while (Console.ReadKey(true).Key != ConsoleKey.Escape);
                }
            }
        }

        static void PipelineTest3()
        {
            using (var pool = new RedisConnectionPool("My redis pool",
                    new RedisPoolSettings("127.0.0.1", 6379, maxConnectionCount: 1)))
            {
                using (var pipeline = pool.CreatePipeline())
                {
                    var i = 0;
                    do
                    {
                        try
                        {
                            Console.Clear();

                            i++;
                            pipeline.Strings.Set("abc_" + i, i);
                            var result = pipeline.Strings.Get("abc_" + i);

                            pipeline.Execute();

                            string str = result;
                            Console.WriteLine(str);
                        }
                        catch (Exception e)
                        {
                            Console.WriteLine(e);
                        }

                        Console.WriteLine();
                        Console.WriteLine("Press any key to continue, ESC to escape ...");
                    }
                    while (Console.ReadKey(true).Key != ConsoleKey.Escape);
                }
            }
        }

        static void PipelineTest2()
        {
            using (var pool = new RedisConnectionPool("My redis pool",
                    new RedisPoolSettings("127.0.0.1", 6379, maxConnectionCount: 1)))
            {
                using (var pipeline = pool.CreatePipeline())
                {
                    var sw = new Stopwatch();
                    do
                    {
                        try
                        {
                            Console.Clear();

                            sw.Restart();

                            var list = new List<RedisResult>();
                            for (var i = 0; i < 100; i++)
                            {
                                var result = pipeline.Connection.Ping(i + 1);
                                list.Add(result);
                            }

                            pipeline.Execute();

                            sw.Stop();

                            for (var i = 0; i < list.Count; i++)
                                Console.WriteLine(list[i]);
                        }
                        catch (Exception e)
                        {
                            Console.WriteLine(e);
                        }

                        Console.WriteLine();
                        Console.WriteLine("Total ticks: " + sw.ElapsedTicks);
                        Console.WriteLine("Total millisecs: " + sw.ElapsedMilliseconds);

                        Console.WriteLine();
                        Console.WriteLine("Press any key to continue, ESC to escape ...");
                    }
                    while (Console.ReadKey(true).Key != ConsoleKey.Escape);
                }
            }
        }

        static void PipelineTest1()
        {
            using (var pool = new RedisConnectionPool("My redis pool",
                    new RedisPoolSettings("127.0.0.1", 6379, maxConnectionCount: 1)))
            {
                using (var pipeline = pool.CreatePipeline())
                {
                    var i = 0;
                    do
                    {
                        try
                        {
                            Console.Clear();

                            var result = pipeline.Connection.Ping(++i);

                            pipeline.Execute();

                            Console.WriteLine(result);
                        }
                        catch (Exception e)
                        {
                            Console.WriteLine(e);
                        }

                        Console.WriteLine();
                        Console.WriteLine("Press any key to continue, ESC to escape ...");
                    }
                    while (Console.ReadKey(true).Key != ConsoleKey.Escape);
                }
            }
        }

        #endregion Pipeline

        #region Transaction

        static void TransactionTest5()
        {
            using (var pool = new RedisConnectionPool("My redis pool",
                    new RedisPoolSettings("127.0.0.1", 6379, maxConnectionCount: 1)))
            {
                using (var db = pool.GetDb())
                {
                    for (var i = 0; i < 5000; i++)
                        db.Strings.Set("abc_" + i, i);
                }

                using (var transaction = pool.BeginTransaction())
                {
                    var sw = new Stopwatch();
                    do
                    {
                        try
                        {
                            Console.Clear();

                            transaction.Watch("foo", "boo");

                            sw.Restart();

                            var list = new List<RedisResult>();
                            for (var i = 0; i < 5000; i++)
                            {
                                var result = transaction.Strings.Get("abc_" + i);
                                list.Add(result);
                            }

                            transaction.Commit();

                            sw.Stop();

                            sw.Stop();

                            for (var i = 0; i < list.Count; i++)
                                Console.WriteLine(list[i]);
                        }
                        catch (Exception e)
                        {
                            Console.WriteLine(e);
                        }

                        Console.WriteLine();
                        Console.WriteLine("Total ticks: " + sw.ElapsedTicks);
                        Console.WriteLine("Total millisecs: " + sw.ElapsedMilliseconds);

                        Console.WriteLine();
                        Console.WriteLine("Press any key to continue, ESC to escape ...");
                    }
                    while (Console.ReadKey(true).Key != ConsoleKey.Escape);
                }
            }
        }

        static void TransactionTest4()
        {
            using (var pool = new RedisConnectionPool("My redis pool",
                    new RedisPoolSettings("127.0.0.1", 6379, maxConnectionCount: 1)))
            {
                using (var transaction = pool.BeginTransaction())
                {
                    var sw = new Stopwatch();
                    do
                    {
                        try
                        {
                            Console.Clear();

                            sw.Restart();

                            var list = new List<RedisResult>();
                            for (var i = 0; i < 100; i++)
                            {
                                transaction.Strings.Set("abc_" + i, i);
                                var result = transaction.Strings.Get("abc_" + i);

                                list.Add(result);
                            }

                            transaction.Commit();

                            sw.Stop();

                            sw.Stop();

                            for (var i = 0; i < list.Count; i++)
                                Console.WriteLine(list[i]);
                        }
                        catch (Exception e)
                        {
                            Console.WriteLine(e);
                        }

                        Console.WriteLine();
                        Console.WriteLine("Total ticks: " + sw.ElapsedTicks);
                        Console.WriteLine("Total millisecs: " + sw.ElapsedMilliseconds);

                        Console.WriteLine();
                        Console.WriteLine("Press any key to continue, ESC to escape ...");
                    }
                    while (Console.ReadKey(true).Key != ConsoleKey.Escape);
                }
            }
        }

        static void TransactionTest3()
        {
            using (var pool = new RedisConnectionPool("My redis pool",
                    new RedisPoolSettings("127.0.0.1", 6379, maxConnectionCount: 1)))
            {
                using (var transaction = pool.BeginTransaction())
                {
                    var i = 0;
                    do
                    {
                        try
                        {
                            Console.Clear();

                            i++;
                            transaction.Strings.Set("abc_" + i, i);
                            var result = transaction.Strings.Get("abc_" + i);

                            transaction.Commit();

                            string str = result;
                            Console.WriteLine(str);
                        }
                        catch (Exception e)
                        {
                            Console.WriteLine(e);
                        }

                        Console.WriteLine();
                        Console.WriteLine("Press any key to continue, ESC to escape ...");
                    }
                    while (Console.ReadKey(true).Key != ConsoleKey.Escape);
                }
            }
        }

        static void TransactionTest2()
        {
            using (var pool = new RedisConnectionPool("My redis pool",
                    new RedisPoolSettings("127.0.0.1", 6379, maxConnectionCount: 1)))
            {
                using (var transaction = pool.BeginTransaction())
                {
                    var sw = new Stopwatch();
                    do
                    {
                        try
                        {
                            Console.Clear();

                            sw.Restart();

                            var list = new List<RedisResult>();
                            for (var i = 0; i < 100; i++)
                            {
                                var result = transaction.Connection.Ping(i + 1);
                                list.Add(result);
                            }

                            transaction.Commit();

                            sw.Stop();

                            for (var i = 0; i < list.Count; i++)
                                Console.WriteLine(list[i]);
                        }
                        catch (Exception e)
                        {
                            Console.WriteLine(e);
                        }

                        Console.WriteLine();
                        Console.WriteLine("Total ticks: " + sw.ElapsedTicks);
                        Console.WriteLine("Total millisecs: " + sw.ElapsedMilliseconds);

                        Console.WriteLine();
                        Console.WriteLine("Press any key to continue, ESC to escape ...");
                    }
                    while (Console.ReadKey(true).Key != ConsoleKey.Escape);
                }
            }
        }

        static void TransactionTest1()
        {
            using (var pool = new RedisConnectionPool("My redis pool",
                    new RedisPoolSettings("127.0.0.1", 6379, maxConnectionCount: 1)))
            {
                using (var transaction = pool.BeginTransaction())
                {
                    var i = 0;
                    do
                    {
                        try
                        {
                            Console.Clear();

                            var result = transaction.Connection.Ping(++i);

                            transaction.Commit();

                            Console.WriteLine(result);
                        }
                        catch (Exception e)
                        {
                            Console.WriteLine(e);
                        }

                        Console.WriteLine();
                        Console.WriteLine("Press any key to continue, ESC to escape ...");
                    }
                    while (Console.ReadKey(true).Key != ConsoleKey.Escape);
                }
            }
        }

        #endregion Transaction

        #region Shutdown

        static void ShutdownTest1()
        {
            using (var pool = new RedisConnectionPool("My redis pool",
                    new RedisPoolSettings("127.0.0.1", 6379, maxConnectionCount: 1)))
            {
                using (var admin = pool.GetAdmin())
                {
                    do
                    {
                        try
                        {
                            Console.Clear();
                            admin.Commands.ShutDown();
                        }
                        catch (Exception e)
                        {
                            Console.WriteLine(e);
                        }

                        Console.WriteLine();
                        Console.WriteLine("Press any key to continue, ESC to escape ...");
                    }
                    while (Console.ReadKey(true).Key != ConsoleKey.Escape);
                }
            }
        }

        #endregion Shutdown

        #region Info

        static void InfoTest1()
        {
            using (var pool = new RedisConnectionPool("My redis pool",
                    new RedisPoolSettings("127.0.0.1", 6379, maxConnectionCount: 1)))
            {
                using (var admin = pool.GetAdmin())
                {
                    do
                    {
                        try
                        {
                            Console.Clear();

                            var infoResult = admin.Commands.Info();

                            if (infoResult == null || infoResult.Value == null)
                                Console.WriteLine("(nil)");
                            else
                            {
                                var info = infoResult.Value;

                                foreach (var kv1 in info)
                                {
                                    Console.WriteLine("# " + kv1.Key);

                                    var section = kv1.Value;
                                    if (section != null)
                                    {
                                        foreach (var kv2 in section)
                                            Console.WriteLine(kv2.Key + ":" + kv2.Value);
                                    }

                                    Console.WriteLine();
                                }
                            }
                        }
                        catch (Exception e)
                        {
                            Console.WriteLine(e);
                        }

                        Console.WriteLine();
                        Console.WriteLine("Press any key to continue, ESC to escape ...");
                    }
                    while (Console.ReadKey(true).Key != ConsoleKey.Escape);
                }
            }
        }

        #endregion Info

        #region SlowLog

        static void SlowLogTest1()
        {
            using (var pool = new RedisConnectionPool("My redis pool",
                    new RedisPoolSettings("127.0.0.1", 6379, maxConnectionCount: 1)))
            {
                using (var admin = pool.GetAdmin())
                {
                    do
                    {
                        try
                        {
                            Console.Clear();

                            var lenResult = admin.Commands.SlowLogLen();

                            Console.WriteLine("SLOWLOG LEN");
                            Console.WriteLine(lenResult);

                            if (lenResult > 0)
                            {
                                var getResult = admin.Commands.SlowLogGet(lenResult);

                                Console.WriteLine("SLOWLOG GET " + lenResult);
                                Console.WriteLine(getResult);
                            }
                        }
                        catch (Exception e)
                        {
                            Console.WriteLine(e);
                        }

                        Console.WriteLine();
                        Console.WriteLine("Press any key to continue, ESC to escape ...");
                    }
                    while (Console.ReadKey(true).Key != ConsoleKey.Escape);
                }
            }
        }

        #endregion SlowLog

        #region Geo

        static void GeoTest1()
        {
            using (var pool = new RedisConnectionPool("My redis pool",
                    new RedisPoolSettings("127.0.0.1", 6379, maxConnectionCount: 1)))
            {
                using (var db = pool.GetDb())
                {
                    do
                    {
                        try
                        {
                            Console.Clear();

                            var addResult = db.Geo.GeoAdd("Sicily",
                                          new RedisGeospatialItem(13.361389, 38.115556, "Palermo"),
                                          new RedisGeospatialItem(15.087269, 37.502669, "Catania"));

                            Console.WriteLine("GEOADD Sicily 13.361389 38.115556 \"Palermo\" 15.087269 37.502669 \"Catania\"");
                            Console.WriteLine(addResult);

                            var distResult = db.Geo.GeoDistance("Sicily", "Palermo", "Catania");
                            Console.WriteLine("GEODIST Sicily Palermo Catania");
                            Console.WriteLine(distResult);

                            distResult = db.Geo.GeoDistance("Sicily", "Palermo", "Catania", RedisGeoDistanceUnit.Kilometers);
                            Console.WriteLine("GEODIST Sicily Palermo Catania km");
                            Console.WriteLine(distResult);

                            distResult = db.Geo.GeoDistance("Sicily", "Palermo", "Catania", RedisGeoDistanceUnit.Miles);
                            Console.WriteLine("GEODIST Sicily Palermo Catania mi");
                            Console.WriteLine(distResult);

                            distResult = db.Geo.GeoDistance("Sicily", "Foo", "Bar");
                            Console.WriteLine("GEODIST Sicily Foo Bar");
                            Console.WriteLine(distResult);

                            var hashResult = db.Geo.GeoHash("Sicily", "Palermo", "Catania");
                            Console.WriteLine("GEOHASH Sicily Palermo Catania");
                            Console.WriteLine(hashResult);

                            var posResult = db.Geo.GeoPosition("Sicily", "Palermo", "Catania", "NonExisting");
                            Console.WriteLine("GEOPOS Sicily Palermo Catania NonExisting");
                            Console.WriteLine(posResult);

                            var radiusResult = db.Geo.GeoRadius("Sicily", new RedisGeoPosition(15, 37),
                                                                200, RedisGeoDistanceUnit.Kilometers);
                            Console.WriteLine("GEORADIUS Sicily 15 37 200 km");
                            Console.WriteLine(radiusResult);

                            radiusResult = db.Geo.GeoRadius("Sicily", new RedisGeoPosition(15, 37),
                                                                200, RedisGeoDistanceUnit.Kilometers, withDist: true);
                            Console.WriteLine("GEORADIUS Sicily 15 37 200 km WITHDIST");
                            Console.WriteLine(radiusResult);

                            radiusResult = db.Geo.GeoRadius("Sicily", new RedisGeoPosition(15, 37),
                                                                200, RedisGeoDistanceUnit.Kilometers, withCoord: true);
                            Console.WriteLine("GEORADIUS Sicily 15 37 200 km WITHCOORD");
                            Console.WriteLine(radiusResult);

                            radiusResult = db.Geo.GeoRadius("Sicily", new RedisGeoPosition(15, 37),
                                        200, RedisGeoDistanceUnit.Kilometers, true, true);
                            Console.WriteLine("GEORADIUS Sicily 15 37 200 km WITHCOORD WITHDIST");
                            Console.WriteLine(radiusResult);

                            radiusResult = db.Geo.GeoRadius("Sicily", new RedisGeoPosition(15, 37),
                                                            200, RedisGeoDistanceUnit.Kilometers, true, true, true);
                            Console.WriteLine("GEORADIUS Sicily 15 37 200 km WITHCOORD WITHDIST WITHHASH");
                            Console.WriteLine(radiusResult);
                        }
                        catch (Exception e)
                        {
                            Console.WriteLine(e);
                        }

                        Console.WriteLine();
                        Console.WriteLine("Press any key to continue, ESC to escape ...");
                    }
                    while (Console.ReadKey(true).Key != ConsoleKey.Escape);
                }
            }
        }

        #endregion Geo

        #region Multi-Threading

        static void MultiThreading8()
        {
            var mediumText = new string('x', 5000);
            var mediumBytes = Encoding.UTF8.GetBytes(mediumText);

            MultiThreadingBase(12, 2, 10, 500, "medium_text", mediumText, true, 5,
                (rdb, dbIndex) =>
                {
                    var result = rdb.Strings.Get("medium_text");
                    return new RedisBytes(mediumBytes);
                }, true);
        }

        static void MultiThreading7b()
        {
            MultiThreadingBase(12, 10, 10, 500, "medium_text", new string('x', 5000), true, 5,
                               (rdb, dbIndex) => { return rdb.Strings.Get("medium_text"); });
        }

        static void MultiThreading7a()
        {
            MultiThreadingBase(12, 2, 1, 5000, "medium_text", new string('x', 5000), true, 5,
                               (rdb, dbIndex) => { return rdb.Strings.Get("medium_text"); });
        }

        static void MultiThreading6()
        {
            MultiThreadingBase(12, 1, 50, 100, "tiny_text", new string('x', 20), false, 5,
                               (rdb, dbIndex) => { return rdb.Strings.Get("tiny_text"); });
        }

        static void MultiThreading5()
        {
            MultiThreadingBase(12, 1, 50, 100, "small_text", new string('x', 1000), false, 5,
                               (rdb, dbIndex) => { return rdb.Strings.Get("small_text"); });
        }

        static void MultiThreading4()
        {
            MultiThreadingBase(12, 1, 50, 100, "small_text", new string('x', 1000), true, 5,
                               (rdb, dbIndex) => { return rdb.Strings.Get("small_text"); });
        }

        static void MultiThreading3()
        {
            MultiThreadingBase(12, 10, 50, 100, "small_text", new string('x', 1000), true, 5,
                               (rdb, dbIndex) => { return rdb.Strings.Get("small_text"); });
        }

        static void MultiThreading2a()
        {
            MultiThreadingBase(12, 10, 50, 100, "large_text", new string('x', 100000), false, 5,
                               (rdb, dbIndex) => { return rdb.Strings.Get("large_text"); });
        }

        static void MultiThreading2b()
        {
            MultiThreadingBase(12, 1, 50, 100, "large_text", new string('x', 100000), false, 5,
                               (rdb, dbIndex) => { return rdb.Strings.Get("large_text"); });
        }

        static void MultiThreading1()
        {
            var tinyText = new string('x', 10);

            MultiThreadingBase(12, 1, 50, 100, "tiny_text", tinyText, true, 5,
                               (rdb, dbIndex) => { return new RedisBytes(Encoding.UTF8.GetBytes(rdb.Connection.Ping(tinyText).Value)); });
        }

        static void MultiThreadingBase(int dbIndex, int maxCount, int threadCount, int loopCount,
                                       string testKey, string testText, bool requireKeyPress,
                                       int sleepSecs, Func<IRedisDb, int, RedisResult> proc, bool transactional = false)
        {
            using (var pool = new RedisConnectionPool("My redis pool",
                     // new RedisSettings(host: "172.28.10.233", port: 6381, maxCount: maxCount))) // DEV
                     new RedisPoolSettings("127.0.0.1", 6379, maxConnectionCount: maxCount, useAsyncCompleter: false))) // LOCAL
            {
                using (var db = transactional ? pool.BeginTransaction(dbIndex) : pool.GetDb(dbIndex))
                {
                    var b = db.Strings.Set(testKey, testText);
                    if (!transactional && !b)
                        throw new Exception("can not set");

                    var g = db.Strings.Get(testKey);
                    if (!transactional && (g == (byte[])null || g.Value == null || g.Value.Length != (testText ?? "").Length))
                        throw new Exception("can not get");

                    if (transactional)
                    {
                        if (!((IRedisTransaction)db).Commit())
                            throw new Exception("can not execute");

                        if (!b)
                            throw new Exception("can not set");

                        if (g == (byte[])null || g.Value == null || g.Value.Length != (testText ?? "").Length)
                            throw new Exception("can not get");
                    }
                }

                var loopIndex = 0;
                List<Thread> thList = null;

                var totalSw = new Stopwatch();

                do
                {
                    totalSw.Reset();

                    var ticks = 0L;
                    Console.Clear();

                    var oldThList = thList;
                    if (oldThList != null)
                    {
                        for (var i = 0; i < oldThList.Count; i++)
                        {
                            var th = oldThList[i];
                            try
                            {
                                if (th.IsAlive)
                                    th.Interrupt();
                            }
                            catch (Exception)
                            { }
                        }
                    }

                    thList = new List<Thread>(threadCount);
                    try
                    {
                        loopIndex = 0;
                        var ticksLock = new object();

                        for (var i = 0; i < threadCount; i++)
                        {
                            var th = new Thread((obj) =>
                            {
                                var tupple = (Tuple<Thread, AutoResetEvent>)obj;

                                var autoReset = tupple.Item2;
                                using (var rdb = transactional ? pool.BeginTransaction(dbIndex) : pool.GetDb(dbIndex))
                                {
                                    var results = new List<Tuple<string, long, RedisResult>>();
                                    try
                                    {
                                        var @this = tupple.Item1;

                                        var sw = new Stopwatch();

                                        for (var j = 0; j < loopCount; j++)
                                        {
                                            sw.Restart();
                                            var data = proc(rdb, dbIndex);
                                            sw.Stop();

                                            if (transactional)
                                                results.Add(new Tuple<string, long, RedisResult>(@this.Name, sw.ElapsedMilliseconds, data));
                                            else
                                                Console.WriteLine(@this.Name + ": Processed, " + sw.ElapsedMilliseconds.ToString("D3") + " msec, " +
                                                    (data != null && data.Length == (testText ?? "").Length ? "OK" : "FAILED"));

                                            lock (ticksLock)
                                            {
                                                ticks += sw.ElapsedTicks;
                                            }
                                        }
                                    }
                                    finally
                                    {
                                        if (transactional)
                                        {
                                            var sw = new Stopwatch();

                                            sw.Restart();
                                            var execResult = ((IRedisTransaction)rdb).Commit();
                                            sw.Stop();

                                            ticks += sw.ElapsedTicks;

                                            if (execResult)
                                            {
                                                for (var j = 0; j < results.Count; j++)
                                                {
                                                    var tuple = results[i];
                                                    var data = tuple.Item3;

                                                    Console.WriteLine(tuple.Item1 + ": Processed, " + tuple.Item2.ToString("D3") + " msec, " +
                                                        (data != null && data.Length == (testText ?? "").Length ? "OK" : "FAILED"));
                                                }
                                            }
                                        }
                                        autoReset.Set();
                                    }
                                }
                            });

                            th.Name = loopIndex++.ToString("D2") + "." + i.ToString("D2");
                            th.IsBackground = true;
                            thList.Add(th);
                        }

                        totalSw.Restart();

                        var autoResets = new AutoResetEvent[thList.Count];
                        try
                        {
                            for (var i = 0; i < thList.Count; i++)
                            {
                                var th = thList[i];

                                var autoReset = new AutoResetEvent(false);
                                autoResets[i] = autoReset;

                                th.Start(new Tuple<Thread, AutoResetEvent>(th, autoReset));
                            }
                        }
                        finally
                        {
                            WaitHandle.WaitAll(autoResets);
                            totalSw.Stop();
                        }
                    }
                    catch (Exception e)
                    {
                        Console.WriteLine(e);
                    }

                    Console.WriteLine();

                    Console.WriteLine("Sum of inner ticks: " + ticks);
                    Console.WriteLine("Total ticks:        " + totalSw.ElapsedTicks);

                    if (requireKeyPress)
                        Console.WriteLine("Press any key to continue, ESC to escape ...");
                    else
                        Thread.Sleep(sleepSecs * 1000);
                }
                while (!requireKeyPress || Console.ReadKey(true).Key != ConsoleKey.Escape);
            }
        }

        #endregion Multi-Threading

        #region Monitor Tests

        static void MonitorTest3()
        {
            using (var pool = new RedisConnectionPool("My redis pool",
                    new RedisPoolSettings("127.0.0.1", 6379, maxConnectionCount: 1)))
            {
                pool.MonitorChannel.Subscribe((m) =>
                {
                    Console.WriteLine("Time: " + m.Time);
                    Console.WriteLine("Client: " + m.ClientInfo);

                    if (m.Data != null)
                        Console.WriteLine("Received data: " + Encoding.UTF8.GetString((byte[])m.Data));
                    else
                        Console.WriteLine("Received data: ?");

                    Console.WriteLine();
                    Console.WriteLine("Press any key to escape ...");
                });

                Thread.Sleep(1000);

                using (var db = pool.GetDb())
                {
                    for (var i = 0; i < 5; i++)
                        db.Connection.Ping("1");
                }

                Thread.Sleep(1000);

                pool.MonitorChannel.Quit();
                Thread.Sleep(1000);

                using (var db = pool.GetDb())
                {
                    for (var i = 0; i < 5; i++)
                        db.Connection.Ping("2");
                }

                Thread.Sleep(5000);
                pool.MonitorChannel.Monitor();
                Thread.Sleep(1000);

                using (var db = pool.GetDb())
                {
                    for (var i = 0; i < 5; i++)
                        db.Connection.Ping("3");
                }

                Console.WriteLine();
                Console.WriteLine("Press any key to escape ...");

                Console.ReadKey();
            }
        }

        static void MonitorTest2()
        {
            using (var pool = new RedisConnectionPool("My redis pool",
                    new RedisPoolSettings("127.0.0.1", 6379, maxConnectionCount: 1)))
            {
                pool.MonitorChannel.Subscribe((m) =>
                {
                    Console.WriteLine("Time: " + m.Time);
                    Console.WriteLine("Client: " + m.ClientInfo);

                    if (m.Data != null)
                        Console.WriteLine("Received data: " + Encoding.UTF8.GetString((byte[])m.Data));
                    else
                        Console.WriteLine("Received data: ?");

                    Console.WriteLine();
                    Console.WriteLine("Press any key to escape ...");
                });

                Thread.Sleep(1000);

                using (var db = pool.GetDb())
                {
                    for (var i = 0; i < 5; i++)
                        db.Connection.Ping();
                }

                Thread.Sleep(10000);

                pool.MonitorChannel.Quit();
                Thread.Sleep(1000);

                using (var db = pool.GetDb())
                {
                    for (var i = 0; i < 5; i++)
                        db.Connection.Ping();
                }

                Console.WriteLine();
                Console.WriteLine("Press any key to escape ...");

                Console.ReadKey();
            }
        }

        static void MonitorTest1()
        {
            var largeText = Encoding.UTF8.GetBytes(new string('x', 100000));

            using (var pool = new RedisConnectionPool("My redis pool",
                    new RedisPoolSettings("127.0.0.1", 6379, maxConnectionCount: 1)))
            {
                pool.MonitorChannel.Subscribe((m) =>
                {
                    Console.WriteLine("Time: " + m.Time);
                    Console.WriteLine("Client: " + m.ClientInfo);

                    if (m.Data != null)
                        Console.WriteLine("Received data: " + Encoding.UTF8.GetString((byte[])m.Data));
                    else
                        Console.WriteLine("Received data: ?");

                    Console.WriteLine();
                    Console.WriteLine("Press any key to escape ...");
                });

                Console.WriteLine();
                Console.WriteLine("Press any key to escape ...");

                Console.ReadKey();
            }
        }

        #endregion Monitor Tests

        #region PubSub Tests

        static void PubSubTest12()
        {
            var largeText = Encoding.UTF8.GetBytes(new string('x', 100000));

            using (var pool = new RedisConnectionPool("My redis pool",
                    new RedisPoolSettings("127.0.0.1", 6379, maxConnectionCount: 1)))
            {
                pool.PubSubChannel.PSubscribe((m) =>
                {
                    Console.WriteLine("Channel: " + m.Channel);
                    Console.WriteLine("Pattern: " + m.Pattern);

                    if (m.Data != null)
                        Console.WriteLine("Received data: " + Encoding.UTF8.GetString((byte[])m.Data));
                    else
                        Console.WriteLine("Received data: ?");

                    Console.WriteLine();
                    Console.WriteLine("Press any key to escape ...");
                }, "ab*", "xyz");

                Thread.Sleep(1000);

                using (var db = pool.GetDb())
                {
                    for (var i = 0; i < 1000; i++)
                        db.PubSubs.Publish("abc", largeText);
                }

                Console.WriteLine();
                Console.WriteLine("Press any key to escape ...");

                Console.ReadKey();
            }
        }

        static void PubSubTest11()
        {
            var smallText = Encoding.UTF8.GetBytes(new string('x', 1000));

            using (var pool = new RedisConnectionPool("My redis pool",
                    new RedisPoolSettings("127.0.0.1", 6379, maxConnectionCount: 1)))
            {
                pool.PubSubChannel.PSubscribe((m) =>
                {
                    Console.WriteLine("Channel: " + m.Channel);
                    Console.WriteLine("Pattern: " + m.Pattern);

                    if (m.Data != null)
                        Console.WriteLine("Received data: " + Encoding.UTF8.GetString((byte[])m.Data));
                    else
                        Console.WriteLine("Received data: ?");

                    Console.WriteLine();
                    Console.WriteLine("Press any key to escape ...");
                }, "ab*", "xyz");

                Thread.Sleep(1000);

                using (var db = pool.GetDb())
                {
                    for (var i = 0; i < 1000; i++)
                        db.PubSubs.Publish("abc", smallText);
                }

                Console.WriteLine();
                Console.WriteLine("Press any key to escape ...");

                Console.ReadKey();
            }
        }

        static void PubSubTest10()
        {
            using (var pool = new RedisConnectionPool("My redis pool",
                    new RedisPoolSettings("127.0.0.1", 6379, maxConnectionCount: 1)))
            {
                pool.PubSubChannel.PSubscribe((m) =>
                {
                    Console.WriteLine("Channel: " + m.Channel);
                    Console.WriteLine("Pattern: " + m.Pattern);

                    if (m.Data != null)
                        Console.WriteLine("Received data: " + Encoding.UTF8.GetString((byte[])m.Data));
                    else
                        Console.WriteLine("Received data: ?");

                    Console.WriteLine();
                    Console.WriteLine("Press any key to escape ...");
                }, "ab*", "xyz");

                Thread.Sleep(5000);
                pool.PubSubChannel.PUnsubscribe();

                Console.WriteLine();
                Console.WriteLine("Press any key to escape ...");

                Console.ReadKey();
            }
        }

        static void PubSubTest9()
        {
            using (var pool = new RedisConnectionPool("My redis pool",
                    new RedisPoolSettings("127.0.0.1", 6379, maxConnectionCount: 1)))
            {
                pool.PubSubChannel.PSubscribe((m) =>
                {
                    Console.WriteLine("Channel: " + m.Channel);
                    Console.WriteLine("Pattern: " + m.Pattern);

                    if (m.Data != null)
                        Console.WriteLine("Received data: " + Encoding.UTF8.GetString((byte[])m.Data));
                    else
                        Console.WriteLine("Received data: ?");

                    Console.WriteLine();
                    Console.WriteLine("Press any key to escape ...");
                }, "ab*", "xyz");

                Thread.Sleep(5000);
                pool.PubSubChannel.PUnsubscribe("ab*");

                Thread.Sleep(5000);
                pool.PubSubChannel.PUnsubscribe("xy*");

                Console.WriteLine();
                Console.WriteLine("Press any key to escape ...");

                Console.ReadKey();
            }
        }

        static void PubSubTest8()
        {
            using (var pool = new RedisConnectionPool("My redis pool",
                    new RedisPoolSettings("127.0.0.1", 6379, maxConnectionCount: 1)))
            {
                pool.PubSubChannel.PSubscribe((m) =>
                {
                    Console.WriteLine("Channel: " + m.Channel);
                    Console.WriteLine("Pattern: " + m.Pattern);

                    if (m.Data != null)
                        Console.WriteLine("Received data: " + Encoding.UTF8.GetString((byte[])m.Data));
                    else
                        Console.WriteLine("Received data: ?");

                    Console.WriteLine();
                    Console.WriteLine("Press any key to escape ...");
                }, "ab*");

                Thread.Sleep(5000);

                pool.PubSubChannel.PUnsubscribe("ab*");

                Console.WriteLine();
                Console.WriteLine("Press any key to escape ...");

                Console.ReadKey();
            }
        }

        static void PubSubTest7()
        {
            using (var pool = new RedisConnectionPool("My redis pool",
                    new RedisPoolSettings("127.0.0.1", 6379, maxConnectionCount: 1)))
            {
                pool.PubSubChannel.PSubscribe((m) =>
                {
                    Console.WriteLine("Channel: " + m.Channel);
                    Console.WriteLine("Pattern: " + m.Pattern);

                    if (m.Data != null)
                        Console.WriteLine("Received data: " + Encoding.UTF8.GetString((byte[])m.Data));
                    else
                        Console.WriteLine("Received data: ?");

                    Console.WriteLine();
                    Console.WriteLine("Press any key to escape ...");
                }, "ab*", "xy*");

                Console.WriteLine();
                Console.WriteLine("Press any key to escape ...");

                Console.ReadKey();
            }
        }

        static void PubSubTest6()
        {
            using (var pool = new RedisConnectionPool("My redis pool",
                    new RedisPoolSettings("127.0.0.1", 6379, maxConnectionCount: 1)))
            {
                pool.PubSubChannel.PSubscribe((m) =>
                {
                    Console.WriteLine("Channel: " + m.Channel);
                    Console.WriteLine("Pattern: " + m.Pattern);

                    if (m.Data != null)
                        Console.WriteLine("Received data: " + Encoding.UTF8.GetString((byte[])m.Data));
                    else
                        Console.WriteLine("Received data: ?");

                    Console.WriteLine();
                    Console.WriteLine("Press any key to escape ...");
                }, "ab*");

                Console.WriteLine();
                Console.WriteLine("Press any key to escape ...");

                Console.ReadKey();
            }
        }

        static void PubSubTest5()
        {
            using (var pool = new RedisConnectionPool("My redis pool",
                    new RedisPoolSettings("127.0.0.1", 6379, maxConnectionCount: 1)))
            {
                pool.PubSubChannel.Subscribe((m) =>
                {
                    Console.WriteLine("Channel: " + m.Channel);
                    if (m.Data != null)
                        Console.WriteLine("Received data: " + Encoding.UTF8.GetString((byte[])m.Data));
                    else
                        Console.WriteLine("Received data: ?");

                    Console.WriteLine();
                    Console.WriteLine("Press any key to escape ...");
                }, "abc", "xyz");

                Thread.Sleep(4000);

                pool.PubSubChannel.Unsubscribe();

                Console.WriteLine();
                Console.WriteLine("Press any key to escape ...");

                Console.ReadKey();
            }
        }

        static void PubSubTest4()
        {
            using (var pool = new RedisConnectionPool("My redis pool",
                    new RedisPoolSettings("127.0.0.1", 6379, maxConnectionCount: 1)))
            {
                pool.PubSubChannel.Subscribe((m) =>
                {
                    Console.WriteLine("Channel: " + m.Channel);
                    if (m.Data != null)
                        Console.WriteLine("Received data: " + Encoding.UTF8.GetString((byte[])m.Data));
                    else
                        Console.WriteLine("Received data: ?");

                    Console.WriteLine();
                    Console.WriteLine("Press any key to escape ...");
                }, "abc", "xyz");

                Thread.Sleep(4000);

                pool.PubSubChannel.Unsubscribe("xyz");

                Console.WriteLine();
                Console.WriteLine("Press any key to escape ...");

                Console.ReadKey();
            }
        }

        static void PubSubTest3()
        {
            using (var pool = new RedisConnectionPool("My redis pool",
                    new RedisPoolSettings("127.0.0.1", 6379, maxConnectionCount: 1)))
            {
                pool.PubSubChannel.Subscribe((m) =>
                {
                    Console.WriteLine("Channel: " + m.Channel);
                    if (m.Data != null)
                        Console.WriteLine("Received data: " + Encoding.UTF8.GetString((byte[])m.Data));
                    else
                        Console.WriteLine("Received data: ?");

                    Console.WriteLine();
                    Console.WriteLine("Press any key to escape ...");
                }, "abc", "xyz");

                Console.WriteLine();
                Console.WriteLine("Press any key to escape ...");

                Console.ReadKey();
            }
        }

        static void PubSubTest2()
        {
            using (var pool = new RedisConnectionPool("My redis pool",
                    new RedisPoolSettings("127.0.0.1", 6379, maxConnectionCount: 1)))
            {
                pool.PubSubChannel.Subscribe((m) =>
                {
                    Console.WriteLine("Channel: " + m.Channel);
                    if (m.Data != null)
                        Console.WriteLine("Received data: " + Encoding.UTF8.GetString((byte[])m.Data));
                    else
                        Console.WriteLine("Received data: ?");

                    Console.WriteLine();
                    Console.WriteLine("Press any key to escape ...");
                }, "abc");

                Console.WriteLine();
                Console.WriteLine("Press any key to escape ...");

                Console.ReadKey();
            }
        }

        static void PubSubTest1()
        {
            using (var pool = new RedisConnectionPool("My redis pool",
                    new RedisPoolSettings("127.0.0.1", 6379, maxConnectionCount: 1)))
            {
                using (var db = pool.GetDb())
                {
                    do
                    {
                        Console.Clear();
                        try
                        {
                            Console.WriteLine("Channels:");

                            var result = db.PubSubs.PubSubChannels();
                            if (result == null || result.Length == 0)
                                Console.WriteLine("(nil)");
                            else
                            {
                                for (var i = 0; i < result.Length; i++)
                                {
                                    var channel = result[i];
                                    Console.WriteLine((i + 1) + ") " + (String.IsNullOrEmpty(channel) ? "(nil)" : channel));
                                }
                            }
                        }
                        catch (Exception e)
                        {
                            Console.WriteLine(e);
                        }

                        Console.WriteLine();
                        try
                        {
                            Console.WriteLine("Numer of Subscribers:");

                            var result = db.PubSubs.PubSubNumerOfSubscribers("xyz");
                            if (result == null)
                                Console.WriteLine("(nil)");
                            else
                            {
                                var value = result.Value;
                                if (value == null || value.Length == 0)
                                    Console.WriteLine("(nil)");
                                else
                                {
                                    for (var i = 0; i < value.Length; i++)
                                    {
                                        var channel = value[i];
                                        Console.WriteLine((i + 1) + ") " + channel.Key + ": " + channel.Value);
                                    }
                                }
                            }
                        }
                        catch (Exception e)
                        {
                            Console.WriteLine(e);
                        }

                        Console.WriteLine();
                        try
                        {
                            Console.Write("Numer of Pattern Subscribers: ");
                            Console.WriteLine(db.PubSubs.PubSubNumerOfSubscriptionsToPatterns());
                        }
                        catch (Exception e)
                        {
                            Console.WriteLine(e);
                        }

                        Console.WriteLine();
                        Console.WriteLine("Press any key to continue, ESC to escape ...");
                    }
                    while (Console.ReadKey(true).Key != ConsoleKey.Escape);
                }
            }
        }

        #endregion PubSub Tests

        #region Scripting Tests

        static void ScriptingNoArgsEvalTest()
        {
            var script = "return {1,2,3.3333,'foo',nil,'bar'}";

            using (var pool = new RedisConnectionPool("My redis pool",
                    new RedisPoolSettings("127.0.0.1", 6379, maxConnectionCount: 1)))
            {
                using (var db = pool.GetDb())
                {
                    do
                    {
                        Console.Clear();
                        try
                        {
                            var result = db.Scripting.Eval(script);
                            Console.WriteLine((result == null) ? "(nil)" : result.ToString());
                        }
                        catch (Exception e)
                        {
                            Console.WriteLine(e);
                        }

                        Console.WriteLine();
                        Console.WriteLine("Press any key to continue, ESC to escape ...");
                    }
                    while (Console.ReadKey(true).Key != ConsoleKey.Escape);
                }
            }
        }

        static void ScriptingWithArgsEvalTest()
        {
            var script = "return {KEYS[1],KEYS[2],ARGV[1],ARGV[2]}";

            using (var pool = new RedisConnectionPool("My redis pool",
                    new RedisPoolSettings("127.0.0.1", 6379, maxConnectionCount: 1)))
            {
                using (var db = pool.GetDb())
                {
                    do
                    {
                        Console.Clear();
                        try
                        {
                            var result = db.Scripting.Eval(script,
                                                                 new RedisKeyValue<RedisParam, RedisParam>("key1", "first"),
                                                                 new RedisKeyValue<RedisParam, RedisParam>("key2", "second"));
                            Console.WriteLine((result == null) ? "(nil)" : result.ToString());
                        }
                        catch (Exception e)
                        {
                            Console.WriteLine(e);
                        }

                        Console.WriteLine();
                        Console.WriteLine("Press any key to continue, ESC to escape ...");
                    }
                    while (Console.ReadKey(true).Key != ConsoleKey.Escape);
                }
            }
        }

        static void ScriptingShaNoArgsEvalTest()
        {
            var script = "return {1,2,3.3333,'foo',nil,'bar'}";

            using (var pool = new RedisConnectionPool("My redis pool",
                    new RedisPoolSettings("127.0.0.1", 6379, maxConnectionCount: 1)))
            {
                using (var db = pool.GetDb())
                {
                    var sha1 = db.Scripting.ScriptLoad(script);
                    do
                    {
                        Console.Clear();
                        try
                        {
                            var result = db.Scripting.EvalSHA(sha1.Value);
                            Console.WriteLine((result == null) ? "(nil)" : result.ToString());
                        }
                        catch (Exception e)
                        {
                            Console.WriteLine(e);
                        }

                        Console.WriteLine();
                        Console.WriteLine("Press any key to continue, ESC to escape ...");
                    }
                    while (Console.ReadKey(true).Key != ConsoleKey.Escape);
                }
            }
        }

        static void ScriptingShaWithArgsEvalTest()
        {
            var script = "return {KEYS[1],KEYS[2],ARGV[1],ARGV[2]}";

            using (var pool = new RedisConnectionPool("My redis pool",
                    new RedisPoolSettings("127.0.0.1", 6379, maxConnectionCount: 1)))
            {
                using (var db = pool.GetDb())
                {
                    var sha1 = db.Scripting.ScriptLoad(script);
                    do
                    {
                        Console.Clear();
                        try
                        {
                            var result = db.Scripting.EvalSHA(sha1.Value,
                                                                 new RedisKeyValue<RedisParam, RedisParam>("key1", "first"),
                                                                 new RedisKeyValue<RedisParam, RedisParam>("key2", "second"));
                            Console.WriteLine((result == null) ? "(nil)" : result.ToString());
                        }
                        catch (Exception e)
                        {
                            Console.WriteLine(e);
                        }

                        Console.WriteLine();
                        Console.WriteLine("Press any key to continue, ESC to escape ...");
                    }
                    while (Console.ReadKey(true).Key != ConsoleKey.Escape);
                }
            }
        }

        #endregion Scripting Tests

        #region Performance Tests

        static void PerformanceTest2()
        {
            var largeText = new string('x', 100000);

            using (var pool = new RedisConnectionPool("My redis pool",
                    new RedisPoolSettings("127.0.0.1", 6379, maxConnectionCount: 1)))
            {
                using (var db = pool.GetDb())
                {
                    db.Strings.Set("large_text", largeText);
                    db.Strings.Get("large_text");
                }

                do
                {
                    Console.Clear();
                    try
                    {
                        var sw = new Stopwatch();
                        using (var db = pool.GetDb())
                        {
                            sw.Restart();

                            for (var i = 0; i < 1000; i++)
                                db.Strings.Get("large_text");
                        }

                        sw.Stop();
                        Console.WriteLine("Elleapsed time: " + sw.ElapsedMilliseconds.ToString("D3") + " msec");
                    }
                    catch (Exception e)
                    {
                        Console.WriteLine(e);
                    }

                    Console.WriteLine();
                    Console.WriteLine("Press any key to continue, ESC to escape ...");
                }
                while (Console.ReadKey(true).Key != ConsoleKey.Escape);
            }
        }

        static void PerformanceTest1()
        {
            var largeText = new string('x', 100000);

            using (var pool = new RedisConnectionPool("My redis pool",
                    new RedisPoolSettings("127.0.0.1", 6379, maxConnectionCount: 1)))
            {
                using (var db = pool.GetDb())
                {
                    db.Connection.Ping();
                }

                do
                {
                    Console.Clear();
                    try
                    {
                        var sw = new Stopwatch();
                        using (var db = pool.GetDb())
                        {
                            sw.Restart();

                            for (var i = 0; i < 1000; i++)
                                db.Connection.Ping();
                        }

                        sw.Stop();
                        Console.WriteLine("Elleapsed time: " + sw.ElapsedMilliseconds.ToString("D3") + " msec");
                    }
                    catch (Exception e)
                    {
                        Console.WriteLine(e);
                    }

                    Console.WriteLine();
                    Console.WriteLine("Press any key to continue, ESC to escape ...");
                }
                while (Console.ReadKey(true).Key != ConsoleKey.Escape);
            }
        }

        #endregion Performance Tests
    }
}
