using FluentAssertions;
using Microsoft.Extensions.Options;
using NSubstitute;
using System;
using System.Collections.Generic;
using System.IO.Pipelines;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

namespace Secs4Net.UnitTests;

public class SecsGemUnitTests
{
    private readonly PipeConnection _pipeConnection1;
    private readonly PipeConnection _pipeConnection2;
    private static readonly IOptions<SecsGemOptions> OptionsActive = Options.Create(new SecsGemOptions
    {
        IsActive = true,
        DeviceId = 0,
        T3 = 60000,
    });

    private static readonly IOptions<SecsGemOptions> OptionsPassive = Options.Create(new SecsGemOptions
    {
        IsActive = false,
        DeviceId = 0,
        T3 = 60000,
    });

    public SecsGemUnitTests()
    {
        var pipe1 = new Pipe(new PipeOptions(useSynchronizationContext: false));
        var pipe2 = new Pipe(new PipeOptions(useSynchronizationContext: false));
        _pipeConnection1 = new PipeConnection(decoderReader: pipe1.Reader, decoderInput: pipe2.Writer);
        _pipeConnection2 = new PipeConnection(decoderReader: pipe2.Reader, decoderInput: pipe1.Writer);
    }

    [Fact]
    public async Task SecsGem_SendAsync_And_Return_Secondary_Message()
    {
        var options = Options.Create(new SecsGemOptions
        {
            SocketReceiveBufferSize = 32,
            DeviceId = 0,
        });
        using var secsGem1 = new SecsGem(options, _pipeConnection1, Substitute.For<ISecsGemLogger>());
        using var secsGem2 = new SecsGem(options, _pipeConnection2, Substitute.For<ISecsGemLogger>());

        var ping = new SecsMessage(s: 1, f: 13)
        {
            SecsItem = A("Ping"),
        };

        var pong = new SecsMessage(s: 1, f: 14, replyExpected: false)
        {
            SecsItem = A("Pong"),
        };

        using var cts = new CancellationTokenSource();
        _pipeConnection1.Start(cts.Token);
        _pipeConnection2.Start(cts.Token);
        _ = Task.Run(async () =>
        {
            var msg = await secsGem2.GetPrimaryMessageAsync(cts.Token).FirstAsync(cts.Token);
            msg.PrimaryMessage.Should().BeEquivalentTo(ping);
            await msg.TryReplyAsync(pong);
        });

        var reply = await secsGem1.SendAsync(ping, cts.Token);
        reply.Should().NotBeNull().And.BeEquivalentTo(pong);
    }

    [Fact]
    public async Task Malformed_WBit_Primary_Message_Should_Reply_S9F7_With_Original_Header()
    {
        byte[] malformedMessage =
        [
            0x00, 0x00, 0x00, 0x1B,
            0x7F, 0xFF, 0x86, 0x0B, 0x00, 0x00, 0x06, 0x0B, 0x00, 0xD6,
            0x01, 0x03, 0xA5, 0x01, 0x3C, 0xA9, 0x02, 0x00, 0x16,
            0x01, 0x01, 0x01, 0x02, 0xA9, 0x02, 0x22, 0xB8,
        ];

        using var secsGem = new SecsGem(OptionsActive, _pipeConnection2, Substitute.For<ISecsGemLogger>());
        using var cancellationSource = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        _pipeConnection1.Start(cancellationSource.Token);
        _pipeConnection2.Start(cancellationSource.Token);

        await ((ISecsConnection)_pipeConnection1).SendAsync(malformedMessage, cancellationSource.Token);
        var (replyHeader, replyItem) = await ((ISecsConnection)_pipeConnection1)
            .GetDataMessages(cancellationSource.Token)
            .FirstAsync(cancellationSource.Token);

        replyHeader.S.Should().Be(9);
        replyHeader.F.Should().Be(7);
        replyHeader.ReplyExpected.Should().BeFalse();
        replyItem.Should().NotBeNull();
        using (replyItem)
        {
            replyItem!.GetMemory<byte>().ToArray().Should().Equal(malformedMessage.Skip(4).Take(10));
        }
    }

    [Fact]
    public void SecsGem_SendAsync_With_Different_Device_Id()
    {
        var options1 = Options.Create(new SecsGemOptions
        {
            SocketReceiveBufferSize = 32,
            DeviceId = 0,
        });
        var options2 = Options.Create(new SecsGemOptions
        {
            SocketReceiveBufferSize = 32,
            DeviceId = 1,
        });
        using var secsGem1 = new SecsGem(options1, _pipeConnection1, Substitute.For<ISecsGemLogger>());
        using var secsGem2 = new SecsGem(options2, _pipeConnection2, Substitute.For<ISecsGemLogger>());

        var ping = new SecsMessage(s: 1, f: 13)
        {
            SecsItem = A("Ping"),
        };

        using var cts = new CancellationTokenSource();
        _pipeConnection1.Start(cts.Token);
        _pipeConnection2.Start(cts.Token);

        var receiver = Substitute.For<Action<SecsMessage>>();
        _ = Task.Run(async () =>
        {
            await foreach (var a in secsGem2.GetPrimaryMessageAsync(cts.Token))
            {
                // can't receive any message, reply S9F1 internally
                receiver(a.PrimaryMessage);
            }
        });

        Func<Task> sendAsync = async () =>
        {
            var reply = await secsGem1.SendAsync(ping, cts.Token);
        };

        sendAsync.Should().ThrowAsync<SecsException>().WithMessage(Resources.S9F1);
        receiver.DidNotReceive();
    }

    [Fact]
    public void SecsGem_SendAsync_With_T3_Timeout()
    {
        var options1 = Options.Create(new SecsGemOptions
        {
            SocketReceiveBufferSize = 32,
            DeviceId = 0,
            T3 = 500,
        });
        using var secsGem1 = new SecsGem(options1, _pipeConnection1, Substitute.For<ISecsGemLogger>());
        using var secsGem2 = new SecsGem(options1, _pipeConnection2, Substitute.For<ISecsGemLogger>());

        var ping = new SecsMessage(s: 1, f: 13)
        {
            SecsItem = A("Ping"),
        };
        var pong = new SecsMessage(s: 1, f: 14, replyExpected: false)
        {
            SecsItem = A("Pong"),
        };

        using var cts = new CancellationTokenSource();

        _ = Task.Run(async () =>
        {
            await foreach (var a in secsGem2.GetPrimaryMessageAsync(cts.Token))
            {
                await Task.Delay(TimeSpan.FromMilliseconds(options1.Value.T3 + 100)); // process delay over T3
                await a.TryReplyAsync(pong);
            }
        });

        var receiver = Substitute.For<Action>();
        Func<Task> sendAsync = async () =>
        {
            var reply = await secsGem1.SendAsync(ping, cts.Token);
            receiver();
        };

        sendAsync.Should().ThrowAsync<SecsException>().WithMessage(Resources.T3Timeout);
        receiver.DidNotReceive();
    }

    [Fact]
    public async Task SecsGem_PipeConnection_SendAsync_With_A_Large_Number_Of_Messages_At_Once()
    {
        using var cts = new CancellationTokenSource();
        _pipeConnection1.Start(cts.Token);
        _pipeConnection2.Start(cts.Token);
        await SendAsyncManyMessagesAtOnce(_pipeConnection1, _pipeConnection2, cts.Token);
    }

    [Fact]
    public async Task SecsGem_HsmsConnection_SendAsync_With_A_Large_Number_Of_Messages_At_Once()
    {
        await using var connection1 = new HsmsConnection(OptionsActive, Substitute.For<ISecsGemLogger>());
        await using var connection2 = new HsmsConnection(OptionsPassive, Substitute.For<ISecsGemLogger>());
        using var cts = new CancellationTokenSource();

        connection1.Start(cts.Token);
        connection2.Start(cts.Token);

        SpinWait.SpinUntil(() => connection1.State is ConnectionState.Selected && connection2.State is ConnectionState.Selected);

        await SendAsyncManyMessagesAtOnce(connection1, connection2, cts.Token);
    }

    [Fact]
    public async Task HsmsConnection_Start_Should_Not_Spawn_Parallel_Connection_Loops()
    {
        var port = GetFreeTcpPort();
        var options = CreateConnectionOptions(isActive: true, port: port, t5: 1000);
        await using var connection = new HsmsConnection(options, Substitute.For<ISecsGemLogger>());
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));

        var transitions = new List<ConnectionState>();
        var syncRoot = new object();
        connection.ConnectionChanged += (_, state) =>
        {
            lock (syncRoot)
            {
                transitions.Add(state);
            }
        };

        await Task.WhenAll(Enumerable.Range(0, 20).Select(_ => Task.Run(() => connection.Start(cts.Token), cts.Token)));

        SpinWait.SpinUntil(() =>
        {
            lock (syncRoot)
            {
                return transitions.Count > 0;
            }
        }, TimeSpan.FromSeconds(2)).Should().BeTrue();

        await Task.Delay(200, cts.Token);

        lock (syncRoot)
        {
            transitions.Count(state => state == ConnectionState.Connecting).Should().Be(1);
        }
    }

    [Fact]
    public async Task HsmsConnection_Reconnect_Should_Only_Run_One_Retry_Cycle_When_Triggered_Concurrently()
    {
        var port = GetFreeTcpPort();
        var activeOptions = CreateConnectionOptions(isActive: true, port: port, t5: 200);
        var passiveOptions = CreateConnectionOptions(isActive: false, port: port, t5: 200);
        await using var activeConnection = new HsmsConnection(activeOptions, Substitute.For<ISecsGemLogger>());
        await using var passiveConnection = new HsmsConnection(passiveOptions, Substitute.For<ISecsGemLogger>());
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(15));

        activeConnection.Start(cts.Token);
        passiveConnection.Start(cts.Token);

        SpinWait.SpinUntil(
            () => activeConnection.State is ConnectionState.Selected && passiveConnection.State is ConnectionState.Selected,
            TimeSpan.FromSeconds(5)).Should().BeTrue();

        var transitions = new List<ConnectionState>();
        var syncRoot = new object();
        activeConnection.ConnectionChanged += (_, state) =>
        {
            lock (syncRoot)
            {
                transitions.Add(state);
            }
        };

        await Task.WhenAll(Enumerable.Range(0, 20).Select(_ => Task.Run(activeConnection.Reconnect, cts.Token)));

        SpinWait.SpinUntil(
            () => activeConnection.State is ConnectionState.Selected && passiveConnection.State is ConnectionState.Selected,
            TimeSpan.FromSeconds(10)).Should().BeTrue();

        await Task.Delay(300, cts.Token);

        lock (syncRoot)
        {
            transitions.Count(state => state == ConnectionState.Retry).Should().Be(1);
            transitions.Count(state => state == ConnectionState.Connecting).Should().Be(1);
            transitions.Count(state => state == ConnectionState.Connected).Should().Be(1);
            transitions.Count(state => state == ConnectionState.Selected).Should().Be(1);
        }
    }

    private static async Task SendAsyncManyMessagesAtOnce(ISecsConnection connection1, ISecsConnection connection2, CancellationToken cancellation)
    {
        using var secsGem1 = new SecsGem(OptionsActive, connection1, Substitute.For<ISecsGemLogger>());
        using var secsGem2 = new SecsGem(OptionsPassive, connection2, Substitute.For<ISecsGemLogger>());

        _ = Task.Run(async () =>
        {
            var pong = new SecsMessage(s: 1, f: 14, replyExpected: false)
            {
                SecsItem = A("Pong"),
            };

            await foreach (var a in secsGem2.GetPrimaryMessageAsync(cancellation))
            {
                await a.TryReplyAsync(pong);
            }
        });

        Func<Task> sendAsync = async () =>
        {
            var ping = new SecsMessage(s: 1, f: 13)
            {
                SecsItem = A("Ping"),
            };

            var sendCount = 100;
            var totalTasks = new List<Task<SecsMessage>>(capacity: sendCount);
            for (var g = 0; g < sendCount; g++)
            {
                totalTasks.Add(secsGem1.SendAsync(ping, cancellation));
            }
            var results = await Task.WhenAll(totalTasks.ToArray());
            results.Should().HaveCount(sendCount);
        };

        await sendAsync.Should().NotThrowAsync();
    }

    private static IOptions<SecsGemOptions> CreateConnectionOptions(bool isActive, int port, int t5)
        => Options.Create(new SecsGemOptions
        {
            IsActive = isActive,
            DeviceId = 0,
            IpAddress = IPAddress.Loopback.ToString(),
            Port = port,
            T5 = t5,
        });

    private static int GetFreeTcpPort()
    {
        using var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        return ((IPEndPoint)listener.LocalEndpoint).Port;
    }

}
