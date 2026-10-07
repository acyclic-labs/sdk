using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

namespace Actors.StreamBinding;

/// Thin .NET async-stream adapters over generated Rust-owned cursors.
/// The adapter owns only iteration and guaranteed cursor close; ordering,
/// replay, recovery, and transport remain in the Rust Stream provider.
public static class StreamAsyncEnumerable
{
    public static IAsyncEnumerable<StreamRecord> ReadStream(
        this StreamClientBridge client,
        string path,
        ulong from,
        uint limit,
        CancellationToken cancellationToken = default) =>
        RecordStream(client.Read(path, from, limit, cancellationToken), cancellationToken);

    public static IAsyncEnumerable<StreamRecord> FollowStream(
        this StreamClientBridge client,
        string path,
        ulong from,
        CancellationToken cancellationToken = default) =>
        RecordStream(client.Follow(path, from, cancellationToken), cancellationToken);

    public static IAsyncEnumerable<StreamRecord> ReadRemoteStream(
        this RemoteStreamClientBridge client,
        string path,
        ulong from,
        uint limit,
        CancellationToken cancellationToken = default) =>
        RecordStream(client.Read(path, from, limit, cancellationToken), cancellationToken);

    public static IAsyncEnumerable<StreamRecord> FollowRemoteStream(
        this RemoteStreamClientBridge client,
        string path,
        ulong from,
        CancellationToken cancellationToken = default) =>
        RecordStream(client.Follow(path, from, cancellationToken), cancellationToken);

    public static IAsyncEnumerable<StreamChild> ChildrenStream(
        this StreamClientBridge client,
        string? parent,
        uint limit,
        CancellationToken cancellationToken = default) =>
        ChildStream(client.Children(parent, limit, cancellationToken), cancellationToken);

    public static IAsyncEnumerable<StreamChild> ChildrenRemoteStream(
        this RemoteStreamClientBridge client,
        string? parent,
        uint limit,
        CancellationToken cancellationToken = default) =>
        ChildStream(client.Children(parent, limit, cancellationToken), cancellationToken);

    private static async IAsyncEnumerable<StreamRecord> RecordStream(
        Task<RecordCursor> open,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        var cursor = await open.ConfigureAwait(false);
        try
        {
            while (true)
            {
                var item = await cursor.Next(cancellationToken).ConfigureAwait(false);
                if (item is null)
                    yield break;
                yield return item;
            }
        }
        finally
        {
            // Never use the caller's cancelled token for cleanup. CloseCursor
            // is the generated call that drops the Rust BoxStream.
            await cursor.CloseCursor(CancellationToken.None).ConfigureAwait(false);
        }
    }

    private static async IAsyncEnumerable<StreamChild> ChildStream(
        Task<ChildCursor> open,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        var cursor = await open.ConfigureAwait(false);
        try
        {
            while (true)
            {
                var item = await cursor.Next(cancellationToken).ConfigureAwait(false);
                if (item is null)
                    yield break;
                yield return item;
            }
        }
        finally
        {
            await cursor.CloseCursor(CancellationToken.None).ConfigureAwait(false);
        }
    }
}
