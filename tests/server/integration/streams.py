import pytest


@pytest.mark.asyncio
@pytest.mark.timeout(20)
async def test_multiple_connections(db_replay, client, put_replay_command, replay_data, get_replay_command):
    """
    Concurency behavior, we can read data, while they're streamed.
    """

    _, writer1 = await client()
    reader2, writer2 = await client()
    reader3, writer3 = await client()
    writer1.write(put_replay_command)
    writer1.write(replay_data)
    await writer1.drain()

    writer2.write(get_replay_command)
    writer3.write(get_replay_command)
    await writer2.drain()
    await writer3.drain()
    # network connection and read() won't stop, until first client will close connection
    writer1.close()

    assert await reader2.read() == replay_data
    assert await reader3.read() == replay_data

    writer2.close()
    writer3.close()


@pytest.mark.asyncio
@pytest.mark.timeout(30)
async def test_common_read_from_multiple_different_streams(
        db_replay, client, replay_data, put_replay_command, get_replay_command):
    """
    Problem: two groups of users will split in 2 or more parts during the game, mostly because of network problems.
    Some players will be kicked, but they might continue the game and send data to the server.
    We have to find the most common stream of the game and send it to watchers, that are watching game now.

    That test checks, that client will receive most common stream.
    """

    _, writer1 = await client()  # same
    _, writer2 = await client()  # same
    _, writer3 = await client()  # different

    reader4, writer4 = await client()

    writer1.write(put_replay_command)
    writer1.write(replay_data)
    writer2.write(put_replay_command)
    writer2.write(replay_data)

    replay_copy = bytearray(replay_data[:])
    # let's add some "difference" at second third part
    # actually it's dangerous to just change something in stream
    replay_copy[-1 * int(len(replay_copy) // 3)] = 126
    writer3.write(put_replay_command)
    writer3.write(replay_copy)

    writer4.write(get_replay_command)

    await writer1.drain()
    await writer2.drain()
    await writer3.drain()

    # connection for reader4 will be alive until that 3 won't close.
    writer1.close()
    writer2.close()
    writer3.close()

    assert await reader4.read() == replay_data
    writer4.close()


@pytest.mark.asyncio
@pytest.mark.timeout(120)
async def test_read_stream(db_replay, client, streamed_replay_data, replay_data, put_replay_command, get_replay_command):
    """
    Testing streamed content, command by command
    """
    _, writer1 = await client()
    reader2, writer2 = await client()
    header_data = next(streamed_replay_data)

    # send header
    writer1.write(put_replay_command)
    writer1.write(header_data)
    await writer1.drain()
    offset = len(header_data)

    # connect to the server
    writer2.write(get_replay_command)
    await writer2.drain()
    assert await reader2.read(offset) == header_data

    i = 0
    sent_data = b''
    for data in streamed_replay_data:
        sent_data += data
        writer1.write(data)
        if i > 0 and i % 10000 == 0:
            await writer1.drain()
            read_data = await reader2.read(len(sent_data))
            assert read_data == sent_data, (read_data, sent_data)
            sent_data = b''
        i += 1

    writer1.close()
    writer2.close()
