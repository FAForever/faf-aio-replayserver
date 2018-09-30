
import pytest

from replay_server.replay_parser.replay_parser.parser import parse


@pytest.mark.asyncio
@pytest.mark.timeout(2)
async def test_multiple_connections(client, replay_data, put_replay, get_replay):
    """
    Concurency behavior, we can read data, while they're streamed.
    """

    _, writer1 = await client()
    reader2, writer2 = await client()
    reader3, writer3 = await client()
    writer1.write(put_replay)
    writer1.write(replay_data)
    await writer1.drain()

    writer2.write(get_replay)
    writer3.write(get_replay)
    await writer2.drain()
    await writer3.drain()
    # network connection and read() won't stop, until first client will close connection
    writer1.close()

    assert await reader2.read() == replay_data
    assert await reader3.read() == replay_data


@pytest.mark.asyncio
@pytest.mark.timeout(2)
async def test_common_read_from_multiple_different_streams(client, replay_data, put_replay, get_replay):
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

    writer1.write(put_replay)
    writer1.write(replay_data)
    writer2.write(put_replay)
    writer2.write(replay_data)

    replay_copy = bytearray(replay_data[:])
    # let's add some "difference" at second third part
    replay_copy[-1 * int(len(replay_copy) // 3)] = 126
    writer3.write(put_replay)
    writer3.write(replay_copy)

    writer4.write(get_replay)

    await writer1.drain()
    await writer2.drain()
    await writer3.drain()

    # connection for reader4 will be alive until that 3 won't close.
    writer1.close()
    writer2.close()
    writer3.close()

    assert await reader4.read() == replay_data


@pytest.mark.asyncio
@pytest.mark.timeout(2)
async def test_read_waited_for_new_data(client, replay_data, put_replay, get_replay):
    """
    Check, that we will know that temporary file is moved at the end of replay
    """
    _, writer1 = await client()
    reader2, writer2 = await client()

    replay_struct = parse(replay_data)
    body_offset = replay_struct['body_offset']

    writer1.write(put_replay)
    writer1.write(replay_data[0:body_offset])
    await writer1.drain()
    writer2.write(get_replay)
    await writer2.drain()

    # header
    part_data = await reader2.read(body_offset)
    assert part_data == replay_data[0:body_offset]

    writer1.write(replay_data[body_offset + 1:body_offset + 8])
    await writer1.drain()
    part_data = await reader2.read(7)
    assert part_data == replay_data[body_offset + 1:body_offset + 8]

    writer1.write(replay_data[body_offset + 9:body_offset + 16])
    await writer1.drain()
    part_data = await reader2.read(7)
    assert part_data == replay_data[body_offset + 9:body_offset + 16]

    writer2.close()
    writer1.close()
