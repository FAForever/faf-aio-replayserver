import asyncio

import pytest


@pytest.mark.asyncio
@pytest.mark.timeout(5)
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
    await asyncio.sleep(0.1)

    writer2.write(get_replay)
    await writer2.drain()
    writer3.write(get_replay)
    await writer3.drain()

    writer1.close()
    data = await reader2.read()
    assert data == replay_data
    writer2.close()

    data = await reader3.read()
    assert data == replay_data
    writer3.close()
