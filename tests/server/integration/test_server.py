import asyncio
import json
import os
import struct
import zlib
from base64 import b64decode

import pytest

from replay_server.utils.paths import get_replay_path
from replay_server.utils.paths import get_temp_replay_path


@pytest.mark.asyncio
@pytest.mark.timeout(1)
async def test_server_error_response(client, bad_replay):
    """
    Testing server reaction on wrong data sended
    """
    reader, writer = await client()

    assert not reader.at_eof()
    writer.write(bad_replay)
    await writer.drain()
    response = (await reader.read()).decode("utf-8")
    assert response == "Determine type: Unexpected data received"
    assert reader.at_eof()


@pytest.mark.asyncio
@pytest.mark.timeout(1)
async def test_server_good_data(client, put_replay_command, replay_data):
    """
    Server will accept good data
    """
    reader, writer = await client()

    writer.write(put_replay_command)
    writer.write(replay_data)
    await writer.drain()
    assert not reader.at_eof()


@pytest.mark.asyncio
@pytest.mark.timeout(1)
async def test_replay_multiple_data_send(client, put_replay_command, replay_data):
    """
    Test multiple db_connection sending "same data" into one stream.
    """
    _, writer1 = await client()
    _, writer2 = await client()
    _, writer3 = await client()

    writer3.write(put_replay_command)
    writer3.write(replay_data[:len(replay_data) - 2])

    writer1.write(put_replay_command)
    writer2.write(put_replay_command)
    for i in range(len(replay_data) // 4096 + 1):
        writer1.write(replay_data[i * 4096:(i + 1) * 4096])
        writer2.write(replay_data[i * 4096:(i + 1) * 4096])
        await writer1.drain()
        await writer2.drain()

    writer1.close()
    writer2.close()

    writer3.write(replay_data[len(replay_data) - 2:])
    await writer3.drain()
    writer3.close()
    await asyncio.sleep(0.1)


@pytest.mark.asyncio
@pytest.mark.timeout(1)
async def test_server_save_data(client, put_replay_command, replay_data, replay_id, db_replay):
    """
    Server will save data in right format
    """
    _, writer = await client()

    # we'll save replay
    writer.write(put_replay_command)
    writer.write(replay_data)
    await writer.drain()
    writer.close()

    # wait until it'll be saved
    await asyncio.sleep(0.5)

    with open(get_replay_path(replay_id), "r") as f:
        replay_header, replay_b64 = f.read().split("\n")
    gzipped = b64decode(replay_b64)
    data = zlib.decompress(gzipped)
    data_length = struct.unpack("i", data[:4])
    assert replay_data == data[4:]
    assert len(data[4:]) == data_length[0]

    header_data = json.loads(replay_header)
    assert header_data['uid'] == replay_id


@pytest.mark.asyncio
@pytest.mark.timeout(2)
async def test_stream_write_and_clean(client, replay_data, put_replay_command, replay_id, db_replay):
    """
    Check, that we will know that temporary file is moved at the end of replay
    """
    temp_replay_path = get_temp_replay_path(replay_id)
    saved_replay_path = get_replay_path(replay_id)
    _, writer1 = await client()
    writer1.write(put_replay_command)
    writer1.write(replay_data)
    await writer1.drain()
    writer1.close()

    await asyncio.sleep(0.1)

    assert not os.path.exists(temp_replay_path)
    assert os.path.exists(saved_replay_path)
