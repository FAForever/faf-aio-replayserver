import asyncio
import pytest

from replayserver.send.timestamp import Timestamp


@pytest.mark.asyncio
async def test_timestamp(mock_replay_streams, time_skipper, event_loop):
    mock_replay_stream = mock_replay_streams()
    stamp = Timestamp(mock_replay_stream, 1, 5)
    mock_replay_stream.configure_mock(data=b"")
    mock_replay_stream.ended.return_value = False

    data_at_second = []
    stream_end_time = 0

    async def add_data():
        nonlocal stream_end_time
        data_at_second.append(0)
        await asyncio.sleep(0.5)
        for i in range(0, 10):
            mock_replay_stream.data += b"a"
            data_at_second.append(len(mock_replay_stream.data))
            await asyncio.sleep(1)
        mock_replay_stream.ended.return_value = True
        stream_end_time = event_loop.time()

    async def check_timestamps():
        async for pos in stamp.timestamps():
            if not mock_replay_stream.ended():
                second = int(event_loop.time() + 0.5)
                past_pos = data_at_second[max(0, second - 5)]
                assert pos <= past_pos
            else:
                assert event_loop.time() - stream_end_time <= 1

    f = asyncio.ensure_future(add_data())
    g = asyncio.ensure_future(check_timestamps())
    for i in range(0, 50):
        await time_skipper.advance(0.25)

    await f
    await g
