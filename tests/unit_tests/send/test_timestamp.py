import asyncio
import pytest
from tests import fast_forward_time, timeout

from replayserver.streams.delayed import Timestamp


@pytest.mark.asyncio
@fast_forward_time(0.25, 25)
@timeout(20)
async def test_timestamp(event_loop, outside_source_stream):
    stamp = Timestamp(outside_source_stream, 1, 5)

    data_at_second = []
    stream_end_time = 0

    async def add_data():
        nonlocal stream_end_time
        data_at_second.append(0)
        await asyncio.sleep(0.5)
        for i in range(0, 10):
            outside_source_stream.feed_data(b"a")
            data_at_second.append(len(outside_source_stream.data))
            await asyncio.sleep(1)
        outside_source_stream.finish()
        stream_end_time = event_loop.time()

    async def check_timestamps():
        async for pos in stamp.timestamps():
            if not outside_source_stream.ended():
                second = int(event_loop.time() + 0.5)
                past_pos = data_at_second[max(0, second - 5)]
                assert pos <= past_pos
            else:
                assert event_loop.time() - stream_end_time <= 1

    f = asyncio.ensure_future(add_data())
    g = asyncio.ensure_future(check_timestamps())

    await f
    await g


@pytest.mark.asyncio
@fast_forward_time(0.25, 2)
@timeout(1)
async def test_timestamp_ends_immediately(event_loop, outside_source_stream):
    stamp = Timestamp(outside_source_stream, 10, 20)

    async def sw():
        async for _ in stamp.timestamps():   # noqa
            pass

    f = asyncio.ensure_future(sw())
    await asyncio.sleep(0.5)
    outside_source_stream.feed_data(b"foo")
    outside_source_stream.finish()
    await f
