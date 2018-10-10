import os
import asyncio
import decorator
import unittest
from tests.timeskipper import TimeSkipper

__all__ = ["timeout", "fast_forward_time", "TimeSkipper",
           "skip_if_needs_asynctest_107", "slow_test",
           "docker_faf_db_config"]


def timeout(time):
    def deco(coro):
        async def wrapper_function(coro, *args, **kwargs):
            return (await asyncio.wait_for(coro(*args, **kwargs), time))
        # Needed for fixtures to work
        return decorator.decorator(wrapper_function, coro)
    return deco


def fast_forward_time(step, amount):
    def deco(coro):
        async def wrapper_function(coro, *args, **kwargs):
            # HACK - passing in fixtures to wrappers is hard, so we require
            # wrapped functions to have event_loop as first argument
            event_loop = args[0]
            f = asyncio.ensure_future(coro(*args, **kwargs))
            skipper = TimeSkipper(event_loop)
            while event_loop.time() < (step * 10 + amount) and not f.done():
                await skipper.advance(step)
            await f
        return decorator.decorator(wrapper_function, coro)
    return deco


def skip_if_needs_asynctest_107(fn):
    return unittest.skipIf(
        "TRAVIS" in os.environ and os.environ["TRAVIS"] == "true",
        "Needs asynctest issue #107 resolved to work")(fn)


def slow_test(fn):
    return unittest.skipIf(
        "SKIP_SLOW_TESTS" in os.environ,
        "Test is slow")(fn)


docker_faf_db_config = {
    'host': os.environ.get("FAF_STACK_DB_IP", "172.19.0.2"),
    'port': 3306,
    'user': 'root',
    'password': 'banana',
    'db': 'faf',
}
