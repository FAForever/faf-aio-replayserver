import os
import asyncio
import decorator
import unittest
import asynctest
import functools
from tests.timeskipper import EventLoopClockAdvancer
from tests.docker_db_config import docker_faf_db_config
from everett.manager import ConfigManager, ConfigDictEnv

# FIXME - there's all kinds of utility stuff here, we should tidy it up

__all__ = ["timeout", "fast_forward_time", "slow_test", "docker_faf_db_config"]


def timeout(time):
    def deco(coro):
        async def wrapper_function(fn, *args, **kwargs):
            return await asyncio.wait_for(fn(*args, **kwargs), time)
        # Needed for fixtures to work
        return decorator.decorator(wrapper_function, coro)
    return deco


def fast_forward_time(timeout):
    def deco(f):
        @functools.wraps(f)
        async def awaiter(*args, **kwargs):
            loop = asyncio.get_event_loop()
            advance_time = EventLoopClockAdvancer(loop)
            time = 0
            fut = asyncio.ensure_future(f(*args, **kwargs))

            while not fut.done() and time < timeout:
                await asynctest.exhaust_callbacks(loop)
                await advance_time(0.1)
                time += 0.1

            return await fut

        return awaiter
    return deco


def skip_stress_test(fn):
    return skip_for_travis(fn, "Manual stress test")


def skip_for_travis(fn, reason):
    return unittest.skipIf(
        "TRAVIS" in os.environ and os.environ["TRAVIS"] == "true",
        reason)(fn)


def slow_test(fn):
    return unittest.skipIf(
        "SKIP_SLOW_TESTS" in os.environ,
        "Test is slow")(fn)


def config_from_dict(d):
    def flatten_dict(d, prefix=""):
        newd = {}
        for key, val in d.items():
            if isinstance(val, dict):
                flatval = flatten_dict(val, f"{prefix}{key}_")
                newd.update(flatval)
            else:
                newd[f"{prefix}{key}"] = val
        return newd

    flatd = flatten_dict(d)
    return ConfigManager([ConfigDictEnv(flatd)])
