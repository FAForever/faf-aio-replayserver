import asyncio
import decorator
from tests.timeskipper import TimeSkipper

__all__ = ["timeout", "TimeSkipper"]


def timeout(time):
    def deco(coro):
        async def wrapper_function(coro, *args, **kwargs):
            return (await asyncio.wait_for(coro(*args, **kwargs), time))
        # Needed for fixtures to work
        return decorator.decorator(wrapper_function, coro)
    return deco
