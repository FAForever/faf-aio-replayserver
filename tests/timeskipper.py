# Copyright 2015 Martin Richard
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy of
# the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations under
# the License.

import asyncio
import functools


# Pulled out from asynctest's ClockedTestCase
class TimeSkipper():
    def __init__(self, loop):
        self.loop = loop
        self.loop.time = functools.wraps(self.loop.time)(lambda: self._time)
        self._time = 0

    async def advance(self, seconds):
        if seconds < 0:
            raise ValueError(
                'Cannot go back in time ({} seconds)'.format(seconds))

        await self._drain_loop()

        target_time = self._time + seconds
        while True:
            next_time = self._next_scheduled()
            if next_time is None or next_time > target_time:
                break

            self._time = next_time
            await self._drain_loop()

        self._time = target_time
        await self._drain_loop()

    def _next_scheduled(self):
        try:
            return self.loop._scheduled[0]._when
        except IndexError:
            return None

    async def _drain_loop(self):
        while True:
            next_time = self._next_scheduled()
            if not self.loop._ready and (next_time is None or
                                         next_time > self._time):
                break
            await asyncio.sleep(0)
