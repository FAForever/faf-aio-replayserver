import asyncio
from replayserver import Server


def main():
    PORT = 15000
    server = Server.build(PORT)
    loop = asyncio.get_event_loop()
    asyncio.ensure_future(server.start())
    try:
        loop.run_forever()
        return 0
    except Exception:
        loop.run_until_complete(server.stop())
        return 1
    finally:
        loop.close()
