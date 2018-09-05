import asyncio
from replayserver import ReplayServer


if __name__ == "__main__":
    PORT = 15000
    server = ReplayServer(PORT)
    loop = asyncio.get_event_loop()
    start = asyncio.ensure_future(server.start())
    try:
        loop.run_forever()
    except Exception:
        loop.run_until_complete(server.stop())
    finally:
        loop.close()
