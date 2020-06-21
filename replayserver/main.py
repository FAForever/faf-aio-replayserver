"""
Entry point to the server, installed as a runnable script. Gets arguments from
the environment, constructs and runs the server. ::
"""

import asyncio
import signal
import os
from everett.manager import ConfigManager, ConfigOSEnv
from everett.ext.yamlfile import ConfigYamlEnv
from everett import ConfigurationError

from replayserver import Server, MainConfig
from replayserver.logging import logger


__all__ = ["main"]


VERSION = "1.0.9"


def get_program_config():
    sources = [ConfigOSEnv()]
    if "RS_CONFIG_FILE" in os.environ:
        sources.append(ConfigYamlEnv(os.environ["RS_CONFIG_FILE"]))
    config = ConfigManager(sources)
    return MainConfig(config.with_namespace("rs"))


def setup_signal_handler(server, loop):
    shutting_down = False

    def shutdown_gracefully():
        nonlocal shutting_down
        if not shutting_down:
            shutting_down = True
            asyncio.ensure_future(server.stop(), loop=loop)

    for sig in [signal.SIGINT, signal.SIGTERM]:
        loop.add_signal_handler(sig, shutdown_gracefully)


def main():
    logger.info(f"FAF replay server version {VERSION} starting")
    try:
        config = get_program_config()
    except ConfigurationError:
        logger.exception("Invalid configuration was provided!")
        return 1

    try:
        logger.setLevel(config.log_level)
        server = Server.build(config=config)
        loop = asyncio.get_event_loop()
        setup_signal_handler(server, loop)
        loop.run_until_complete(server.run())
        loop.close()
        return 0
    except Exception:
        logger.exception("Critical server error!")
        return 1
