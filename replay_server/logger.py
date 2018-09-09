import logging

import bugsnag

from replay_server.constants import BUG_SNAG_API_KEY, BASE_DIR, ENV, APP_VERSION
from replay_server.constants import LOGGING_LEVEL

__all__ = ('logger',)

logging.basicConfig(format='%(asctime)-15s %(levelname)s %(message)s (%(filename)s:%(lineno)d in %(funcName)s)')
logger = logging.getLogger('replay_server')
logger.setLevel(LOGGING_LEVEL)


bugsnag.configure(
    api_key=BUG_SNAG_API_KEY,
    project_root=BASE_DIR,
    release_stage=ENV,
    app_version=APP_VERSION,
)
