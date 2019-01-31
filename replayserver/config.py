"""
Tools and wrappers for Everett config classes. Lets us define configuration
with less boilerplate, provides some common parsers with extra checks.
"""

from everett.component import RequiredConfigMixin


class Config(RequiredConfigMixin):
    """
    Wrapper for RequiredConfigMixin that reduces boilerplate. Just define your
    options in required_config, and all values will be turned into members,
    raising errors if any are missing.

    In general subclasses should be PODs, so that they can be trivially mocked.
    You can add Config members to your config classes (following your
    composition hierarchy), but they shouldn't be required for unit testing.
    """
    def __init__(self, config):
        self.config = config.with_options(self)
        for option in self.required_config:
            setattr(self, option.key,
                    self.config(option.key, raise_error=True))

    def _set_children(self):
        pass
