"""
Tools and wrappers for Everett config classes. Lets us define configuration
with less boilerplate, provides some common parsers with extra checks.
"""

import os
from everett.component import RequiredConfigMixin, ConfigOptions


__all__ = ["positive_int", "nonnegative_int", "is_dir", "Config"]


def positive_int(v):
    i = int(v)
    if i <= 0:
        raise ValueError("Expected a positive value")
    return i


def nonnegative_int(v):
    i = int(v)
    if i < 0:
        raise ValueError("Expected a nonnegative value")
    return i


def is_dir(d):
    if not os.path.is_dir(d):
        raise ValueError(f"Directory {d} does not exist")


class _ConfigMeta(type):
    def __init__(cls, name, bases, attrs, *args, **kwargs):
        super().__init__(name, bases, attrs, *args, **kwargs)

        cls.required_config = ConfigOptions()
        if "_options" in attrs:
            for name, attrs in attrs["_options"].items():
                cls.required_config.add_option(name, **attrs)


class Config(RequiredConfigMixin, metaclass=_ConfigMeta):
    """
    Wrapper for RequiredConfigMixin that reduces boilerplate. Just define your
    options in an _options dict, and all values will be turned into members,
    raising errors if any are missing.

    In general subclasses should be PODs, so that they can be trivially mocked.
    You can add Config members to your config classes (following your component
    hierarchy), but they shouldn't be required for unit testing.

    Config object hierarchy roughly matches component hierarchy; feel free to
    skip some nesting levels where we don't need config, put config in parent
    scope if it's just one param, or pass a param directly from build() to
    __init__.
    """
    _options = {}

    def __init__(self, config):
        self.config = config.with_options(self)
        for option in self.required_config:
            setattr(self, option.key,
                    self.config(option.key, raise_error=True))
