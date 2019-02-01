Configuration
=============

The replay server can be configured either with environment variables or with
YAML files. To configure the server with a YAML file, provide a path to it via
an RS_CONFIG_FILE environment variable.

The YAML config file has hierarchical structure. Each value is a string parsed
into an appropriate type. You can set or override values in the YAML config by
setting an env var with scoped concatenated by '\_' as name, e.g. for:

::

  foo:
      bar:
          baz="1"

The overriding variable is ``FOO_BAR_BAZ``. An example configuration is provided
in repository root as example_config.yml.

Configuration scheme
--------------------

The YAML file has a hierarchical structure, with leaf nodes being actual
options. Config file structure without leaf nodes is shown below:

::

  rs:
      <main options>
      server:
          <server options>
      db:
          <db options>
      storage:
          <storage options>
      replay:
          <replay options>
          send:
              <replay sending options>
          merge:
              <replay receiving options>
              strategy_config:
                  <configuration of specific merge strategies>

Configuration leaf nodes
------------------------

Main options
^^^^^^^^^^^^

.. autocomponent:: replayserver.MainConfig
    :hide-classname:

Server options
^^^^^^^^^^^^^^

.. autocomponent:: replayserver.server.server.ServerConfig
    :hide-classname:

DB options
^^^^^^^^^^

.. autocomponent:: replayserver.bookkeeping.database.DatabaseConfig
    :hide-classname:

Storage options
^^^^^^^^^^^^^^^

.. autocomponent:: replayserver.bookkeeping.bookkeeper.BookkeeperConfig
    :hide-classname:

Replay options
^^^^^^^^^^^^^^

.. autocomponent:: replayserver.server.replay.ReplayConfig
    :hide-classname:

Replay sending options
^^^^^^^^^^^^^^^^^^^^^^

.. autocomponent:: replayserver.send.sender.SenderConfig
    :hide-classname:


Replay receiving options
^^^^^^^^^^^^^^^^^^^^^^^^

.. autocomponent:: replayserver.receive.merger.MergerConfig
    :hide-classname:

Merge strategy configuration
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Each strategy's configuration should be provided under a separate key. You only
need to provide configuration for a strategy you use.

.. autocomponent:: replayserver.receive.mergestrategy.GreedyMergeStrategyConfig

No configuration available.

.. autocomponent:: replayserver.receive.mergestrategy.FollowStreamMergeStrategyConfig
