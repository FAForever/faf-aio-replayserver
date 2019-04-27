Requirements
============

As players on FAF play games, other players would like to watch them in real
time, or from the replay vault after they are finished. This is a feature that
existed back in GPG times, and the game itself has facilities for that - at the
start the game can be instructed to send the game replay as it progresses to a
specific port, or read the replay from a port.

In general, the replay server is supposed to:

* read the replay streams from players that play the game,

* send these streams to players who want to watch a game,

* save the replay once the game is over.

There are several important details regarding the above:

* Each game has several replay streams sent to us from multiple players. These
  streams might end unexpectedly, or start differing between each other. We
  don't want a replay watcher to have to guess which replay isn't short or
  malformed, neither do we want to pick one of the streams to save to hard disk
  at random. To deal with that, we should merge replays we receive into a
  single 'canonical replay' for the game, and use that for streaming and
  storage in the vault.

* Better yet, replays that end early routinely differ on a last few hundred
  bytes. Merging replays should be able to deal with that.

* To prevent cheating, the replay sent to live watchers should be delayed by
  some time (e.g. 5 minutes).

* We should support all the legacy protocols and formats (initial communication,
  replay header, saved replay format, DB queries etc.).

As well as all kinds of special conditions we need to watch out for, e.g.:

* Usual issues with malformed connection data, aborted connections, DB and
  filesystem errors etc.

* Connection shenanigans, like a 'replay watcher' type connection arriving while
  nobody is streaming a given replay, or 'replay sender' type connections
  arriving in an unexpected order,

* Connection stalling - it's not uncommon for connection from the game to never
  get closed on the other end, but at the same time there are valid reasons for
  connections to resume after dozens of minutes,

* Ability to 'force' an end to a replay if it was taking way too long (e.g. 5
  hours).
