FAF replay server
=================

This is a prototype for the new FAF replay server. It uses asyncio a lot and
hopefully is better designed than the old replay server.

General requirements
====================

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
  malformed, so we should merge replays we receive into a single 'canonical
  replay' for the game, and use that for streaming and storage in the vault.
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
* Abilitiy to 'force' an end to a replay if it was taking way too long (e.g. 5
  hours).


General architecture
====================

Two most important entities in the replay server are the Connection and the
Replay. The Connection represents someone connecting to the server - either to
send us a replay stream from a game in progress, or to receive one. The Replay
is the game itself - streams from Connections are merged by the Replay and the
merged stream is sent from it, and a finished Replay is saved.

Various functions revolving around these two entities - creating a Connection,
figuring out its type and replay ID, creating Replays, merging / sending
streams, saving the replay etc. - are provided or delegated to other classes.


Connection lifetime
===================

General connection lifetime is as follows:

1. Connection is made. A small header is read that indicates which game ID the
   Connection deals with, and whether the Connection reads or writes data.
2. The Connection is handed over to its Replay. If applicable, the Replay for
   the Connection is created, or Connection is aborted if it can't be assigned
   to one.
3. Depending on its type, the Connection is handed over to be read from
   (WRITER), or to be written to (READER).
4. For a WRITER Connection, a replay header is read, then all Connection's data
   is read in a loop. If the header is invalid, Connection is aborted. The read
   data is merged into a canonical replay stream.
5. For a READER Connection, it is fed a header and data from a delayed canonical
   replay stream until the stream is over.
6. Connection is closed, at any earlier point - either because of errors, or
   because the server is closing.


Replay lifetime
===============

In order to decide when to accept connections and when to save replay data, we
need to describe lifetime of a Replay.

1. The Replay is created when the first WRITER Connection for a given game ID is
   made. Not every new Connection might create a replay though - we might, for
   example, decide that we won't create a replay for an ID that already exists.
   Along with the first WRITER connection, Replay's write phase begins.
2. As long as there are WRITER Connections matching a Replay, its write phase
   continues. The write phase ends when there have been no WRITER Connections
   matching a Replay for some time (e.g. a grace period of 30 seconds).
3. Both READER and WRITER connections are accepted only during the write phase.
4. Once the write phase ends, we are done streaming the replay. Replay data is
   saved.
5. After the write phase is complete and replay data is saved, the Replay waits
   for streaming data to all remaining readers to end.
6. Once there are no readers remaining, the Replay is complete.
7. If a Replay has been running for longer than some time (e.g. 5 hours), it
   ensures that its write phase ends quickly and all readers are dropped.
