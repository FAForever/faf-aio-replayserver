Architecture
============

General
-------

Two most important entities in the replay server are the Connection and the
Replay. The Connection represents someone connecting to the server - either to
send us a replay stream from a game in progress, or to receive one. The Replay
is the game itself - streams from Connections are merged by the Replay and the
merged stream is sent from it, and a finished Replay is saved.

Various functions revolving around these two entities - creating a Connection,
figuring out its type and replay ID, creating Replays, merging / sending
streams, saving the replay etc. - are provided or delegated to other classes.

Connection lifetime
-------------------

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
---------------

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

Top-down code overview
----------------------

Server
^^^^^^

The server as a whole is a blackbox with 3 external dependencies - connection
producer, database and saved replay directory. The server accepts connections
from the producer and does things with them, saves replay files on the disk and
reads from / writes to the database as required.

Server has start and stop methods. Start method prepares the database to run
and activates the connection producer. Stop method stops the connection
producer, closes all connections and finalizes all replays, then closes the
database.

Actual handling of connections and replays is delegated to Connections and
Replays classes. Since the latter needs a Bookkeeper for saving replays, we keep
it in the Server as well.

Connections
^^^^^^^^^^^

Connections manage connection lifetime, perform initial communication and final
closing of a connection. They also allow to close all active connections and
wait for all connections to finish. Most connection handling is handed off to
Replays.

Replays
^^^^^^^

Replays manage replay lifetime and creation. They also accept Connections,
creating associated Replay as appropriate. After figuring out which Replay a
Connection belongs to, the Connection is handed off to the Replay. The
Bookkeeper is handed to each Replay so it can save its data when appropriate.

Replays allow to close all active replays and wait for all replays to finish.

Bookkeeper
^^^^^^^^^^

There's a single Bookkeeper created by the Server. When provided with a game id
and a replay stream, it uses the Database to save the Replay on the disk.
