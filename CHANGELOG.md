1.0.2 (03.09.2019)
------------------

Fixes:

* Bump version.

1.0.1 (03.09.2019)
------------------
Fixes:

* Fix implementation of slice in DelayedStream.
* Discard more data from incoming streams while merging.
* Tentative fix for a rare bug where the last 5 minutes of the stream aren't
  sent to a listener.
