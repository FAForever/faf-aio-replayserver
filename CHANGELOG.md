1.0.7 (17.01.2020)
------------------

* Small logging fixes.
* We now save replays even if there are no players for it in the db.

1.0.6 (16.10.2019)
------------------

* Fix uncaught TimeoutError when awaiting connection end.
* Add some missing f-string modifiers.
* Fix an unawaited coroutine.

1.0.5 (22.09.2019)
------------------

* Fix not catching `wait_closed` exceptions.

1.0.4 (22.09.2019)
------------------

* Move to python3.7 to take advantage of `wait_closed` for streams.
* Use `wait_closed` to (hopefully) fix the issue with short live replays.

1.0.3 (16.09.2019)
------------------

Fixes:

* Fix connection metrics not covering connection closing. Now we don't
  decrement the metric until the connection is done for good.

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
