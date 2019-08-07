# Conveyor C++ library : Efficient event cache for osquery

The library was designed as a standalone replacement for the key-value storage scheme used for events in [osquery](https://github.com/osquery/osquery).  The actual format and encoding of the records is left to the application, the library treats all records as opaque std::string data.
Data is stored in a simple first-in-first-out (FIFO) scenario, with separate read and write 'cursors'.  Application can choose to read the stored records multiple times before advancing the read cursor and effectively 'discarding' the read records.  When all records in a file have been discarded, the file gets deleted from disk.

## IDs and Timestamps
As records are added, they are associated with an ID and a timestamp.  The timestamp is passed in by the application in the `addRecord()` and `addBatch()` calls.  The first record id will start at 0, and increase sequentially.  The id datatype is uint32_t, and will eventually wrap back around to zero (0) after 4 billion or so records.

One of the settings is **expirySeconds**.  If expirySeconds have expired between the timestamp provided to enumerateRecords() and the timestamp associated with the stored record, then the record is ignored, and the application does not get an onRecord() callback for it.

## Read Cursors
To read the records, you must open a cursor and pass it to `enumerateRecords()`.
By default, calling enumerateRecords() will not advance a cursor ...  the application must call advanceCursor(), or have created the cursor with autoAdvance=true.  This design allows for multiple-passes on the set of unread data.
```
auto cursor = conveyor->openCursor();
conveyor->enumerateRecords(listener, nullptr, cursor, ts);
conveyor->advanceCursor(cursor);
```

## Multiple Independent Cursors
There is support for multiple independent cursors.  A record is only considered discarded when all active cursors have advanced past it.
Consider an application that reads new data every 60 seconds for one purpose, and reads new data every 5-minutes for another purpose.  Multiple cursors allows the 60-second task to read new data each time, advancing it's cursor, without affecting other cursors.  The 5-minute task will read the data 5 times less often, but they all see the same records.  Once both cursors have advanced, then the records are considered discarded, making more write space available.

## Drops
All real-world applications need to deal with performance edge cases.  One of the central design ideas for this library is to have a hard drop if the configured limits are reached.  Normally this is when `maxRecords` has been reached, but it can also be less.  The return value of addRecord() will be 1 when drop occurs.  The application can call `getNumDrops()` to receive the number of total drops.

The worst case is `(numRecords - (numRecords / numChunks) - 1)`, where there is one available record left in a file where the rest are discarded.  The other files full of available records.  The writer has to wait until the application has read that last available record before it can delete the file and start a fresh one to add more records.

## Disk Layout
The library stores records across multiple files in round-robin fashion, based on the application settings.  
If we choose simple settings like
`name="gustavo", MaxRecords=15 , numChunks=3` we would see data written across 3 files, with up to 5 records per file.
If we have read records id=0 and id=1, 10 available records, and the next record to be written is id=12 the files on disk would look like the following.

**Legend: '#' : available record, '.' : discarded record .**  *The discarded records aren't actually modified on disk, it's all based on runtime state.*
```
_FR_gustavo_0 [..###]
_FR_gustavo_1 [#####]
_FR_gustavo_2 [##]
```
After a call to enumerateRecords() and advanceCursor() , it would look like the following.  If all records in a file have been read, then the file is deleted.
```
_FR_gustavo_2 [..]
```
After writing 4 more records, the layout will be the following.  Notice that the newest file has rolled around back to \_0 suffix:
```
_FR_gustavo_2 [..###]
_FR_gustavo_0 [#]
```

## Persisted State
The application can optionally persist the state by calling `persistState()`.  If the application exits and runs at a later time, it can call `loadPersistedState()` to resume using the available records previously written to disk.  The data is stored in a file with suffix ".state" .

## Dependencies

- C++11
- boost-filesystem (needs boost-system)
- gtest (tests only)

## Building Tests
Define an environment variable named DEPDIR that has dependencies installed in ./include and ./lib subdirectories.  I build them using [mason](https://github.com/mapbox/mason) install and link .
```
DEPDIR=/path/to/mason_packages/.link cmake ..
```
