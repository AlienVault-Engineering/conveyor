#include <gtest/gtest.h>
#include <string>

#include "../include/conveyor.h"

class ConveyorSimple : public ::testing::Test {
protected:
  virtual void SetUp() override {
  }
  virtual void TearDown() override {
  }

};

// rec1 is 26 bytes, rec2 is 284 bytes
const std::string rec1 = "abcdefghijklmnopqrstuvwxyz";
const std::string rec2 = "{  \"action\": \"added\",  \"columns\": {    \"name\": \"osqueryd\",    \"path\": \"/usr/local/bin/osqueryd\",    \"pid\": \"97830\"  },  \"name\": \"processes\",  \"hostname\": \"hostname.local\",  \"calendarTime\": \"Tue Sep 30 17:37:30 2014\",  \"unixTime\": \"1412123850\",  \"epoch\": \"314159265\",  \"counter\": \"1\"}";

const ConveyorSettings gSettings1 = { ".", "test_events", 3, 12, 1, 8192 };

struct TestConveyorListener : public ConveyorListener {
  void onRecord(void *context, const std::string &value, std::time_t ts, uint32_t id) override {
    numBytes += value.size();
    numRecords++;
  }
  uint64_t numBytes {0};
  uint64_t numRecords {0};
};

//--------------------------------------------------------------
// fill to limit + 1, should have a drop
//--------------------------------------------------------------
TEST_F(ConveyorSimple, fill_and_drop) {
  int rv = 0;

  auto settings = gSettings1;
  std::time_t ts = time(NULL);
  SPConveyor conveyor = ConveyorNew(settings);
  conveyor->deleteAndStartFresh();

  for (int i=0; i < settings.maxRecords; i++) {
    rv = conveyor->addRecord(i % 2 == 0 ? rec2 : rec1, ts);
    ASSERT_EQ(0, rv);
  }
  rv = conveyor->addRecord(rec1, ts);
  ASSERT_EQ(1, rv); // drop

  ASSERT_EQ(1, conveyor->getNumDrops());
}

//--------------------------------------------------------------
// Fill and read back
//--------------------------------------------------------------
TEST_F(ConveyorSimple, fill_and_read) {
  int rv = 0;

  auto settings = gSettings1;
  std::time_t ts = time(NULL);
  SPConveyor conveyor = ConveyorNew(settings);
  conveyor->deleteAndStartFresh();

  int expBytes = 0;
  for (int i=0; i < settings.maxRecords; i++) {
    rv = conveyor->addRecord(i % 2 == 0 ? rec2 : rec1, ts);
    expBytes += (i % 2 == 0 ? rec2 : rec1).size();
    ASSERT_EQ(0, rv);
  }

  TestConveyorListener listener;
  auto cursor = conveyor->openCursor();

  conveyor->enumerateRecords(listener, nullptr, cursor, ts);

  EXPECT_EQ(settings.maxRecords, listener.numRecords);
  EXPECT_EQ(expBytes, listener.numBytes);

  EXPECT_EQ(settings.maxRecords, conveyor->getNumRecords());
  EXPECT_EQ(0, conveyor->getNumDrops());
}

//--------------------------------------------------------------
// Once a (non-autoAdvance) cursor has enumerateRecords(), it will have its
// end position set.  Consecutive reads should always stop at the same record,
// even if new records are added between reads.  Calling advanceCursor() will
// clear the cursor's end position so that new records can be read.
//--------------------------------------------------------------
TEST_F(ConveyorSimple, consec_reads_same_stop) {
  int rv = 0;

  auto settings = gSettings1;
  std::time_t ts = time(NULL);
  SPConveyor conveyor = ConveyorNew(settings);
  conveyor->deleteAndStartFresh();

  int expBytes = 0;
  for (int i=0; i < settings.maxRecords / 2; i++) {
    rv = conveyor->addRecord(i % 2 == 0 ? rec2 : rec1, ts);
    expBytes += (i % 2 == 0 ? rec2 : rec1).size();
    ASSERT_EQ(0, rv);
  }
  EXPECT_EQ(settings.maxRecords / 2, conveyor->getNumRecords());

  TestConveyorListener listener;
  auto cursor = conveyor->openCursor();

  conveyor->enumerateRecords(listener, nullptr, cursor, ts);

  EXPECT_EQ(settings.maxRecords / 2, listener.numRecords);
  EXPECT_EQ(expBytes, listener.numBytes);

  // add 2 more records

  rv = conveyor->addRecord(rec2, ts);
  ASSERT_EQ(0, rv);
  rv = conveyor->addRecord(rec2, ts);
  ASSERT_EQ(0, rv);

  EXPECT_EQ(settings.maxRecords / 2 + 2, conveyor->getNumRecords());

  // read again.  should not read the two new ones
  listener = TestConveyorListener();
  conveyor->enumerateRecords(listener, nullptr, cursor, ts);

  EXPECT_EQ(settings.maxRecords / 2, listener.numRecords);

  conveyor->advanceCursor(cursor);
  EXPECT_EQ(2, conveyor->getNumRecords());

  // now we should pick up the last two

  listener = TestConveyorListener();
  conveyor->enumerateRecords(listener, nullptr, cursor, ts);

  EXPECT_EQ(2, listener.numRecords);

  conveyor->enumerateRecords(listener, nullptr, cursor, ts);

  EXPECT_EQ(4, listener.numRecords);

  // advance again, should be empty
  conveyor->advanceCursor(cursor);

  EXPECT_EQ(0, conveyor->getNumRecords());
}

//--------------------------------------------------------------
// fill, read, advance,
//--------------------------------------------------------------
TEST_F(ConveyorSimple, fill_and_read_advance_cursor) {
  int rv = 0;

  auto settings = gSettings1;
  std::time_t ts = time(NULL);
  SPConveyor conveyor = ConveyorNew(settings);
  conveyor->deleteAndStartFresh();

  int expBytes = 0;
  for (int i=0; i < settings.maxRecords; i++) {
    rv = conveyor->addRecord(i % 2 == 0 ? rec2 : rec1, ts);
    expBytes += (i % 2 == 0 ? rec2 : rec1).size();
    ASSERT_EQ(0, rv);
  }

  TestConveyorListener listener;
  auto cursor = conveyor->openCursor();

  // read

  conveyor->enumerateRecords(listener, nullptr, cursor, ts);
  EXPECT_EQ(settings.maxRecords, conveyor->getNumRecords());
  EXPECT_EQ(settings.maxRecords, listener.numRecords);
  EXPECT_EQ(expBytes, listener.numBytes);

  // advance. should discard all records

  conveyor->advanceCursor(cursor);
  EXPECT_EQ(0, conveyor->getNumRecords());

  // without any new records added, listener stats should be same

  listener = TestConveyorListener();
  conveyor->enumerateRecords(listener, nullptr, cursor, ts);
  conveyor->closeCursor(cursor);

  EXPECT_EQ(0, listener.numRecords);
}


//--------------------------------------------------------------
// test state
//--------------------------------------------------------------
TEST_F(ConveyorSimple, fill_check_state) {
  int rv = 0;

  auto settings = gSettings1;
  std::time_t ts = time(NULL) - 10;
  SPConveyor conveyor = ConveyorNew(settings);
  conveyor->deleteAndStartFresh();

  int expBytes = 0;
  for (int i=0; i < settings.maxRecords; i++) {
    rv = conveyor->addRecord(i % 2 == 0 ? rec2 : rec1, ts + i);
    expBytes += (i % 2 == 0 ? rec2 : rec1).size();
    ASSERT_EQ(0, rv);
  }

  std::vector<ConveyorFile> state = conveyor->testGetFileState();
  ASSERT_EQ(settings.numChunks, state.size());
  EXPECT_EQ(0, state[0].startId);
  EXPECT_EQ(4, state[1].startId);
  EXPECT_EQ(8, state[2].startId);
  EXPECT_EQ(4, state[0].numRecords);
  EXPECT_EQ(4, state[1].numRecords);
  EXPECT_EQ(4, state[2].numRecords);
  EXPECT_EQ(true, state[0].isActive);
  EXPECT_EQ(true, state[1].isActive);
  EXPECT_EQ(true, state[2].isActive);
}

//--------------------------------------------------------------
// fill, advance to discard, fill again
//--------------------------------------------------------------
TEST_F(ConveyorSimple, fill_twice) {
  int rv = 0;

  auto settings = gSettings1;
  std::time_t ts = time(NULL);
  SPConveyor conveyor = ConveyorNew(settings);
  conveyor->deleteAndStartFresh();

  // fill

  int expBytes = 0;
  for (int i=0; i < settings.maxRecords; i++) {
    rv = conveyor->addRecord(i % 2 == 0 ? rec2 : rec1, ts);
    expBytes += (i % 2 == 0 ? rec2 : rec1).size();
    ASSERT_EQ(0, rv);
  }
  EXPECT_EQ(settings.maxRecords, conveyor->getNumRecords());

  TestConveyorListener listener;

  // read, advancing cursor

  auto cursor = conveyor->openCursor();
  conveyor->enumerateRecords(listener, nullptr, cursor, ts);
  conveyor->advanceCursor(cursor);

  EXPECT_EQ(0, conveyor->getNumRecords());

  // fill again

  for (int i=0; i < settings.maxRecords; i++) {
    rv = conveyor->addRecord(i % 2 == 0 ? rec2 : rec1, ts);
    ASSERT_EQ(0, rv);
  }
  EXPECT_EQ(settings.maxRecords, conveyor->getNumRecords());

  // read again

  conveyor->enumerateRecords(listener, nullptr, cursor, ts);
  conveyor->advanceCursor(cursor);

  EXPECT_EQ(0, conveyor->getNumRecords());
  EXPECT_EQ(settings.maxRecords*2, listener.numRecords);

}

//--------------------------------------------------------------
// Using autoAdvance=true cursor, add and read one-by-one.
//--------------------------------------------------------------
TEST_F(ConveyorSimple, one_by_one) {
  int rv = 0;

  TestConveyorListener listener;
  auto settings = gSettings1;
  std::time_t ts = time(NULL);
  SPConveyor conveyor = ConveyorNew(settings);
  conveyor->deleteAndStartFresh();

  auto cursor = conveyor->openCursor(true);
  int expBytes = 0;
  for (int i=0; i < settings.maxRecords * 2; i++) {
    rv = conveyor->addRecord(i % 2 == 0 ? rec2 : rec1, ts);
    ASSERT_EQ(0, rv);
    expBytes += (i % 2 == 0 ? rec2 : rec1).size();
    EXPECT_EQ(1, conveyor->getNumRecords());
    conveyor->enumerateRecords(listener, nullptr, cursor, ts);
    EXPECT_EQ(0, conveyor->getNumRecords());
  }
  EXPECT_EQ(0, conveyor->getNumRecords());

  EXPECT_EQ(settings.maxRecords*2, listener.numRecords);
  EXPECT_EQ(expBytes, listener.numBytes);

}

//--------------------------------------------------------------
// Tests the situation where the uint32 id rollsover back to 0.
//--------------------------------------------------------------
TEST_F(ConveyorSimple, id_rollover) {
  int rv = 0;

  TestConveyorListener listener;
  auto settings = gSettings1;
  std::time_t ts = time(NULL);
  SPConveyor conveyor = ConveyorNew(settings);
  conveyor->testResetAtId((uint32_t)-6);

  auto cursor = conveyor->openCursor(/* autoAdvance= */true);
  int expBytes = 0;
  for (int i=0; i < settings.maxRecords; i++) {
    rv = conveyor->addRecord(i % 2 == 0 ? rec2 : rec1, ts);
    ASSERT_EQ(0, rv);
    expBytes += (i % 2 == 0 ? rec2 : rec1).size();
    EXPECT_EQ(1, conveyor->getNumRecords());
    conveyor->enumerateRecords(listener, nullptr, cursor, ts);
    EXPECT_EQ(0, conveyor->getNumRecords());
  }
  EXPECT_EQ(0, conveyor->getNumRecords());

  EXPECT_EQ(settings.maxRecords, listener.numRecords);
  EXPECT_EQ(expBytes, listener.numBytes);
}

//--------------------------------------------------------------
// Test filling with batch, then reading it back.
//--------------------------------------------------------------
TEST_F(ConveyorSimple, batch_fill_and_read) {
  int rv = 0;

  auto settings = gSettings1;
  std::time_t ts = time(NULL);
  SPConveyor conveyor = ConveyorNew(settings);
  conveyor->deleteAndStartFresh();

  int expBytes = 0;
  std::vector<std::string> batch;
  for (int i=0; i < settings.maxRecords; i++) {
    batch.push_back(i % 2 == 0 ? rec2 : rec1);
    expBytes += (i % 2 == 0 ? rec2 : rec1).size();
  }
  rv = conveyor->addBatch(batch, ts);
  ASSERT_EQ(0,rv);

  TestConveyorListener listener;
  auto cursor = conveyor->openCursor();

  conveyor->enumerateRecords(listener, nullptr, cursor, ts);

  EXPECT_EQ(settings.maxRecords, listener.numRecords);
  EXPECT_EQ(expBytes, listener.numBytes);
}

//--------------------------------------------------------------
// Multiple calls to addBatch(), testing the drop count.
// batches can have partial success, partial drops
//--------------------------------------------------------------
TEST_F(ConveyorSimple, batch_fill_drop_and_read) {
  int rv = 0;

  auto settings = gSettings1;
  std::time_t ts = time(NULL);
  SPConveyor conveyor = ConveyorNew(settings);
  conveyor->deleteAndStartFresh();

  std::vector<std::string> batch = {"A","B","C","D","E"};

  rv = conveyor->addBatch(batch, ts);
  ASSERT_EQ(0,rv);
  ASSERT_EQ(5,conveyor->getNumRecords());

  rv = conveyor->addBatch(batch, ts);
  ASSERT_EQ(0,rv);
  ASSERT_EQ(10,conveyor->getNumRecords());
  ASSERT_EQ(0,conveyor->getNumDrops());

  // if rv > 0, it's the number of drops
  rv = conveyor->addBatch(batch, ts);
  int expectedDrops = 3;
  ASSERT_EQ(expectedDrops,rv);
  ASSERT_EQ(12,conveyor->getNumRecords());
  ASSERT_EQ(expectedDrops,conveyor->getNumDrops());

  rv = conveyor->addBatch(batch, ts);
  expectedDrops = 5;
  ASSERT_EQ(expectedDrops,rv);
  ASSERT_EQ(12,conveyor->getNumRecords());
  ASSERT_EQ(3 + 5,conveyor->getNumDrops());

  TestConveyorListener listener;
  auto cursor = conveyor->openCursor();

  conveyor->enumerateRecords(listener, nullptr, cursor, ts);

  EXPECT_EQ(settings.maxRecords, listener.numRecords);
  EXPECT_EQ(12, listener.numBytes);
}

//--------------------------------------------------------------
// Test that expired records get discarded as they are read,
// even if cursor is not autoAdvance.
//--------------------------------------------------------------
TEST_F(ConveyorSimple, test_expiry) {
  int rv = 0;

  auto settings = gSettings1;
  settings.expirySeconds = 3;
  std::time_t ts = time(NULL);
  SPConveyor conveyor = ConveyorNew(settings);
  conveyor->deleteAndStartFresh();

  rv = conveyor->addRecord(rec1, ts-3);
  rv = conveyor->addRecord(rec2, ts-3);

  rv = conveyor->addRecord(rec1, ts-2);
  rv = conveyor->addRecord(rec2, ts-2);

  rv = conveyor->addRecord(rec1, ts);
  rv = conveyor->addRecord(rec2, ts);

  TestConveyorListener listener;

  // this is not an autoAdvance cursor, but as records are expired,
  // the cursor will still be advanced and expired records discarded.

  auto cursor = conveyor->openCursor();
  size_t numSkipped=0, numExpired=0, numNotified = 0;

  EXPECT_EQ(6, conveyor->getNumRecords());

  conveyor->enumerateRecords(listener, nullptr, cursor, ts-3);
  EXPECT_EQ(6, listener.numRecords);
  EXPECT_EQ(6, conveyor->getNumRecords());

  listener = TestConveyorListener();
  conveyor->enumerateRecords(listener, nullptr, cursor, ts-2);
  EXPECT_EQ(6, listener.numRecords);
  EXPECT_EQ(6, conveyor->getNumRecords());

  listener = TestConveyorListener();
  conveyor->enumerateRecords(listener, nullptr, cursor, ts-1);
  EXPECT_EQ(6, listener.numRecords);
  EXPECT_EQ(6, conveyor->getNumRecords());
  conveyor->testGetReadCounters(numSkipped, numExpired, numNotified);
  EXPECT_EQ(0,numExpired);

  // the next reads should start expiring records

  listener = TestConveyorListener();
  conveyor->enumerateRecords(listener, nullptr, cursor, ts);
  EXPECT_EQ(4, listener.numRecords);
  EXPECT_EQ(4, conveyor->getNumRecords());
  conveyor->testGetReadCounters(numSkipped, numExpired, numNotified);
  EXPECT_EQ(2,numExpired);

  listener = TestConveyorListener();
  conveyor->enumerateRecords(listener, nullptr, cursor, ts+1);
  EXPECT_EQ(2, listener.numRecords);
  EXPECT_EQ(2, conveyor->getNumRecords());
  conveyor->testGetReadCounters(numSkipped, numExpired, numNotified);
  EXPECT_EQ(2,numExpired);

  listener = TestConveyorListener();
  conveyor->enumerateRecords(listener, nullptr, cursor, ts+2);
  EXPECT_EQ(2, listener.numRecords);
  EXPECT_EQ(2, conveyor->getNumRecords());

  listener = TestConveyorListener();
  conveyor->enumerateRecords(listener, nullptr, cursor, ts+3);
  EXPECT_EQ(0, listener.numRecords);
  EXPECT_EQ(0, conveyor->getNumRecords());
}

//--------------------------------------------------------------
// Test loading of persisted state.
//--------------------------------------------------------------
TEST_F(ConveyorSimple, fill_and_read_state) {
  int rv = 0;

  auto settings = gSettings1;
  std::time_t ts = time(NULL);
  SPConveyor conveyor = ConveyorNew(settings);
  conveyor->deleteAndStartFresh();

  int expBytes = 0;
  for (int i=0; i < settings.maxRecords; i++) {
    rv = conveyor->addRecord(i % 2 == 0 ? rec2 : rec1, ts);
    expBytes += (i % 2 == 0 ? rec2 : rec1).size();
    ASSERT_EQ(0, rv);
  }

  conveyor->persistState();

  TestConveyorListener listener;
  conveyor = ConveyorNew(settings);  // allocate new one, must read state from file
  auto cursor = conveyor->openCursor();

  conveyor->loadPersistedState();

  conveyor->enumerateRecords(listener, nullptr, cursor, ts);

  EXPECT_EQ(settings.maxRecords, listener.numRecords);
  EXPECT_EQ(expBytes, listener.numBytes);

  conveyor->enumerateRecords(listener, nullptr, cursor, ts);

  EXPECT_EQ(settings.maxRecords*2, listener.numRecords);
  EXPECT_EQ(expBytes*2, listener.numBytes);
}

//--------------------------------------------------------------
// Load persisted state, where the read cursor is not at the
// start of a file.
//--------------------------------------------------------------
TEST_F(ConveyorSimple, fill_and_read_state_offset) {
  int rv = 0;

  auto settings = gSettings1;
  std::time_t ts = time(NULL);
  SPConveyor conveyor = ConveyorNew(settings);
  conveyor->deleteAndStartFresh();

  // first write 2 records and read them

  rv = conveyor->addRecord(rec1, ts);
  rv = conveyor->addRecord(rec2, ts);

  TestConveyorListener listener;
  auto cursor = conveyor->openCursor();

  EXPECT_EQ(2,conveyor->getNumRecords());
  conveyor->enumerateRecords(listener, nullptr, cursor, ts);

  // advance will clear the two records

  conveyor->advanceCursor(cursor);
  EXPECT_EQ(0,conveyor->getNumRecords());
  EXPECT_EQ(2, listener.numRecords);

  // now write 10 records

  int expBytes = 0;
  for (int i=2; i < settings.maxRecords; i++) {
    rv = conveyor->addRecord(i % 2 == 0 ? rec2 : rec1, ts);
    expBytes += (i % 2 == 0 ? rec2 : rec1).size();
    ASSERT_EQ(0, rv);
  }

  EXPECT_EQ(settings.maxRecords-2,conveyor->getNumRecords());

  // save state

  conveyor->persistState();

  // make new instance, load state

  conveyor = ConveyorNew(settings);  // allocate new one, must read state from file
  conveyor->loadPersistedState();

  EXPECT_EQ(settings.maxRecords-2,conveyor->getNumRecords());

  cursor = conveyor->openCursor();
  listener = TestConveyorListener();
  conveyor->enumerateRecords(listener, nullptr, cursor, ts);

  EXPECT_EQ(settings.maxRecords - 2, listener.numRecords);
  EXPECT_EQ(expBytes, listener.numBytes);

  // At this point, all files are waiting to be read.
  // Even though 2 records from file 0 are 'discarded'.
  // so we will drop even though numRecords < maxRecords

  rv = conveyor->addRecord(rec1, ts);
  EXPECT_EQ(1,rv);

  // now read

  conveyor->enumerateRecords(listener, nullptr, cursor, ts);
  conveyor->advanceCursor(cursor);

  // can write again

  rv = conveyor->addRecord(rec2, ts);
  EXPECT_EQ(0,rv);
  rv = conveyor->addRecord(rec2, ts);
  EXPECT_EQ(0,rv);
  EXPECT_EQ(2,conveyor->getNumRecords());
}


//--------------------------------------------------------------
// consecutive reads should seek to last cursor offset,
// rather than reading entire file.
//--------------------------------------------------------------
TEST_F(ConveyorSimple, ensure_read_seek) {
  int rv = 0;

  size_t numSkipped=0, numExpired=0, numNotified = 0;
  auto settings = gSettings1;
  settings.maxRecords = 100;
  settings.numChunks = 2;
  std::time_t ts = time(NULL);
  SPConveyor conveyor = ConveyorNew(settings);
  conveyor->deleteAndStartFresh();

  for (int i=0; i < 75; i++) {    rv = conveyor->addRecord(rec2, ts); }
  EXPECT_EQ(75,conveyor->getNumRecords());

  TestConveyorListener listener;
  conveyor->testGetReadCounters(numSkipped, numExpired, numNotified);
  EXPECT_EQ(0,numSkipped);
  EXPECT_EQ(0,numExpired);
  EXPECT_EQ(0,numNotified);

  auto cursor = conveyor->openCursor();

  conveyor->enumerateRecords(listener, nullptr, cursor, ts);
  conveyor->advanceCursor(cursor);
  conveyor->testGetReadCounters(numSkipped, numExpired, numNotified);
  EXPECT_EQ(0,conveyor->getNumRecords());
  EXPECT_EQ(0,numSkipped);
  EXPECT_EQ(0,numExpired);
  EXPECT_EQ(75,numNotified);

  for (int i=0; i < 25; i++) {    rv = conveyor->addRecord(rec2, ts); }
  EXPECT_EQ(25,conveyor->getNumRecords());

  listener = TestConveyorListener();
  conveyor->enumerateRecords(listener, nullptr, cursor, ts);
  EXPECT_EQ(25,conveyor->getNumRecords());
  conveyor->testGetReadCounters(numSkipped, numExpired, numNotified);
  EXPECT_EQ(0,numSkipped);
  EXPECT_EQ(0,numExpired);
  EXPECT_EQ(25,numNotified);

  conveyor->enumerateRecords(listener, nullptr, cursor, ts);
  conveyor->advanceCursor(cursor);
  EXPECT_EQ(0,conveyor->getNumRecords());
  conveyor->testGetReadCounters(numSkipped, numExpired, numNotified);
  EXPECT_EQ(0,numSkipped);
  EXPECT_EQ(0,numExpired);
  EXPECT_EQ(25,numNotified);
}

//--------------------------------------------------------------
// test 32KB record size
//--------------------------------------------------------------
TEST_F(ConveyorSimple, fill_large) {
  int rv = 0;

  auto settings = gSettings1;
  settings.maxRecordSize = 32*1024;
  std::time_t ts = time(NULL);
  SPConveyor conveyor = ConveyorNew(settings);
  conveyor->deleteAndStartFresh();

  std::string tmp;
  tmp.reserve(settings.maxRecordSize);
  tmp.resize(settings.maxRecordSize);

  // add 5 records

  rv = conveyor->addRecord(tmp, ts);
  ASSERT_EQ(0, rv);
  rv = conveyor->addRecord(tmp, ts);
  ASSERT_EQ(0, rv);
  rv = conveyor->addRecord(tmp, ts);
  ASSERT_EQ(0, rv);
  rv = conveyor->addRecord(tmp, ts);
  ASSERT_EQ(0, rv);
  rv = conveyor->addRecord(tmp, ts);
  ASSERT_EQ(0, rv);

  auto listener = TestConveyorListener();
  auto cursor = conveyor->openCursor();

  conveyor->enumerateRecords(listener, nullptr, cursor, ts);
  EXPECT_EQ(5, conveyor->getNumRecords());
  EXPECT_EQ(5, listener.numRecords);
  EXPECT_EQ(settings.maxRecordSize * 5, listener.numBytes);

  conveyor->advanceCursor(cursor);
  EXPECT_EQ(0, conveyor->getNumRecords());
}

//--------------------------------------------------------------
// Multiple cursors
//--------------------------------------------------------------
TEST_F(ConveyorSimple, multiple_cursors) {
  int rv = 0;

  auto settings = gSettings1;
  std::time_t ts = time(NULL);
  SPConveyor conveyor = ConveyorNew(settings);
  conveyor->deleteAndStartFresh();

  std::vector<std::string> batch = {"A","B","C"};

  rv = conveyor->addBatch(batch, ts);
  ASSERT_EQ(0,rv);
  ASSERT_EQ(3,conveyor->getNumRecords());

  auto cursor1 = conveyor->openCursor();
  auto cursor2 = conveyor->openCursor();
  TestConveyorListener listener1;
  TestConveyorListener listener2;

  conveyor->enumerateRecords(listener1, nullptr, cursor1, ts);
  conveyor->advanceCursor(cursor1);

  EXPECT_EQ(3, listener1.numRecords);

  rv = conveyor->addBatch(batch, ts);
  ASSERT_EQ(6,conveyor->getNumRecords());

  conveyor->enumerateRecords(listener2, nullptr, cursor2, ts);
  conveyor->advanceCursor(cursor2);

  ASSERT_EQ(3,conveyor->getNumRecords());

  EXPECT_EQ(3, listener1.numRecords);
  EXPECT_EQ(6, listener2.numRecords);

}

//--------------------------------------------------------------
// loop of partial-fill, read+advance
//--------------------------------------------------------------
TEST_F(ConveyorSimple, many_loops_no_drops) {
  int rv = 0;
  int num_loops = 100;

  auto settings = gSettings1;
  std::time_t ts = time(NULL);
  SPConveyor conveyor = ConveyorNew(settings);
  conveyor->deleteAndStartFresh();
  auto cursor = conveyor->openCursor(true);
  TestConveyorListener listener;
  auto num_records_per_file = (settings.maxRecords / settings.numChunks);
  auto num_records_n_minus_one_files = (settings.numChunks - 1) * num_records_per_file;

  for (int j=0; j < num_loops; j++) {

    // limit max fill to num_records_per_file, since it's possible to
    // have less than settings.maxRecords available

    int num_records_this_loop = j % (num_records_n_minus_one_files) + 1;

    for (int i=0; i < num_records_this_loop; i++) {
      rv = conveyor->addRecord(i % 2 == 0 ? rec2 : rec1, ts);
      ASSERT_EQ(0, rv);
    }
    EXPECT_EQ(num_records_this_loop, conveyor->getNumRecords());


    // read, advancing cursor

    conveyor->enumerateRecords(listener, nullptr, cursor, ts);
    conveyor->advanceCursor(cursor);

    EXPECT_EQ(0, conveyor->getNumRecords());
  }

}

//--------------------------------------------------------------
// loop of partial-fill, read+advance
//--------------------------------------------------------------
TEST_F(ConveyorSimple, many_loops) {
  int rv = 0;
  int num_loops = 100;

  auto settings = gSettings1;
  std::time_t ts = time(NULL);
  SPConveyor conveyor = ConveyorNew(settings);
  conveyor->deleteAndStartFresh();
  auto cursor = conveyor->openCursor(true);
  TestConveyorListener listener;

  for (int j=0; j < num_loops; j++) {

    int num_records_this_loop = j % settings.maxRecords + 1;
    auto prevDrops = conveyor->getNumDrops();

    int num_drops = 0;
    for (int i=0; i < num_records_this_loop; i++) {
      rv = conveyor->addRecord(i % 2 == 0 ? rec2 : rec1, ts);
      ASSERT_TRUE(rv >= 0);
      if (rv > 0) num_drops += 1;
    }
    int num_records_written = num_records_this_loop - num_drops;

    EXPECT_EQ(prevDrops + num_drops, conveyor->getNumDrops());
    EXPECT_EQ(num_records_written, conveyor->getNumRecords());


    // read, advancing cursor

    conveyor->enumerateRecords(listener, nullptr, cursor, ts);
    conveyor->advanceCursor(cursor);

    EXPECT_EQ(0, conveyor->getNumRecords());
  }

}
