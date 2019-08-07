#pragma once

#include <stdint.h>

#include <ctime>
#include <memory>
#include <string>
#include <vector>

struct ConveyorPosition {
  uint32_t    fileIndex;
  uint32_t    id;
  uint64_t    offset;
  std::time_t ts;
};

struct ConveyorCursor {
  virtual ConveyorPosition getStart() = 0;
  virtual ConveyorPosition getEnd() = 0;
};

struct ConveyorSettings {
  std::string dirpath;
  std::string name;
  int         numChunks;
  uint32_t    maxRecords;
  uint32_t    expirySeconds;
  uint32_t    maxRecordSize;  // sanity check baseline
};

struct ConveyorFile {
  std::string path;
  bool        isActive;
  uint32_t    fileIndex;
  uint32_t    startId;
  std::time_t startTs;
  uint32_t    numRecords;
  bool        isEmpty() { return numRecords == 0;}
};

typedef std::shared_ptr<ConveyorCursor> SPFileCursor;

struct ConveyorListener {
  virtual void onRecord(void *context, const std::string &value, std::time_t ts, uint32_t id) = 0;
};

/*
 * Contains state on a single chunked file-buffer.
 */
struct Conveyor {

  /*
   * Persist a record.
   * @returns 0 on success, -1 on error, 1 on drop
   */
  virtual int addRecord(const std::string &value, std::time_t now) = 0;

  /*
   * Persist a batch of records.
   * @returns 0 on success, -1 on error, or number of dropped
   */
  virtual int addBatch(const std::vector<std::string> &batch, std::time_t now) = 0;

  virtual SPFileCursor openCursor(bool autoAdvance=false) = 0;

  virtual void closeCursor(SPFileCursor spCursor) = 0;

  virtual void advanceCursor(SPFileCursor spCursor) = 0;

  /**
   * For a new cursor, listener.onRecord() will be called for every
   * record (that hasn't expired based on 'now' param).  Invoking
   * this function again without advancing the cursor will call
   * listener.onRecord() with the exact same set of records.
   * Advancing the cursor will invoke listener.onRecord() for the
   * first record after the previous
   * cursor.end until the most recently stored record.
   *
   * @param context Any value passed in context will be passed
   *                verbatim to listener.onRecord() calls.
   */
  virtual int enumerateRecords(ConveyorListener &listener, void *context, SPFileCursor spCursor, std::time_t now) = 0;

  /**
   * @return number of records not written due to being full.
   */
  virtual size_t getNumDrops() const = 0;

  /**
   * @return number of records waiting to be read.
   */
  virtual size_t getNumRecords() const = 0;

  /**
   * Will delete record and state files, start back at id=0.
   * @return true on error, false on success.
   */
  virtual bool deleteAndStartFresh() = 0;

  /**
   * Will write a .state file containing information needed to
   * load and resume current state.
   * @return true on error, false on success.
   */
  virtual bool persistState() = 0;

  /**
   * Load and resume last known state.
   * @return true on error, false on success.
   */
  virtual bool loadPersistedState() = 0;

  /*
   * A method used in unit tests to move the
   * writeCursor.id and readCursor.id = writeCursor.id-1
   * Used to test rollover.  Will call deleteAndStartFresh.
   */
  virtual void testResetAtId(uint32_t value) = 0;

  /*
   * Used by unit tests.
   */
  virtual void testGetReadCounters(size_t &numSkipped, size_t &numExpired, size_t &numNotified) = 0;

  /*
   * Used in unit tests.
   */
  virtual std::vector<ConveyorFile> testGetFileState() = 0;
};

typedef std::shared_ptr<Conveyor> SPConveyor;

SPConveyor ConveyorNew(ConveyorSettings settings);
