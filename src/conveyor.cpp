
#include "../include/conveyor.h"

#include <stdio.h>
#include <fcntl.h>
#include <mutex>
#include <sys/stat.h>
#include <set>

#ifdef WINDOWS
#include <io.h>
#define CLOSE _close
#define WRITE _write
#define READ _read
#define LSEEK _lseek
#else
#define O_BINARY  0       // empty for non-windows
#define CLOSE close
#define WRITE write
#define READ read
#define LSEEK lseek
#endif // else WIN32

#include <boost/filesystem.hpp>
namespace fs = boost::filesystem;


static const int FR_MAX_CHUNKS = 10;
static const int FR_DEFAULT_CHUNKS = 2;
static const std::string FR_PREFIX = "_FR_";

static const uint32_t FRHDR_FLAG_HAS_TIMESTAMP = 0x80000000UL;
static const uint32_t FRHDR_FLAG_HAS_ID = 0x40000000UL;

#define REC_LEN(hdrval) ((hdrval) & 0x0FFFFFFFUL)

#define DBGV if(0)

struct ConveyorState {
  uint32_t structlen;
  uint32_t maxRecords;
  uint32_t numChunks;
  uint64_t numStoredRecords;
  uint32_t writeFileIndex;
  uint32_t writeCursorId;
  uint32_t readFileIndex;
  uint32_t readCursorId;
  uint64_t writeOffset;
  uint64_t readOffset;
  std::time_t readCursorTs;
};

struct ConveyorCursorImpl : public ConveyorCursor {
  ConveyorPosition getStart() override {
    return _start;
  }

  ConveyorPosition getEnd() override {
    return _end;
  }
  virtual ~ConveyorCursorImpl() { }

  ConveyorPosition _start;
  ConveyorPosition _end;
  bool          _autoAdvance;
};
typedef std::shared_ptr<ConveyorCursorImpl> SPConveyorCursorImpl;

// All records are preceded by a uint32_t
// that contains the length and optional flags.
// if FRHDR_FLAG_HAS_TIMESTAMP then length includes std::time_t
// if FRHDR_FLAG_HAS_ID then length includes uint32_t recordId
// if no timestamp, then record has same timestamp as last record
// if no recordId, then record id is previous + 1

/*
 * Contains state on a single chunked file-buffer.
 */
struct ConveyorImpl : public Conveyor {
  ConveyorImpl(ConveyorSettings settings) : _settings(settings), _fileVec(), _numDrops(),
    _numRecords(), _writePos(), _readPos(), _cursors(),
    _writeFd(), _mutex(), _numRecordsPerFile(0),
    _readCounterSkipped(0), _readCounterExpired(0), _readCounterNotified(0)
  {
    if (_settings.numChunks < 1 || _settings.numChunks > FR_MAX_CHUNKS) {
      _settings.numChunks = FR_DEFAULT_CHUNKS;
    }

    _numRecordsPerFile = _settings.maxRecords / settings.numChunks;

    _readPos.id = (uint32_t)-1;

    _fileVec.resize(_settings.numChunks);
    std::string filepath;
    for (size_t i = 0; i < _fileVec.size(); i++) {
      ConveyorFile &f = _fileVec[i];
      f.fileIndex = i;
      std::string fname = (FR_PREFIX + _settings.name + "_" + std::to_string(i));
      f.path = (fs::path(_settings.dirpath) / fname).string();
    }
  }

  virtual ~ConveyorImpl() {
    // close file handles
    if (_writeFd > 0) {
      CLOSE(_writeFd);
    }
  }

  /*
   * Handles u32 rollover.
   * @return true if a.id < b.id
   */
  bool IdLessThan(const ConveyorPosition &a, const ConveyorPosition &b) {
    auto delta = b.id - a.id;
    return ((delta != 0) && (delta < _settings.maxRecords));
  }

  /*
   * @returns 0 on success, -1 on error, 1 on drop
   */
  int addRecord(const std::string &value, std::time_t ts) override {
    std::lock_guard<std::mutex> lock(_mutex);

    return _addRecord(value,ts);
  }

  int _addRecord(const std::string &value, std::time_t ts) {

    // reached max?
    if (_numRecords >= _settings.maxRecords) {
      _numDrops++;
      return 1; // drop
    }

    _rotateIfNeeded(ts);

    if (_writeFd <= 0) {
      _numDrops++;
      return 1;
    }

    ConveyorFile &f = _fileVec[_writePos.fileIndex];
    bool isFirstRecordInFile = f.isEmpty();

    // write header

    uint32_t hdr[4] = {0,0,0,0};
    uint32_t *p = &hdr[1];
    hdr[0] = (uint32_t)value.size();
    bool needsTimestamp = isFirstRecordInFile || (ts != _writePos.ts);
    bool needsRecordId = isFirstRecordInFile;

    if (needsTimestamp) {
      hdr[0] |= FRHDR_FLAG_HAS_TIMESTAMP;
      hdr[0] += sizeof(std::time_t);
      *((std::time_t*)p) = ts;
      p += 2;
    }
    if (needsRecordId) {
      hdr[0] |= FRHDR_FLAG_HAS_ID;
      hdr[0] += sizeof(uint32_t);
      *p++ = _writePos.id;
    }

    size_t hdrlen = (size_t)((char *)p - (char *)&hdr[0]);
    int len = write(_writeFd, &hdr[0], hdrlen);
    if (len != hdrlen) {
      // TODO
      _errs = ("failed to write record header");
      return -1;
    }

    // write record

    len = write(_writeFd, value.c_str(), value.size());
    if (len != value.size()) {
      return -1;
    }

    // update state

    if (isFirstRecordInFile) {
      f.startTs = ts;
      f.startId = _writePos.id;
    }
    _writePos.id++;
    _writePos.ts = ts;
    _numRecords++;
    f.numRecords++;

    return 0;
  }

  int addBatch(const std::vector<std::string> &batch, std::time_t ts) override {
    std::lock_guard<std::mutex> lock(_mutex);
    int retval = 0;
    int drops = 0;

    for (const std::string &value : batch) {
      int rv = _addRecord(value,ts);
      if (rv < 0) { retval = rv; }
      if (rv > 0) { drops ++; }
    }

    if (retval < 0) {
      return retval;
    }
    return drops;
  }

  SPFileCursor openCursor(bool autoAdvance) override {
    std::lock_guard<std::mutex> lock(_mutex);
    auto spCursor = std::make_shared<ConveyorCursorImpl>();
    spCursor->_autoAdvance = autoAdvance;

    spCursor->_start = _readPos;

    _cursors.insert(spCursor);
    return spCursor;
  }

  void closeCursor(SPFileCursor spCursor) override {
    std::lock_guard<std::mutex> lock(_mutex);
    auto spc = std::static_pointer_cast<ConveyorCursorImpl>(spCursor);
    auto it = _cursors.find(spc);
    if (it != _cursors.end()) {
      _cursors.erase(it);
    }
  }

  void advanceCursor(SPFileCursor spCursor) override {
    std::lock_guard<std::mutex> lock(_mutex);
    if (spCursor == nullptr) {
      return;
    }
    _advanceCursor(spCursor);
  }

  void _advanceCursor(SPFileCursor spCursor) {
    auto spc = std::static_pointer_cast<ConveyorCursorImpl>(spCursor);

    // ensure this is active cursor on this instance

    if (_cursors.count(spc) != 1) {
      return;
    }

    // can only advance cursors that have end set.

    if (spc->_end.ts == 0) {
      return;
    }

    spc->_start = spc->_end;

    // if cursor is at end of file, wrap the fileIndex and reset offset

    auto &f = _fileVec[spc->_end.fileIndex];
    if ((spc->_start.id >= (f.startId+f.numRecords-1)) && f.numRecords >= _numRecordsPerFile) {
      _incr(spc->_start.fileIndex);
      spc->_start.offset = 0;
    }

    // discard records

    _discardRecordsIfAllCursorsDone(spc->_end);

    // clear the cursor end position

    spc->_end = ConveyorPosition();
  }

  /*
   * Will advance all cursors less than pos to pos.
   * Then call _discardRecordsIfAllCursorsDone() to
   * discard the records.
   */
  void _expireAfter(ConveyorPosition pos) {

    pos.offset = 0;

    for (auto &spc : _cursors) {
      if (IdLessThan(spc->_start,pos)) {
        spc->_start = pos;
        if (IdLessThan(spc->_end,pos)) {
          spc->_end = ConveyorPosition();
        }
      }
    }
    _discardRecordsIfAllCursorsDone(pos);
  }

  void _discardRecordsIfAllCursorsDone(ConveyorPosition pos) {

    if (IdLessThan(pos,_readPos)) {
      return;
    }

    // Find minimum position to advance to that is greater than readpos

    ConveyorPosition minpos = pos;
    for (auto cursor : _cursors) {
      if (IdLessThan(cursor->_start,minpos)) {
        minpos = cursor->_start;
      }
    }

    if (minpos.id == _readPos.id) {
      return;
    }
    pos = minpos;

    // id_rollover tests this for readPos.id > minpos.id

    int64_t delta = pos.id - _readPos.id;

    uint32_t idx = _readPos.fileIndex;

    for (int i=0; i < _fileVec.size(); i++, _incr(idx)) {
      ConveyorFile &f = _fileVec[idx];
      if (f.isActive == false || f.path.empty()) {
        break;
      }
      if (idx == pos.fileIndex && f.numRecords < _numRecordsPerFile) {
        break;
      }
      if (f.startId > pos.id) {
        break;
      }

      if (idx == _writePos.fileIndex) {
        _rotateIfNeeded(_writePos.ts);
      }

      if (idx == pos.fileIndex) {
        if ((f.startId+_numRecordsPerFile-1) <= pos.id) {
          // entire file has been read
          _incr(pos.fileIndex);
          pos.offset = 0;
        } else {
          continue;
        }
      }
	  if (idx == _writePos.fileIndex) {
		  continue;
	  }
      _cleanupFile(f);
    }

    _readPos = pos;

    DBGV fprintf(stderr, " Discarded %lu record(s) _readPos.id=%lu\n", (unsigned long)delta, (unsigned long)_readPos.id);

    _numRecords -= delta;

  }

  int enumerateRecords(ConveyorListener &listener, void *context, SPFileCursor spCursor, std::time_t now) override {
    std::lock_guard<std::mutex> lock(_mutex);
    if (_numRecords == 0 || spCursor == nullptr) {
      return 0;
    }

    auto spc = std::static_pointer_cast<ConveyorCursorImpl>(spCursor);
    if (IdLessThan(spc->_start, _readPos)) {
      return -1;
    }

    _readCounterSkipped = 0;
    _readCounterExpired = 0;
    _readCounterNotified = 0;
    _lastExpiredPos = ConveyorPosition();

    // since files are in a ring, start with read cursor's current file.
    uint32_t idx = spc->_start.fileIndex;
    ConveyorPosition pos = spc->_start;


    for (int i=0; i < _fileVec.size(); i++, _incr(idx)) {
      ConveyorFile &f = _fileVec[idx];
      if (f.isActive == false || f.path.empty()) {
        continue;
      }

      if (_readFile(f, &listener, context, spc, false, now, pos)) {
        break;
      }
    }

    if (_readCounterExpired > 0) {
      _expireAfter(_lastExpiredPos);
    }

    if (spc->_autoAdvance) {
      _advanceCursor(spc);
    }

    return 0;
  }

  size_t getNumDrops() const override { return _numDrops; }

  size_t getNumRecords() const override { return _numRecords; }

  bool persistState() override {
    std::lock_guard<std::mutex> lock(_mutex);
    _saveState();
    return false;
  }
  bool loadPersistedState() override {
    std::lock_guard<std::mutex> lock(_mutex);
    _loadState();
    return false;
  }

  bool deleteAndStartFresh() override {
    std::lock_guard<std::mutex> lock(_mutex);
    return _deleteAndStartFresh();
  }

  std::vector<ConveyorFile> testGetFileState() override {
    return _fileVec;
  }

  void testGetReadCounters(size_t &numSkipped, size_t &numExpired, size_t &numNotified) override {
    numSkipped = _readCounterSkipped;
    numExpired = _readCounterExpired;
    numNotified = _readCounterNotified;
  }

  void testResetAtId(uint32_t value) override {
    deleteAndStartFresh();
    _writePos.id = value;
    _readPos.id = value-1;
  }

protected:

  /**
   * @brief Remove a file from disk, and reset state.
   *----------------------------------------------------------*/
  bool _deleteAndStartFresh() {
    for (int i=0; i < _fileVec.size(); i++) {
      auto &f = _fileVec[i];
      if (!f.path.empty()) {
        fs::remove(f.path);
      }
      f.startId = 0;
      f.startTs = 0;
    }
    std::string path = (fs::path(_settings.dirpath) / (FR_PREFIX + _settings.name + ".state")).string();
    fs::remove(path);

    // clear cursor
    _writePos = ConveyorPosition();
    _readPos = ConveyorPosition();
    _readPos.id  = _writePos.id-1;

    _openWriteCursor();

    return true;
  }

  /**
   * @brief Remove a file from disk, and reset state.
   *----------------------------------------------------------*/
  void _cleanupFile(ConveyorFile &f) {
    fs::remove(f.path);
    f.isActive = false;
    f.numRecords = 0;
    f.startTs = 0;
    f.startId = 0;
  }

  // increment and rollover index, bounded by _settings.numChunks
  inline void _incr(uint32_t &fileIndex) {
    fileIndex = (fileIndex + 1) % _settings.numChunks;
  }

  /*
   * In the simple case, if readCursor.recordId > recordId, then
   * we have already seen the record.  Except when the recordId
   * counter rolls over the size of 32-bits.
   *----------------------------------------------------------*/
  bool _seenIt(uint32_t recordId, uint32_t readCursorId) {
    int64_t delta = (int64_t)(readCursorId + 1) - recordId;
    return ((delta > 0) || ((0-delta) >= _settings.maxRecords));
  }

  /**
   * @brief Called from _addRecord() to check to see if the
   * current write file is at the end.  Before writing record,
   * need to close the current file, open next.
   *----------------------------------------------------------*/
  void _rotateIfNeeded(std::time_t ts) {
    if (_fileVec[_writePos.fileIndex].numRecords < _numRecordsPerFile) {
      if (_writeFd <= 0) {
        _openWriteCursor();
      }
      return;
    }

    if (_writeFd > 0) {
      CLOSE(_writeFd);
      _writeFd = 0;
    }

    _incr(_writePos.fileIndex);

    if (_fileVec[_writePos.fileIndex].isActive) {
      if (_readPos.fileIndex != _writePos.fileIndex) {
        return;  // entirely full
      }
      // Can't write into this one, it still needs reading.
      // This is a case in which capacity is LESS than maxRecords
      return;
    }

    _openWriteCursor();
  }

  /**
   * @brief Quick sanity check on file
   *----------------------------------------------------------*/
  bool _isValid(ConveyorState &state) {
    if (state.numChunks != _settings.numChunks || state.maxRecords != _settings.maxRecords
        || state.readFileIndex >= _settings.numChunks || state.writeFileIndex >= _settings.numChunks ) {
      return false;
    }

    // write.id is read.id+1 when empty
    // should never be more than maxRecords, take into account 32-bit rollover

    auto delta = state.writeCursorId - state.readCursorId;
    if (delta > (_settings.maxRecords + 1)) {
      return false;
    }

    return true;
  }

  /**
   * @brief Reads length + flags dword and optional timestamp and id.
   *        Updates lastId, lastTs, reclen on exit
   * @return Number of bytes read on success, -1 on error.
   *----------------------------------------------------------*/
  int _readHeaders(int fd, uint32_t &lastId, std::time_t &lastTs, uint32_t &reclen) {
    uint32_t hdr;
    int bytesRead;

    // read first 32-bit value (length + flags)

    int len = read(fd, &hdr, sizeof(hdr));
    if (len <= 0) {
      return -1;
    }
    if (len != sizeof(hdr)) {
      return -1; // not expected
    }
    bytesRead = len;

    reclen = REC_LEN(hdr);

    if (hdr & FRHDR_FLAG_HAS_TIMESTAMP) {
      len = read(fd, &lastTs, sizeof(lastTs));
      if (len <= 0) {
        return -1;
      }
      bytesRead += len;
      if (lastTs == 0) {
        _errs = "invalid timestamp";
      }
      reclen -= sizeof(lastTs);
    }

    if (hdr & FRHDR_FLAG_HAS_ID) {
      len = read(fd, &lastId, sizeof(lastId));
      if (len <= 0) {
        return -1;
      }
      bytesRead += len;
      reclen -= sizeof(lastId);
    } else {
      lastId++;
    }

    return bytesRead;
  }

  /**
   * @brief read file.
   * @param ts == now , used for ts-record.ts >= expirySeconds
   *----------------------------------------------------------*/
  bool _readFile(ConveyorFile &f, ConveyorListener *listener, void *context, SPConveyorCursorImpl spc, bool updateStats, std::time_t ts, ConveyorPosition &pos) {

    uint32_t reclen = 0;
    uint64_t startOffset = 0;
    bool isAtCursorEnd = false;

    if (nullptr != spc) {
      pos = spc->_start;

      // _readCounterNotified is set to 0 at start of enumerateRecords().
      // So this ensures we can seek to last offset in file.

      if (_readCounterNotified == 0) {
        startOffset = spc->_start.offset;
      }
    }

    // open file

    int fd = open(f.path.c_str(), O_RDONLY | O_BINARY );
    if (fd <= 0) {
      return true;
    }

    if (updateStats) {

      // determine start id and timestamp

      if (-1 == _readHeaders(fd, pos.id, pos.ts, reclen)) {
        CLOSE(fd);
        return true;
      }
      f.startId = pos.id;
      f.startTs = pos.ts;
      // rewind to start
      lseek(fd, 0, SEEK_SET);
    }

    // seek to offset in f

    if (nullptr != spc && startOffset > 0) {
      pos.ts = spc->_start.ts;
      pos.id = spc->_start.id;

      auto lpos = lseek(fd, startOffset, SEEK_SET);
      if (lpos == -1) {
        CLOSE(fd);
        return true;
      }
    }
    pos.offset = startOffset;

    // read records
    bool isRecordTimeExpired = false;
    while (true) {

      pos.fileIndex = f.fileIndex;
      int headerLen = _readHeaders(fd, pos.id, pos.ts, reclen);
      if (-1 == headerLen) {
        break;
      }

      // sanity check

      if (pos.ts == 0 || reclen <= 0 || reclen > _settings.maxRecordSize) {
        DBGV fprintf(stderr, "invalid reclen(%u) or ts(%lu)\n", reclen, (unsigned long)pos.ts);
        break;
      }

      DBGV fprintf(stderr, "[%d] offset:%lu id:%lu ts:%lu reclen:%lu\n", (int)f.fileIndex, (unsigned long)pos.offset, (unsigned long)pos.id, (unsigned long)pos.ts, (unsigned long)reclen);

      // consecutive reads of same cursor without advanceCursor()
      // will have _end position set.  Need to stop at the same record
      // so that the exact same data set is read.

      if (spc != nullptr) {
        if (spc->_end.ts == 0) {
          int64_t delta = _writePos.id - pos.id;
          if (delta == 1) {
            spc->_end = pos;
            spc->_end.offset += headerLen + reclen;
          }
        } else if (pos.id > spc->_end.id ) {
          isAtCursorEnd = true;
          break;
        }
      }

      isRecordTimeExpired = (ts - pos.ts) >= _settings.expirySeconds;

      // either skip or read the record

      if (spc == nullptr || _seenIt(pos.id, spc->_start.id)) {

        if (updateStats) {
          f.numRecords++;
        }

        _readCounterSkipped++;

        auto offset = lseek(fd, reclen, SEEK_CUR);

        if (offset <= 0) {
          DBGV fprintf(stderr, "failed to skip over record\n");
          break;
        }

      } else {

        std::string tmpstr;
        tmpstr.resize(reclen);    // allocate

        int addlen = 0;
        int len;
        while (true) {
          len = read(fd, (char*)tmpstr.data() + addlen, reclen - addlen);
          if (len <= 0) {
            DBGV fprintf(stderr, "failed to read record value\n");
            break;
          }
          addlen += len;
          if (addlen >= reclen) { break; }
        }
        if (len <= 0) { break; }

        // only notify for non-expired

        if (isRecordTimeExpired) {
          _lastExpiredPos = pos;
          _readCounterExpired++;
        } else {

          _readCounterNotified++;
          DBGV fprintf(stderr," Notify id:%u ts:%ld\n", pos.id, (long)pos.ts );
          if (nullptr != listener) {
            listener->onRecord(context, tmpstr, pos.ts, pos.id);
          }
        }
      }
      pos.offset += headerLen + reclen;
    }

    CLOSE(fd);

    return (isAtCursorEnd == true);
  }

  /**
   * @brief Load state from disk.
   *----------------------------------------------------------*/
  void _loadState() {
    ConveyorState state;

    std::string path = (fs::path(_settings.dirpath) / (FR_PREFIX + _settings.name + ".state")).string();
    FILE *fp = fopen(path.c_str(), "rb");
    if (NULL != fp) {
      fread(&state,sizeof(state),1, fp);
      fclose(fp);
    } else {
      _openWriteCursor(true);
      return;
    }

    // sanity check

    if (state.structlen != sizeof(state) || !_isValid(state)) {
      _deleteAndStartFresh();
      return;
    }

    if (!fs::exists(_fileVec[state.readFileIndex].path)
        || !fs::exists(_fileVec[state.writeFileIndex].path)) {
      _deleteAndStartFresh();
      return;
    }

    // init from state

    _settings.maxRecords = state.maxRecords;
    _settings.numChunks = state.numChunks;
    _readPos.fileIndex = state.readFileIndex;
    _readPos.id = state.readCursorId;
    _writePos.fileIndex = state.writeFileIndex;
    _writePos.id = state.writeCursorId;
    _readPos.offset = state.readOffset;
    _writePos.offset = state.writeOffset;
    _numRecords = state.numStoredRecords;
    _readPos.ts = state.readCursorTs;

    bool isError = false;
    auto idx = _readPos.fileIndex;
    size_t countedRecords = 0;
    ConveyorPosition pos = _readPos;
    while (true) {
      ConveyorFile &f = _fileVec[idx];
      f.isActive = true;

      if(_readFile(f,nullptr, nullptr, nullptr, true /* updateStats */, std::time(NULL), pos)) {
        isError = true;
        break;
      }
      countedRecords += f.numRecords;

      // done after reading in current write file

      if (idx == _writePos.fileIndex) { break; }
      _incr(idx);
    }

    if (isError) {
      _deleteAndStartFresh();
      return;
    }

    // if no files exist, cursor is all 0's
    _openWriteCursor();
  }

  /**
   * @brief Save state to disk.
   *----------------------------------------------------------*/
  void _saveState() {
    ConveyorState state = {
      sizeof(ConveyorState),
      _settings.maxRecords, (uint32_t)_settings.numChunks,
      _numRecords,
      _writePos.fileIndex, _writePos.id,
      _readPos.fileIndex, _readPos.id,
      _writePos.offset, _readPos.offset, _readPos.ts
    };
    std::string path = (fs::path(_settings.dirpath) / (FR_PREFIX + _settings.name + ".state")).string();
    FILE *fp = fopen(path.c_str(), "wb");
    if (NULL != fp) {
      fwrite(&state, sizeof(state), 1, fp);
      fclose(fp);
    }
  }


  /**
   * @brief Open file for writing
   *----------------------------------------------------------*/
  void _openWriteCursor(bool doAppend=false) {
    ConveyorFile &f = _fileVec[_writePos.fileIndex];

    if (doAppend) {
      // open existing
      _writeFd = open(f.path.c_str(), O_WRONLY | O_APPEND | O_BINARY, 0660 );
      if (_writeFd <= 0) {
        _errs = ("unable to open file for writing:" + f.path);
      } else {
        f.isActive = true;
      }
    } else {
      // create new
      _writeFd = open(f.path.c_str(), O_WRONLY | O_CREAT | O_BINARY, 0660 );
      if (_writeFd <= 0) {
        _errs = ("unable to open file for writing:" + f.path);
      } else {
        f.isActive = true;
      }
    }
  }

  ConveyorSettings          _settings;
  std::vector<ConveyorFile> _fileVec;
  size_t                 _numDrops;
  size_t                 _numRecords;
  ConveyorPosition          _writePos;
  ConveyorPosition          _readPos;
  std::set<SPConveyorCursorImpl> _cursors;
  int                    _writeFd;
  std::string            _errs;
  std::mutex             _mutex;
  uint32_t               _numRecordsPerFile; // derived from settings.numRecords and numChunks
  uint32_t               _readCounterSkipped;
  uint32_t               _readCounterExpired;
  uint32_t               _readCounterNotified;
  ConveyorPosition          _lastExpiredPos;
};

SPConveyor ConveyorNew(ConveyorSettings settings) {

  // test for essentially zero values
  if (settings.maxRecords < 10 || settings.numChunks < 1 || settings.maxRecordSize < 128 || settings.expirySeconds == 0) {
    return nullptr;
  }

  return std::make_shared<ConveyorImpl>(settings);
}

