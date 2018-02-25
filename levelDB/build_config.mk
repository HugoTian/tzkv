SOURCES=db/builder.cc db/c.cc db/db_impl.cc db/db_iter.cc db/dbformat.cc db/dumpfile.cc db/filename.cc db/log_reader.cc db/log_writer.cc db/memtable.cc db/repair.cc db/table_cache.cc db/version_edit.cc db/version_set.cc db/write_batch.cc table/block.cc table/block_builder.cc table/filter_block.cc table/format.cc table/iterator.cc table/merger.cc table/table.cc table/table_builder.cc table/two_level_iterator.cc util/arena.cc util/bloom.cc util/cache.cc util/coding.cc util/comparator.cc util/crc32c.cc util/env.cc util/env_posix.cc util/filter_policy.cc util/hash.cc util/histogram.cc util/logging.cc util/options.cc util/status.cc  port/port_posix.cc
MEMENV_SOURCES=helpers/memenv/memenv.cc
CC=cc
CXX=c++
PLATFORM=OS_MACOSX
PLATFORM_LDFLAGS=
PLATFORM_LIBS= -lsnappy
PLATFORM_CCFLAGS=  -DLEVELDB_PLATFORM_POSIX=1 -DLEVELDB_ATOMIC_PRESENT -DHAVE_SNAPPY=1
PLATFORM_CXXFLAGS=-std=c++0x  -DLEVELDB_PLATFORM_POSIX=1 -DLEVELDB_ATOMIC_PRESENT -DHAVE_SNAPPY=1
PLATFORM_SHARED_CFLAGS=-fPIC -fvisibility=hidden
PLATFORM_SHARED_EXT=dylib
PLATFORM_SHARED_LDFLAGS=-dynamiclib -install_name /Users/tzhang/Desktop/Self/leveldb/
PLATFORM_SHARED_VERSIONED=true
