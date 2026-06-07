/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.bookkeeper.bookie.storage.ldb;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLongArray;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Group;
import org.openjdk.jmh.annotations.GroupThreads;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.infra.ThreadParams;

/**
 * Benchmark for repeated last-entry lookups in the RocksDB-backed entry location index.
 *
 * <p>The read-only benchmark models broker failover traffic where many threads repeatedly ask a
 * bookie for the last entry of already flushed ledgers. The read/write group keeps appending
 * entries while readers are issuing last-entry lookups, which models hot ledgers receiving data
 * while clients are also asking for LAC.
 *
 * <p>Run it on master and on the cache patch to compare the cost of repeated RocksDB getFloor()
 * calls with the cached path.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Fork(1)
@Warmup(iterations = 2, time = 5, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 5, timeUnit = TimeUnit.SECONDS)
public class EntryLocationIndexBenchmark {

    private static final int READ_LEDGER_INCREMENT = 0x9e3779b9;
    private static final int WRITE_LEDGER_INCREMENT = 0x7f4a7c15;

    @State(Scope.Benchmark)
    public static class IndexState {
        @Param({"4096", "10000"})
        private int ledgers;

        @Param({"128"})
        private int entriesPerLedger;

        @Param({"10000", "9500", "9000", "8500", "8000", "7500", "7000", "6000", "5000",
                "4000", "3000", "2048", "1000", "512", "256", "128", "64", "32"})
        private int lastEntryCacheMaxSize;

        private EntryLocationIndex index;
        private AtomicLongArray nextEntryIds;
        private Path tempDir;

        @Setup(Level.Trial)
        public void setup() throws Exception {
            tempDir = Files.createTempDirectory("bk-entry-location-index-benchmark");

            ServerConfiguration conf = new ServerConfiguration();
            conf.setProperty("dbStorage_lastEntryCacheMaxSize", lastEntryCacheMaxSize);

            index = new EntryLocationIndex(conf, KeyValueStorageRocksDB.factory,
                    tempDir.toString(), NullStatsLogger.INSTANCE);
            nextEntryIds = new AtomicLongArray(ledgers);

            try (KeyValueStorage.Batch batch = index.newBatch()) {
                for (int ledgerId = 0; ledgerId < ledgers; ledgerId++) {
                    for (int entryId = 0; entryId < entriesPerLedger; entryId++) {
                        long location = (((long) ledgerId) << 32) | entryId;
                        index.addLocation(batch, ledgerId, entryId, location);
                    }
                    nextEntryIds.set(ledgerId, entriesPerLedger);
                }
                batch.flush();
            }

            index.compact();

            for (int ledgerId = 0; ledgerId < ledgers; ledgerId++) {
                index.getLastEntryInLedger(ledgerId);
            }
        }

        @TearDown(Level.Trial)
        public void tearDown() throws Exception {
            if (index != null) {
                index.close();
            }
            if (tempDir != null) {
                deleteRecursively(tempDir);
            }
        }

        long ledgerId(int index) {
            return Integer.remainderUnsigned(index, ledgers);
        }

        long nextEntryId(long ledgerId) {
            return nextEntryIds.getAndIncrement((int) ledgerId);
        }

        long location(long ledgerId, long entryId) {
            return (ledgerId << 48) ^ entryId;
        }

        private static void deleteRecursively(Path path) throws IOException {
            if (!Files.exists(path)) {
                return;
            }
            try (java.util.stream.Stream<Path> paths = Files.walk(path)) {
                paths.sorted(Comparator.reverseOrder())
                        .forEach(EntryLocationIndexBenchmark.IndexState::deleteIfExists);
            }
        }

        private static void deleteIfExists(Path path) {
            try {
                Files.deleteIfExists(path);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @State(Scope.Thread)
    public static class ReadCursorState {
        private int nextLedger;

        @Setup(Level.Trial)
        public void setup(ThreadParams threadParams) {
            nextLedger = mix32(0x1f123bb5 + threadParams.getThreadIndex());
        }

        int next() {
            nextLedger += READ_LEDGER_INCREMENT;
            return mix32(nextLedger);
        }
    }

    @State(Scope.Thread)
    public static class WriteCursorState {
        private int nextLedger;

        @Setup(Level.Trial)
        public void setup(ThreadParams threadParams) {
            nextLedger = mix32(0x5a7f3c29 + threadParams.getThreadIndex());
        }

        int next() {
            nextLedger += WRITE_LEDGER_INCREMENT;
            return mix32(nextLedger);
        }
    }

    @Benchmark
    @Threads(16)
    public void getLastEntryInLedger(IndexState state, ReadCursorState cursor, Blackhole blackhole) throws Exception {
        blackhole.consume(state.index.getLastEntryInLedger(state.ledgerId(cursor.next())));
    }

    @Benchmark
    @Group("readWrite")
    @GroupThreads(12)
    public void getLastEntryInLedgerWhileWriting(IndexState state, ReadCursorState cursor,
                                                 Blackhole blackhole) throws Exception {
        blackhole.consume(state.index.getLastEntryInLedger(state.ledgerId(cursor.next())));
    }

    @Benchmark
    @Group("readWrite")
    @GroupThreads(4)
    public void addLocationWhileReading(IndexState state, WriteCursorState cursor,
                                        Blackhole blackhole) throws Exception {
        long ledgerId = state.ledgerId(cursor.next());
        long entryId = state.nextEntryId(ledgerId);
        state.index.addLocation(ledgerId, entryId, state.location(ledgerId, entryId));
        blackhole.consume(entryId);
    }

    private static int mix32(int value) {
        value ^= value >>> 16;
        value *= 0x7feb352d;
        value ^= value >>> 15;
        value *= 0x846ca68b;
        value ^= value >>> 16;
        return value;
    }
}
