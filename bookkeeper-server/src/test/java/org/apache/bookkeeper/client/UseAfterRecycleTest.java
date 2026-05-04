/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.
 */
package org.apache.bookkeeper.client;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.util.ReferenceCounted;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.client.BKException.Code;
import org.apache.bookkeeper.client.BookKeeper.DigestType;
import org.apache.bookkeeper.client.DistributionSchedule.WriteSet;
import org.apache.bookkeeper.client.api.LedgerEntries;
import org.apache.bookkeeper.client.api.LedgerMetadata;
import org.apache.bookkeeper.common.util.OrderedScheduler;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.proto.BookieClient;
import org.apache.bookkeeper.proto.BookieProtocol;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.ReadEntryCallback;
import org.apache.bookkeeper.proto.checksum.DigestManager;
import org.apache.bookkeeper.proto.checksum.DummyDigestManager;
import org.apache.bookkeeper.test.TestStatsProvider;
import org.apache.bookkeeper.util.ByteBufList;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Verifies suspected use-after-recycle bugs in client-side Read ops.
 *
 * <p>Each test wraps the {@link WriteSet} returned by the {@link DistributionSchedule}
 * in a tracking wrapper that records the order of {@code recycle()} versus other
 * accesses. After exercising the relevant code path the test asserts that no method
 * was invoked on a recycled object.
 */
@Slf4j
public class UseAfterRecycleTest {

    private static final long LEDGER_ID = 0xBEEFL;
    private static final int ENSEMBLE_SIZE = 3;
    private static final int WRITE_QUORUM_SIZE = 3;
    private static final int ACK_QUORUM_SIZE = 2;

    private final TestStatsProvider testStatsProvider = new TestStatsProvider();
    private BookKeeperClientStats clientStats;
    private ClientContext mockClientCtx;
    private BookieClient mockBookieClient;
    private LedgerHandle mockLh;
    private OrderedScheduler orderedScheduler;
    private ClientInternalConf internalConf;
    private EnsemblePlacementPolicy placementPolicy;
    private LedgerMetadata ledgerMetadata;
    private RoundRobinDistributionSchedule realSchedule;
    private TrackingDistributionSchedule trackingSchedule;
    private DigestManager digestManager;
    private ArrayList<BookieId> ensemble;

    @Before
    public void setup() throws Exception {
        clientStats = BookKeeperClientStats.newInstance(testStatsProvider.getStatsLogger(""));
        ClientConfiguration conf = new ClientConfiguration();
        conf.setReorderReadSequenceEnabled(false);
        conf.setFirstSpeculativeReadLACTimeout(100);
        conf.setMaxSpeculativeReadLACTimeout(200);
        conf.setSpeculativeReadLACTimeoutBackoffMultiplier(2);
        internalConf = ClientInternalConf.fromConfig(conf);

        ensemble = new ArrayList<>(ENSEMBLE_SIZE);
        for (int i = 0; i < ENSEMBLE_SIZE; i++) {
            ensemble.add(new BookieSocketAddress("127.0.0.1", 3181 + i).toBookieId());
        }
        ledgerMetadata = LedgerMetadataBuilder.create()
                .withId(LEDGER_ID)
                .withEnsembleSize(ENSEMBLE_SIZE)
                .withWriteQuorumSize(WRITE_QUORUM_SIZE)
                .withAckQuorumSize(ACK_QUORUM_SIZE)
                .withPassword(new byte[0])
                .withDigestType(DigestType.CRC32.toApiDigestType())
                .newEnsembleEntry(0L, ensemble).build();
        realSchedule = new RoundRobinDistributionSchedule(
                WRITE_QUORUM_SIZE, ACK_QUORUM_SIZE, ENSEMBLE_SIZE);
        trackingSchedule = new TrackingDistributionSchedule(realSchedule);

        orderedScheduler = OrderedScheduler.newSchedulerBuilder()
                .name("uar-test").numThreads(1).build();

        mockBookieClient = mock(BookieClient.class);
        placementPolicy = new DefaultEnsemblePlacementPolicy();
        mockClientCtx = mock(ClientContext.class);
        when(mockClientCtx.getBookieClient()).thenReturn(mockBookieClient);
        when(mockClientCtx.getPlacementPolicy()).thenReturn(placementPolicy);
        when(mockClientCtx.getConf()).thenReturn(internalConf);
        when(mockClientCtx.getScheduler()).thenReturn(orderedScheduler);
        when(mockClientCtx.getMainWorkerPool()).thenReturn(orderedScheduler);
        when(mockClientCtx.getClientStats()).thenReturn(clientStats);

        mockLh = mock(LedgerHandle.class);
        when(mockLh.getId()).thenReturn(LEDGER_ID);
        when(mockLh.getCurrentEnsemble()).thenReturn(ensemble);
        when(mockLh.getLedgerMetadata()).thenReturn(ledgerMetadata);
        when(mockLh.getDistributionSchedule()).thenReturn(trackingSchedule);
        when(mockLh.getWriteSetForReadOperation(anyLong()))
                .thenAnswer(inv -> trackingSchedule.getWriteSet(inv.getArgument(0, Long.class)));
        digestManager = new DummyDigestManager(LEDGER_ID, false, UnpooledByteBufAllocator.DEFAULT);
        when(mockLh.getDigestManager()).thenReturn(digestManager);
        // PendingReadOp uses lh.macManager and lh.ledgerId (package-private fields directly)
        java.lang.reflect.Field f = LedgerHandle.class.getDeclaredField("macManager");
        f.setAccessible(true);
        f.set(mockLh, digestManager);
        java.lang.reflect.Field lid = LedgerHandle.class.getDeclaredField("ledgerId");
        lid.setAccessible(true);
        lid.set(mockLh, LEDGER_ID);
        java.lang.reflect.Field ds = LedgerHandle.class.getDeclaredField("distributionSchedule");
        ds.setAccessible(true);
        ds.set(mockLh, trackingSchedule);
    }

    @After
    public void teardown() {
        orderedScheduler.shutdown();
    }

    /** Captures the (callback, ctx) pair from a mocked readEntry invocation. */
    static class ReadCapture {
        final ReadEntryCallback cb;
        final Object ctx;
        ReadCapture(ReadEntryCallback cb, Object ctx) { this.cb = cb; this.ctx = ctx; }
    }

    private void stubReadEntry(Map<BookieId, ReadCapture> captures) {
        // 7-arg overload: (addr, ledgerId, entryId, cb, ctx, flags, masterKey)
        doAnswer(inv -> {
            captures.put(inv.getArgument(0), new ReadCapture(inv.getArgument(3), inv.getArgument(4)));
            return null;
        }).when(mockBookieClient).readEntry(any(BookieId.class),
                anyLong(), anyLong(), any(ReadEntryCallback.class), any(),
                anyInt(), any(byte[].class));
        // 6-arg overload: (addr, ledgerId, entryId, cb, ctx, flags)
        doAnswer(inv -> {
            captures.put(inv.getArgument(0), new ReadCapture(inv.getArgument(3), inv.getArgument(4)));
            return null;
        }).when(mockBookieClient).readEntry(any(BookieId.class),
                anyLong(), anyLong(), any(ReadEntryCallback.class), any(),
                anyInt());
    }

    /** Helper: package the entry data with a dummy digest header, mimicking what a bookie returns. */
    private ByteBuf makeEntryWithDigest(long entryId, long lac, byte[] data) {
        ByteBuf payload = Unpooled.copiedBuffer(data);
        ReferenceCounted refCnt = digestManager.computeDigestAndPackageForSending(
                entryId, lac, data.length, payload, new byte[20], 0);
        ByteBufList bbl = (ByteBufList) refCnt;
        byte[] bytesWithDigest = new byte[bbl.readableBytes()];
        assertEquals(bytesWithDigest.length, bbl.getBytes(bytesWithDigest));
        bbl.release();
        return Unpooled.wrappedBuffer(bytesWithDigest);
    }

    // ====================================================================
    // FINDING #8: PendingReadOp.SequenceReadRequest.complete() reads
    //             writeSet AFTER super.complete() has called writeSet.recycle()
    // ====================================================================
    @Test
    public void testPendingReadOpSequenceDoesNotUseWriteSetAfterRecycle() throws Exception {
        // To trigger the slow-bookie loop in SequenceReadRequest.complete (which reads
        // writeSet AFTER super.complete recycled it), we need numReplicasTried > 1.
        // So: fail the first bookie's read, then succeed on the second.
        final long entryId = 0L;
        final Map<BookieId, ReadCapture> captures = Collections.synchronizedMap(new HashMap<>());
        // Use a list so we can capture in arrival order rather than by addr
        final java.util.List<ReadCapture> captureOrder = Collections.synchronizedList(new ArrayList<>());
        doAnswer(inv -> {
            ReadCapture c = new ReadCapture(inv.getArgument(3), inv.getArgument(4));
            captures.put(inv.getArgument(0), c);
            captureOrder.add(c);
            return null;
        }).when(mockBookieClient).readEntry(any(BookieId.class),
                anyLong(), anyLong(), any(ReadEntryCallback.class), any(),
                anyInt(), any(byte[].class));
        doAnswer(inv -> {
            ReadCapture c = new ReadCapture(inv.getArgument(3), inv.getArgument(4));
            captures.put(inv.getArgument(0), c);
            captureOrder.add(c);
            return null;
        }).when(mockBookieClient).readEntry(any(BookieId.class),
                anyLong(), anyLong(), any(ReadEntryCallback.class), any(),
                anyInt());

        PendingReadOp op = new PendingReadOp(mockLh, mockClientCtx, entryId, entryId, false);
        op.parallelRead(false); // SequenceReadRequest path
        op.initiate();

        long deadline = System.currentTimeMillis() + 5000;
        while (captureOrder.isEmpty() && System.currentTimeMillis() < deadline) {
            Thread.sleep(5);
        }
        assertTrue("no read sent", !captureOrder.isEmpty());

        // 1) First bookie returns NoSuchEntry — triggers logErrorAndReattemptRead → sendNextRead
        ReadCapture first = captureOrder.get(0);
        first.cb.readEntryComplete(Code.NoSuchEntryException, LEDGER_ID, entryId, null, first.ctx);

        // wait for the retry to be sent
        deadline = System.currentTimeMillis() + 5000;
        while (captureOrder.size() < 2 && System.currentTimeMillis() < deadline) {
            Thread.sleep(5);
        }
        assertEquals("retry not sent — got " + captureOrder.size() + " reads", 2, captureOrder.size());

        // 2) Second bookie returns OK — triggers SequenceReadRequest.complete() with
        //    numReplicasTried = 2, so the slow-bookie loop runs writeSet.get(0).
        ReadCapture second = captureOrder.get(1);
        ByteBuf entry = makeEntryWithDigest(entryId, 0L, "hello".getBytes(UTF_8));
        second.cb.readEntryComplete(Code.OK, LEDGER_ID, entryId, entry, second.ctx);

        try (LedgerEntries entries = op.future.get(5, TimeUnit.SECONDS)) {
            assertNotNull(entries.getEntry(entryId));
        }

        Throwable violation = trackingSchedule.firstViolation.get();
        if (violation != null) {
            throw new AssertionError(
                    "Use-after-recycle on WriteSet during PendingReadOp.SequenceReadRequest.complete: "
                    + violation.getMessage(), violation);
        }
    }

    // ====================================================================
    // FINDING #9: BatchedReadOp.SequenceReadRequest.complete() reads writeSet
    //             AFTER super.complete() called writeSet.recycle()
    // ====================================================================
    @Test
    public void testBatchedReadOpSequenceDoesNotUseWriteSetAfterRecycle() throws Exception {
        final long startEntryId = 0L;
        final int maxCount = 1;
        final long maxSize = 1024;

        final java.util.List<BatchCapture> captures = Collections.synchronizedList(new ArrayList<>());
        doAnswer(inv -> {
            BatchCapture c = new BatchCapture(
                    inv.getArgument(5),  // BatchedReadEntryCallback
                    inv.getArgument(6)); // ctx
            captures.add(c);
            return null;
        }).when(mockBookieClient).batchReadEntries(any(BookieId.class),
                anyLong(), anyLong(), anyInt(), anyLong(),
                any(org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.BatchedReadEntryCallback.class),
                any(), anyInt(), any(byte[].class), org.mockito.ArgumentMatchers.anyBoolean());
        doAnswer(inv -> {
            BatchCapture c = new BatchCapture(
                    inv.getArgument(5),
                    inv.getArgument(6));
            captures.add(c);
            return null;
        }).when(mockBookieClient).batchReadEntries(any(BookieId.class),
                anyLong(), anyLong(), anyInt(), anyLong(),
                any(org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.BatchedReadEntryCallback.class),
                any(), anyInt());

        BatchedReadOp op = new BatchedReadOp(mockLh, mockClientCtx, startEntryId, maxCount, maxSize, false);
        op.initiate();

        long deadline = System.currentTimeMillis() + 5000;
        while (captures.isEmpty() && System.currentTimeMillis() < deadline) {
            Thread.sleep(5);
        }
        assertTrue("no batch read sent", !captures.isEmpty());

        // Trigger a retry: first bookie fails
        BatchCapture first = captures.get(0);
        first.cb.readEntriesComplete(Code.NoSuchEntryException, LEDGER_ID, startEntryId, null, first.ctx);

        deadline = System.currentTimeMillis() + 5000;
        while (captures.size() < 2 && System.currentTimeMillis() < deadline) {
            Thread.sleep(5);
        }
        assertEquals("retry not sent — got " + captures.size(), 2, captures.size());

        // Second bookie returns success — drives super.complete() → recycle, then slow-bookie loop
        BatchCapture second = captures.get(1);
        ByteBuf entry = makeEntryWithDigest(startEntryId, 0L, "world".getBytes(UTF_8));
        ByteBufList list = ByteBufList.get(entry);
        second.cb.readEntriesComplete(Code.OK, LEDGER_ID, startEntryId, list, second.ctx);

        try (LedgerEntries entries = op.future.get(5, TimeUnit.SECONDS)) {
            assertNotNull(entries.getEntry(startEntryId));
        }

        Throwable violation = trackingSchedule.firstViolation.get();
        if (violation != null) {
            throw new AssertionError(
                    "Use-after-recycle on WriteSet during BatchedReadOp.SequenceReadRequest.complete: "
                    + violation.getMessage(), violation);
        }
    }

    // ====================================================================
    // FINDING #10: ReadLastConfirmedAndEntryOp.SequenceReadRequest.complete()
    //              reads orderedEnsemble AFTER super.complete() recycled it
    // ====================================================================
    @Test
    public void testReadLastConfirmedAndEntryOpDoesNotUseOrderedEnsembleAfterRecycle() throws Exception {
        // ReadLac's SequenceReadRequest.complete loop is `for (i = 0; i < numReplicasTried; i++)`
        // — note no `-1`. So even a single successful read triggers orderedEnsemble.get(0).
        final long entryId = 2L;
        final long lac = 1L;

        ByteBuf entry = makeEntryWithDigest(entryId, lac, "lacdata".getBytes(UTF_8));

        final java.util.List<ReadCapture> captures = Collections.synchronizedList(new ArrayList<>());
        doAnswer(inv -> {
            captures.add(new ReadCapture(inv.getArgument(6), inv.getArgument(7)));
            return null;
        }).when(mockBookieClient).readEntryWaitForLACUpdate(any(BookieId.class),
                anyLong(), anyLong(), anyLong(), anyLong(),
                org.mockito.ArgumentMatchers.anyBoolean(),
                any(ReadEntryCallback.class), any());

        java.util.concurrent.CompletableFuture<org.apache.bookkeeper.client.api.LastConfirmedAndEntry>
                resultFuture = new java.util.concurrent.CompletableFuture<>();
        ReadLastConfirmedAndEntryOp.LastConfirmedAndEntryCallback resultCallback =
                (rc, lastAddConfirmed, e) -> {
                    if (Code.OK != rc) {
                        resultFuture.completeExceptionally(BKException.create(rc));
                    } else {
                        resultFuture.complete(
                                org.apache.bookkeeper.client.impl.LastConfirmedAndEntryImpl.create(
                                        lastAddConfirmed, e));
                    }
                };

        ReadLastConfirmedAndEntryOp op = new ReadLastConfirmedAndEntryOp(
                mockLh, mockClientCtx, mockLh.getCurrentEnsemble(), resultCallback, lac, 10000);
        op.initiate();

        long deadline = System.currentTimeMillis() + 5000;
        while (captures.isEmpty() && System.currentTimeMillis() < deadline) {
            Thread.sleep(5);
        }
        assertTrue("no readEntryWaitForLACUpdate sent", !captures.isEmpty());

        // Single successful response → numReplicasTried becomes 1 → loop body executes once
        ReadCapture rc = captures.get(0);
        org.apache.bookkeeper.proto.ReadLastConfirmedAndEntryContext lacCtx =
                (org.apache.bookkeeper.proto.ReadLastConfirmedAndEntryContext) rc.ctx;
        lacCtx.setLastAddConfirmed(entryId);
        rc.cb.readEntryComplete(Code.OK, LEDGER_ID, entryId, entry, lacCtx);

        try (org.apache.bookkeeper.client.api.LastConfirmedAndEntry result =
                     resultFuture.get(5, TimeUnit.SECONDS)) {
            assertNotNull(result.getEntry());
        }

        Throwable violation = trackingSchedule.firstViolation.get();
        if (violation != null) {
            throw new AssertionError(
                    "Use-after-recycle on WriteSet/orderedEnsemble during "
                    + "ReadLastConfirmedAndEntryOp.SequenceReadRequest.complete: " + violation.getMessage(),
                    violation);
        }
    }

    // ====================================================================
    // FINDING #1: BatchedReadOp.complete() sets `complete=true` BEFORE digest
    //             verification. On digest mismatch the request gets stuck —
    //             the retry response cannot land because isComplete() is true.
    // ====================================================================
    @Test
    public void testBatchedReadOpDoesNotGetStuckOnDigestMismatch() throws Exception {
        final long startEntryId = 0L;
        final int maxCount = 1;
        final long maxSize = 1024;

        final java.util.List<BatchCapture> captures = Collections.synchronizedList(new ArrayList<>());
        doAnswer(inv -> {
            captures.add(new BatchCapture(inv.getArgument(5), inv.getArgument(6)));
            return null;
        }).when(mockBookieClient).batchReadEntries(any(BookieId.class),
                anyLong(), anyLong(), anyInt(), anyLong(),
                any(org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.BatchedReadEntryCallback.class),
                any(), anyInt(), any(byte[].class), org.mockito.ArgumentMatchers.anyBoolean());
        doAnswer(inv -> {
            captures.add(new BatchCapture(inv.getArgument(5), inv.getArgument(6)));
            return null;
        }).when(mockBookieClient).batchReadEntries(any(BookieId.class),
                anyLong(), anyLong(), anyInt(), anyLong(),
                any(org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.BatchedReadEntryCallback.class),
                any(), anyInt());

        BatchedReadOp op = new BatchedReadOp(mockLh, mockClientCtx, startEntryId, maxCount, maxSize, false);
        op.initiate();

        long deadline = System.currentTimeMillis() + 5000;
        while (captures.isEmpty() && System.currentTimeMillis() < deadline) {
            Thread.sleep(5);
        }
        assertTrue("no batch read sent", !captures.isEmpty());

        // First bookie: corrupt entry — packaged with entryId=999 but we read as entry 0.
        // verifyDigestAndReturnData throws BKDigestMatchException → triggers logErrorAndReattemptRead
        BatchCapture first = captures.get(0);
        ByteBuf badEntry = makeEntryWithDigest(999L, 0L, "corrupt".getBytes(UTF_8));
        ByteBufList badList = ByteBufList.get(badEntry);
        first.cb.readEntriesComplete(Code.OK, LEDGER_ID, startEntryId, badList, first.ctx);

        // Wait for retry to be sent
        deadline = System.currentTimeMillis() + 5000;
        while (captures.size() < 2 && System.currentTimeMillis() < deadline) {
            Thread.sleep(5);
        }
        assertEquals("retry was not sent (or sent too many)", 2, captures.size());

        // Second bookie: correct entry
        BatchCapture second = captures.get(1);
        ByteBuf goodEntry = makeEntryWithDigest(startEntryId, 0L, "good".getBytes(UTF_8));
        ByteBufList goodList = ByteBufList.get(goodEntry);
        second.cb.readEntriesComplete(Code.OK, LEDGER_ID, startEntryId, goodList, second.ctx);

        // The future should complete successfully. If the bug exists (complete=true was set
        // by the first digest mismatch), the second response will short-circuit on
        // isComplete() in super.complete() and submitCallback never fires → future hangs.
        try (LedgerEntries entries = op.future.get(3, TimeUnit.SECONDS)) {
            assertNotNull(entries.getEntry(startEntryId));
        } catch (java.util.concurrent.TimeoutException te) {
            throw new AssertionError(
                    "BatchedReadOp got stuck after a digest-mismatch on the first response: "
                    + "the retry response could not complete the request because complete=true "
                    + "was already set (BatchedReadOp.complete:163-186).", te);
        }
    }

    /** Sanity check / control: PendingReadOp is NOT stuck on digest mismatch (bug-free). */
    @Test
    public void testPendingReadOpRecoversFromDigestMismatch() throws Exception {
        final long entryId = 0L;
        final java.util.List<ReadCapture> captures = Collections.synchronizedList(new ArrayList<>());
        doAnswer(inv -> {
            captures.add(new ReadCapture(inv.getArgument(3), inv.getArgument(4)));
            return null;
        }).when(mockBookieClient).readEntry(any(BookieId.class),
                anyLong(), anyLong(), any(ReadEntryCallback.class), any(),
                anyInt(), any(byte[].class));
        doAnswer(inv -> {
            captures.add(new ReadCapture(inv.getArgument(3), inv.getArgument(4)));
            return null;
        }).when(mockBookieClient).readEntry(any(BookieId.class),
                anyLong(), anyLong(), any(ReadEntryCallback.class), any(),
                anyInt());

        PendingReadOp op = new PendingReadOp(mockLh, mockClientCtx, entryId, entryId, false);
        op.parallelRead(false);
        op.initiate();

        long deadline = System.currentTimeMillis() + 5000;
        while (captures.isEmpty() && System.currentTimeMillis() < deadline) {
            Thread.sleep(5);
        }
        assertTrue("no read sent", !captures.isEmpty());

        // First bookie: corrupt
        ReadCapture first = captures.get(0);
        ByteBuf badEntry = makeEntryWithDigest(999L, 0L, "corrupt".getBytes(UTF_8));
        first.cb.readEntryComplete(Code.OK, LEDGER_ID, entryId, badEntry, first.ctx);

        deadline = System.currentTimeMillis() + 5000;
        while (captures.size() < 2 && System.currentTimeMillis() < deadline) {
            Thread.sleep(5);
        }
        assertEquals(2, captures.size());

        // Second bookie: correct
        ReadCapture second = captures.get(1);
        ByteBuf goodEntry = makeEntryWithDigest(entryId, 0L, "good".getBytes(UTF_8));
        second.cb.readEntryComplete(Code.OK, LEDGER_ID, entryId, goodEntry, second.ctx);

        try (LedgerEntries entries = op.future.get(3, TimeUnit.SECONDS)) {
            assertNotNull(entries.getEntry(entryId));
        }
    }

    // ====================================================================
    // FINDING #11: ReadLastConfirmedAndEntryOp.SequenceReadRequest.logErrorAndReattemptRead
    //              reads orderedEnsemble (and parent reads writeSet/entryImpl) AFTER
    //              the request was already completed and recycled.
    // ====================================================================
    @Test
    public void testReadLastConfirmedAndEntryOpDoesNotUseRecycledStateInLateError() throws Exception {
        // 1) one bookie returns OK with a real entry → completeRequest fires → request.close() →
        //    entryImpl.close() (recycle), super.complete() called writeSet.recycle()/orderedEnsemble.recycle().
        // 2) a LATE error response from a previously-spec bookie arrives → readEntryComplete's
        //    "else" branch calls request.logErrorAndReattemptRead → reads recycled state.

        final long entryId = 2L;
        final long lac = 1L;

        ByteBuf entry = makeEntryWithDigest(entryId, lac, "lacdata".getBytes(UTF_8));

        final java.util.List<ReadCapture> captures = Collections.synchronizedList(new ArrayList<>());
        doAnswer(inv -> {
            captures.add(new ReadCapture(inv.getArgument(6), inv.getArgument(7)));
            return null;
        }).when(mockBookieClient).readEntryWaitForLACUpdate(any(BookieId.class),
                anyLong(), anyLong(), anyLong(), anyLong(),
                org.mockito.ArgumentMatchers.anyBoolean(),
                any(ReadEntryCallback.class), any());

        java.util.concurrent.CompletableFuture<org.apache.bookkeeper.client.api.LastConfirmedAndEntry>
                resultFuture = new java.util.concurrent.CompletableFuture<>();
        ReadLastConfirmedAndEntryOp.LastConfirmedAndEntryCallback resultCallback =
                (rc, lastAddConfirmed, e) -> {
                    if (Code.OK != rc) {
                        resultFuture.completeExceptionally(BKException.create(rc));
                    } else {
                        resultFuture.complete(
                                org.apache.bookkeeper.client.impl.LastConfirmedAndEntryImpl.create(
                                        lastAddConfirmed, e));
                    }
                };

        ReadLastConfirmedAndEntryOp op = new ReadLastConfirmedAndEntryOp(
                mockLh, mockClientCtx, mockLh.getCurrentEnsemble(), resultCallback, lac, 10000);
        op.initiate();

        // Wait for speculative reads to fan out — speculative timeout is 100ms.
        long deadline = System.currentTimeMillis() + 5000;
        while (captures.size() < 2 && System.currentTimeMillis() < deadline) {
            Thread.sleep(20);
        }
        assertTrue("speculative read not sent (got " + captures.size() + ")", captures.size() >= 2);

        // First captured read finishes with OK → completes the request, recycles state.
        ReadCapture first = captures.get(0);
        org.apache.bookkeeper.proto.ReadLastConfirmedAndEntryContext firstCtx =
                (org.apache.bookkeeper.proto.ReadLastConfirmedAndEntryContext) first.ctx;
        firstCtx.setLastAddConfirmed(entryId);
        first.cb.readEntryComplete(Code.OK, LEDGER_ID, entryId, entry, firstCtx);

        // Wait for the future to complete
        try (org.apache.bookkeeper.client.api.LastConfirmedAndEntry result =
                     resultFuture.get(5, TimeUnit.SECONDS)) {
            assertNotNull(result.getEntry());
        }

        // Reset violation counter so we ignore violations during the success path (#10
        // is tested in another method) and only watch for ones in the late-error path.
        trackingSchedule.firstViolation.set(null);

        // Now the LATE error response arrives from the speculative bookie.
        ReadCapture second = captures.get(1);
        second.cb.readEntryComplete(Code.NoSuchEntryException, LEDGER_ID,
                BookieProtocol.LAST_ADD_CONFIRMED, null, second.ctx);

        Throwable violation = trackingSchedule.firstViolation.get();
        if (violation != null) {
            throw new AssertionError(
                    "Use-after-recycle on writeSet/orderedEnsemble during "
                    + "ReadLastConfirmedAndEntryOp.SequenceReadRequest.logErrorAndReattemptRead "
                    + "(late error after request close): " + violation.getMessage(), violation);
        }
    }

    // ====================================================================
    // FINDING #3: WriteLacCompletion and ForceLedgerCompletion DON'T override
    //             maybeTimeout(), so they inherit CompletionValue's read-timeout
    //             check — even though PerChannelBookieClient explicitly comments
    //             that they "use addEntryTimeout" (lines 707 / 746).
    //
    // We verify this via reflection: any *Completion class whose owning op should
    // honour add-entry semantics MUST declare its own maybeTimeout. AddCompletion
    // and GetBookieInfoCompletion already do.
    // ====================================================================
    @Test
    public void testWriteCompletionsOverrideMaybeTimeout() throws Exception {
        // Sanity: AddCompletion and GetBookieInfoCompletion are known to override.
        assertOverridesMaybeTimeout("org.apache.bookkeeper.proto.AddCompletion", true);
        assertOverridesMaybeTimeout("org.apache.bookkeeper.proto.GetBookieInfoCompletion", true);

        // Bug claim: these two should override but do NOT.
        // The test fails today and will pass after the fix.
        java.util.List<String> brokenOnes = new ArrayList<>();
        if (!classDeclaresMaybeTimeout("org.apache.bookkeeper.proto.WriteLacCompletion")) {
            brokenOnes.add("WriteLacCompletion");
        }
        if (!classDeclaresMaybeTimeout("org.apache.bookkeeper.proto.ForceLedgerCompletion")) {
            brokenOnes.add("ForceLedgerCompletion");
        }
        if (!brokenOnes.isEmpty()) {
            throw new AssertionError(
                    brokenOnes + " do not override maybeTimeout() — they inherit "
                    + "CompletionValue.maybeTimeout which uses readEntryTimeoutNanos. "
                    + "PerChannelBookieClient.java explicitly states (lines 707, 746) that "
                    + "these operations should use addEntryTimeout. Override maybeTimeout()  "
                    + "to use perChannelBookieClient.addEntryTimeoutNanos.");
        }
    }

    private static boolean classDeclaresMaybeTimeout(String className) throws ClassNotFoundException {
        Class<?> cls = Class.forName(className);
        for (java.lang.reflect.Method m : cls.getDeclaredMethods()) {
            if ("maybeTimeout".equals(m.getName()) && m.getParameterCount() == 0) {
                return true;
            }
        }
        return false;
    }

    private static void assertOverridesMaybeTimeout(String className, boolean expected) throws Exception {
        boolean actual = classDeclaresMaybeTimeout(className);
        assertEquals("Expected " + className + " override=" + expected, expected, actual);
    }

    static class BatchCapture {
        final org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.BatchedReadEntryCallback cb;
        final Object ctx;
        BatchCapture(org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.BatchedReadEntryCallback cb, Object ctx) {
            this.cb = cb; this.ctx = ctx;
        }
    }

    // ====================================================================
    // Tracking WriteSet that records use-after-recycle violations.
    // ====================================================================
    static class TrackingWriteSet implements WriteSet {
        final WriteSet delegate;
        final AtomicBoolean recycled = new AtomicBoolean(false);
        final AtomicReference<Throwable> firstViolation;

        TrackingWriteSet(WriteSet delegate, AtomicReference<Throwable> firstViolation) {
            this.delegate = delegate;
            this.firstViolation = firstViolation;
        }

        private void checkNotRecycled(String op) {
            if (recycled.get()) {
                Throwable t = new IllegalStateException("WriteSet." + op + " called after recycle()");
                t.fillInStackTrace();
                firstViolation.compareAndSet(null, t);
            }
        }

        @Override public int size() { checkNotRecycled("size"); return delegate.size(); }
        @Override public boolean contains(int i) { checkNotRecycled("contains"); return delegate.contains(i); }
        @Override public int get(int i) { checkNotRecycled("get"); return delegate.get(i); }
        @Override public int set(int i, int idx) { checkNotRecycled("set"); return delegate.set(i, idx); }
        @Override public void sort() { checkNotRecycled("sort"); delegate.sort(); }
        @Override public int indexOf(int idx) { checkNotRecycled("indexOf"); return delegate.indexOf(idx); }
        @Override public void addMissingIndices(int max) { checkNotRecycled("addMissingIndices"); delegate.addMissingIndices(max); }
        @Override public void moveAndShift(int from, int to) { checkNotRecycled("moveAndShift"); delegate.moveAndShift(from, to); }
        @Override public void recycle() {
            // Don't actually recycle the delegate — keep its data live so subsequent calls
            // still return the original values. We only flag whether a call occurs.
            recycled.set(true);
        }
        @Override public WriteSet copy() {
            checkNotRecycled("copy");
            return new TrackingWriteSet(delegate.copy(), firstViolation);
        }
    }

    /** Subclass keeps real schedule logic; only writeSet-producing methods are wrapped. */
    static class TrackingDistributionSchedule extends RoundRobinDistributionSchedule {
        final RoundRobinDistributionSchedule delegate;
        final AtomicReference<Throwable> firstViolation = new AtomicReference<>();

        TrackingDistributionSchedule(RoundRobinDistributionSchedule delegate) {
            super(WRITE_QUORUM_SIZE, ACK_QUORUM_SIZE, ENSEMBLE_SIZE);
            this.delegate = delegate;
        }

        @Override public WriteSet getWriteSet(long entryId) {
            return new TrackingWriteSet(delegate.getWriteSet(entryId), firstViolation);
        }
        @Override public WriteSet getEnsembleSet(long entryId) {
            return new TrackingWriteSet(delegate.getEnsembleSet(entryId), firstViolation);
        }
    }
}
