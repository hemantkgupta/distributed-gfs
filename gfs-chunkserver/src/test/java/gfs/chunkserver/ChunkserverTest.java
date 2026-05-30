package gfs.chunkserver;

import gfs.common.*;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;

class ChunkserverTest {

    @TempDir
    Path tempDir1;
    @TempDir
    Path tempDir2;

    private Clock clock;
    private MockServer mockMaster;
    private ChunkserverId masterId;

    @BeforeEach
    void setUp() {
        clock = Clock.fixed(Instant.now(), ZoneId.systemDefault());
    }

    @AfterEach
    void tearDown() throws Exception {
        if (mockMaster != null) {
            mockMaster.close();
        }
    }

    @Test
    @Timeout(value = 5, unit = TimeUnit.SECONDS)
    void testHeartbeatAndGC() throws Exception {
        // Setup mock master that instructs the chunkserver to delete chunk 999
        ChunkHandle toDelete = new ChunkHandle(999L);
        mockMaster = new MockServer(req -> {
            if (req.type() == MessageType.HEARTBEAT) {
                return new WireMessage(req.reqId(), MessageType.HEARTBEAT_ACK,
                        new HeartbeatAck(List.of(toDelete), List.of()));
            }
            return null;
        });
        masterId = ChunkserverId.of("127.0.0.1", mockMaster.getPort());

        ChunkStore store = new ChunkStore(tempDir1);
        // Pre-create the chunk to delete
        store.writeAt(toDelete, 0, new byte[]{1, 2, 3});
        assertThat(store.exists(toDelete)).isTrue();

        ChunkserverId csId = ChunkserverId.of("127.0.0.1", 0);
        try (Chunkserver chunkserver = new Chunkserver(csId, store, clock, masterId)) {
            // Trigger heartbeat manually
            chunkserver.sendHeartbeat();

            // Give a tiny amount of time for the GC command to be processed
            long start = System.currentTimeMillis();
            while (store.exists(toDelete) && System.currentTimeMillis() - start < 1000) {
                Thread.sleep(10);
            }

            // Verify it was deleted
            assertThat(store.exists(toDelete)).isFalse();
        }
    }

    @Test
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    void testReReplicationCopy() throws Exception {
        ChunkHandle handle = new ChunkHandle(555L);
        byte[] expectedData = "Copied Chunk Content through Binary Search".getBytes();

        // 1. Setup mock source chunkserver
        MockServer mockSourceCS = new MockServer(req -> {
            if (req.type() == MessageType.READ_CHUNK_REQUEST) {
                ReadChunkRequest r = (ReadChunkRequest) req.payload();
                if (r.offset() == 0 && r.size() == 65536) {
                    // Fail full read to trigger binary search (as if file is partial)
                    return new WireMessage(req.reqId(), MessageType.READ_CHUNK_RESPONSE,
                            new ReadChunkResponse(false, new Bytes(new byte[0]), "EOF"));
                } else if (r.offset() == 0 && r.size() == expectedData.length) {
                    // Succeed exact size
                    return new WireMessage(req.reqId(), MessageType.READ_CHUNK_RESPONSE,
                            new ReadChunkResponse(true, new Bytes(expectedData), null));
                } else if (r.offset() == 0 && r.size() < expectedData.length) {
                    // Succeed any smaller size
                    byte[] sub = Arrays.copyOf(expectedData, r.size());
                    return new WireMessage(req.reqId(), MessageType.READ_CHUNK_RESPONSE,
                            new ReadChunkResponse(true, new Bytes(sub), null));
                } else {
                    // Request size is larger than expected data length, fail with EOF
                    return new WireMessage(req.reqId(), MessageType.READ_CHUNK_RESPONSE,
                            new ReadChunkResponse(false, new Bytes(new byte[0]), "EOF"));
                }
            }
            return null;
        });

        ChunkserverId sourceId = ChunkserverId.of("127.0.0.1", mockSourceCS.getPort());

        // 2. Setup mock master that receives the replica state report
        CompletableFuture<ReplicaStateReport> reportFuture = new CompletableFuture<>();
        mockMaster = new MockServer(req -> {
            if (req.type() == MessageType.REPLICA_STATE_REPORT) {
                reportFuture.complete((ReplicaStateReport) req.payload());
                return new WireMessage(req.reqId(), MessageType.REPLICA_STATE_REPORT, req.payload());
            }
            return null;
        });
        masterId = ChunkserverId.of("127.0.0.1", mockMaster.getPort());

        ChunkStore targetStore = new ChunkStore(tempDir1);
        ChunkserverId targetId = ChunkserverId.of("127.0.0.1", 0);

        try (Chunkserver chunkserver = new Chunkserver(targetId, targetStore, clock, masterId)) {
            // Run the copy task directly
            chunkserver.runCopyTask(handle, sourceId);

            // Verify local store has the data
            assertThat(targetStore.exists(handle)).isTrue();
            byte[] readLocal = targetStore.read(handle, 0, expectedData.length);
            assertThat(readLocal).isEqualTo(expectedData);

            // Verify replica state report was sent to master
            ReplicaStateReport report = reportFuture.get(2, TimeUnit.SECONDS);
            assertThat(report.handle()).isEqualTo(handle);
            assertThat(report.sizeBytes()).isEqualTo(expectedData.length);
            assertThat(report.id()).isEqualTo(targetId);
        } finally {
            mockSourceCS.close();
        }
    }

    @Test
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    void testDaisyChainAndCommitSequencing() throws Exception {
        // We will boot three chunkservers (Primary A, Secondary B, Secondary C)
        ChunkStore storeA = new ChunkStore(tempDir1.resolve("A"));
        ChunkStore storeB = new ChunkStore(tempDir1.resolve("B"));
        ChunkStore storeC = new ChunkStore(tempDir1.resolve("C"));

        ChunkserverId masterDummyId = ChunkserverId.of("127.0.0.1", 9999);

        try (Chunkserver serverA = new Chunkserver(ChunkserverId.of("127.0.0.1", 10001), storeA, clock, masterDummyId);
             Chunkserver serverB = new Chunkserver(ChunkserverId.of("127.0.0.1", 10002), storeB, clock, masterDummyId);
             Chunkserver serverC = new Chunkserver(ChunkserverId.of("127.0.0.1", 10003), storeC, clock, masterDummyId);
             ChunkserverRpcServer rpcA = new ChunkserverRpcServer(0, serverA);
             ChunkserverRpcServer rpcB = new ChunkserverRpcServer(0, serverB);
             ChunkserverRpcServer rpcC = new ChunkserverRpcServer(0, serverC)) {

            rpcA.start();
            rpcB.start();
            rpcC.start();

            ChunkserverId idA = ChunkserverId.of("127.0.0.1", rpcA.getPort());
            ChunkserverId idB = ChunkserverId.of("127.0.0.1", rpcB.getPort());
            ChunkserverId idC = ChunkserverId.of("127.0.0.1", rpcC.getPort());

            ChunkHandle handle = new ChunkHandle(777L);
            byte[] dataToPush = "Daisy Chain Data".getBytes();

            // Grant lease to A locally so it is primary
            Instant grantedAt = clock.instant();
            Instant expiresAt = grantedAt.plusSeconds(60);
            serverA.grantLeaseLocally(new LeaseToken(handle, idA, grantedAt, expiresAt));

            // Create chunk initially on all
            storeA.createChunk(handle);
            storeB.createChunk(handle);
            storeC.createChunk(handle);

            // 1. Client pushes bytes to A, chaining to B and C
            try (Socket socket = new Socket("127.0.0.1", rpcA.getPort());
                 OutputStream out = socket.getOutputStream();
                 InputStream in = socket.getInputStream()) {

                List<ChunkserverId> remainingChain = List.of(idB, idC);
                WireMessage pushMsg = new WireMessage(101, MessageType.REPLICATE_BYTES,
                        new ReplicateBytes(handle, new Bytes(dataToPush), remainingChain));
                WireCodec.encode(pushMsg, out);

                WireMessage pushResp = WireCodec.decode(in);
                assertThat(pushResp.type()).isEqualTo(MessageType.PUSH_BYTES_RESPONSE);
                PushBytesResponse pbr = (PushBytesResponse) pushResp.payload();
                assertThat(pbr.success()).isTrue();
            }

            // Verify all nodes buffered the bytes
            assertThat(serverA.getBufferedBytes(handle)).isEqualTo(dataToPush);
            assertThat(serverB.getBufferedBytes(handle)).isEqualTo(dataToPush);
            assertThat(serverC.getBufferedBytes(handle)).isEqualTo(dataToPush);

            // 2. Client commits write to primary A
            try (Socket socket = new Socket("127.0.0.1", rpcA.getPort());
                 OutputStream out = socket.getOutputStream();
                 InputStream in = socket.getInputStream()) {

                WireMessage commitMsg = new WireMessage(102, MessageType.COMMIT_WRITE_REQUEST,
                        new CommitWriteRequest(handle, 0, dataToPush.length, List.of(idB, idC)));
                WireCodec.encode(commitMsg, out);

                WireMessage commitResp = WireCodec.decode(in);
                assertThat(commitResp.type()).isEqualTo(MessageType.COMMIT_RESPONSE);
                CommitResponse cr = (CommitResponse) commitResp.payload();
                assertThat(cr.success()).isTrue();
                assertThat(cr.offset()).isEqualTo(0L);
            }

            // Verify chunk store contents on all three
            assertThat(storeA.read(handle, 0, dataToPush.length)).isEqualTo(dataToPush);
            assertThat(storeB.read(handle, 0, dataToPush.length)).isEqualTo(dataToPush);
            assertThat(storeC.read(handle, 0, dataToPush.length)).isEqualTo(dataToPush);
        }
    }

    static class MockServer implements AutoCloseable {
        private final ServerSocket ss;
        private final ExecutorService executor = Executors.newCachedThreadPool();
        private final Function<WireMessage, WireMessage> handler;
        private volatile boolean running = true;

        MockServer(Function<WireMessage, WireMessage> handler) throws IOException {
            this.ss = new ServerSocket(0);
            this.handler = handler;
            this.executor.submit(this::acceptLoop);
        }

        int getPort() {
            return ss.getLocalPort();
        }

        private void acceptLoop() {
            while (running) {
                try {
                    Socket s = ss.accept();
                    executor.submit(() -> {
                        try (s;
                             InputStream in = s.getInputStream();
                             OutputStream out = s.getOutputStream()) {
                            while (running) {
                                WireMessage req = WireCodec.decode(in);
                                WireMessage resp = handler.apply(req);
                                if (resp != null) {
                                    WireCodec.encode(resp, out);
                                }
                            }
                        } catch (Exception e) {
                            // ignore socket closed/EOF
                        }
                    });
                } catch (IOException e) {
                    break;
                }
            }
        }

        @Override
        public void close() throws IOException {
            running = false;
            ss.close();
            executor.shutdownNow();
        }
    }
}
