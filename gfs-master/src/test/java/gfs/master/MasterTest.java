package gfs.master;

import gfs.common.*;
import gfs.oplog.OperationLog;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class MasterTest {

    @TempDir
    Path tempDir;

    private MutableClock clock;
    private OperationLog opLog;
    private MasterConfig config;
    private Master master;

    @BeforeEach
    void setUp() throws IOException {
        clock = new MutableClock(Instant.parse("2026-05-24T00:00:00Z"));
        Path logFile = tempDir.resolve("master.log");
        opLog = new OperationLog(logFile, List.of());
        config = MasterConfig.defaultTestConfig();
        master = new Master(clock, opLog, config);
    }

    @AfterEach
    void tearDown() throws Exception {
        if (master != null) {
            master.close();
        }
    }

    @Test
    void testNamespaceBasic() throws IOException {
        // Root exists
        assertThat(master.stat("/")).isNotNull();
        assertThat(master.stat("/").isDirectory()).isTrue();

        // Create file under non-existent dir fails
        assertThat(master.createFile("/dir1/file1")).isFalse();

        // Mkdir succeeds
        assertThat(master.mkdir("/dir1")).isTrue();
        assertThat(master.stat("/dir1")).isNotNull();
        assertThat(master.stat("/dir1").isDirectory()).isTrue();

        // Create file under dir1 succeeds
        assertThat(master.createFile("/dir1/file1")).isTrue();
        assertThat(master.stat("/dir1/file1")).isNotNull();
        assertThat(master.stat("/dir1/file1").isDirectory()).isFalse();

        // Duplicate creation fails
        assertThat(master.createFile("/dir1/file1")).isFalse();
        assertThat(master.mkdir("/dir1")).isFalse();

        // Delete file moves it to /__deleted/
        assertThat(master.deleteFile("/dir1/file1")).isTrue();
        assertThat(master.stat("/dir1/file1")).isNull();

        // Check renamed path exists in namespace
        Optional<String> deletedKey = master.getNamespace().keySet().stream()
                .filter(k -> k.startsWith("/__deleted/") && k.endsWith("/dir1/file1"))
                .findFirst();
        assertThat(deletedKey).isPresent();
        NamespaceEntry deletedEntry = master.getNamespace().get(deletedKey.get());
        assertThat(deletedEntry.deletedAt()).isPresent();
        assertThat(deletedEntry.deletedAt().get()).isEqualTo(clock.instant());
    }

    @Test
    void testChunkAllocation() throws IOException {
        // Register chunkservers
        Instant now = clock.instant();
        master.handleHeartbeat(new Heartbeat(ChunkserverId.of("127.0.0.1", 5001), 1000L, 0, List.of(), Map.of(), false, List.of()));
        master.handleHeartbeat(new Heartbeat(ChunkserverId.of("127.0.0.1", 5002), 5000L, 0, List.of(), Map.of(), false, List.of()));
        master.handleHeartbeat(new Heartbeat(ChunkserverId.of("127.0.0.1", 5003), 10000L, 0, List.of(), Map.of(), false, List.of()));

        // Dead chunkserver (heartbeat 20s ago)
        master.getChunkservers().put(ChunkserverId.of("127.0.0.1", 5004), new ChunkserverState(
                ChunkserverId.of("127.0.0.1", 5004),
                now.minusSeconds(20),
                20000L,
                Set.of()
        ));

        assertThat(master.createFile("/testfile")).isTrue();

        // Trigger allocation (GetChunkLocations of size requiring first chunk)
        List<LocatedChunk> locations = master.getChunkLocations("/testfile", 0L, 1024L);
        assertThat(locations).hasSize(1);
        LocatedChunk chunk = locations.get(0);
        assertThat(chunk.handle()).isEqualTo(new ChunkHandle(1));
        assertThat(chunk.version()).isEqualTo(ChunkVersion.ZERO);

        // Verify replicas selected based on disk space and excluding the dead server
        // Picked should be 5003 (10k), 5002 (5k), 5001 (1k). 5004 excluded as dead.
        assertThat(chunk.replicas()).containsExactlyInAnyOrder(
                ChunkserverId.of("127.0.0.1", 5001),
                ChunkserverId.of("127.0.0.1", 5002),
                ChunkserverId.of("127.0.0.1", 5003)
        );
    }

    @Test
    void testLeaseManagement() throws IOException {
        // Register chunkservers and allocate chunk
        master.handleHeartbeat(new Heartbeat(ChunkserverId.of("127.0.0.1", 5001), 1000L, 0, List.of(), Map.of(), false, List.of()));
        assertThat(master.createFile("/file")).isTrue();
        ChunkMetadata meta = master.allocateChunkForFile("/file");

        // Grant lease
        LeaseToken lease = master.grantLease(meta.handle());
        assertThat(lease).isNotNull();
        assertThat(lease.primary()).isEqualTo(ChunkserverId.of("127.0.0.1", 5001));
        assertThat(master.isLeaseActive(meta.handle())).isTrue();

        // Advance 30 seconds - lease remains active
        clock.advanceSeconds(30);
        assertThat(master.isLeaseActive(meta.handle())).isTrue();

        // Primary requests lease renewal
        master.handleHeartbeat(new Heartbeat(ChunkserverId.of("127.0.0.1", 5001), 1000L, 1, List.of(), Map.of(meta.handle(), 0L), true, List.of()));
        
        // Advance 40 seconds (total 70 seconds from initial grant, but lease was renewed 40s ago)
        clock.advanceSeconds(40);
        assertThat(master.isLeaseActive(meta.handle())).isTrue();

        // Advance another 30 seconds (total 100s, 70s since renewal) - lease should expire
        clock.advanceSeconds(30);
        assertThat(master.isLeaseActive(meta.handle())).isFalse();
    }

    @Test
    void testHeartbeatReconciliation() throws IOException {
        ChunkserverId csId = ChunkserverId.of("127.0.0.1", 5001);
        master.handleHeartbeat(new Heartbeat(csId, 1000L, 0, List.of(), Map.of(), false, List.of()));
        assertThat(master.createFile("/file")).isTrue();
        ChunkMetadata meta = master.allocateChunkForFile("/file");

        // Master expects csId to have meta.handle() because it was allocated to it.
        // If csId heartbeats and DOES NOT report it in chunkSizes, it's detected as missing/stale.
        master.handleHeartbeat(new Heartbeat(csId, 1000L, 0, List.of(), Map.of(), false, List.of()));

        // Verify csId is removed from replica set
        ChunkMetadata updated = master.getChunkMap().get(meta.handle());
        assertThat(updated.replicas()).isEmpty();

        // Add back via ReplicaStateReport with older version -> marked stale
        master.getChunkMap().put(meta.handle(), new ChunkMetadata(
                meta.handle(),
                new ChunkVersion(2),
                Set.of(csId),
                0L,
                Optional.empty(),
                Optional.empty()
        ));
        // CS reports version 1
        master.handleReplicaStateReport(new ReplicaStateReport(csId, meta.handle(), new ChunkVersion(1), 0L));
        // Verify it was removed
        assertThat(master.getChunkMap().get(meta.handle()).replicas()).isEmpty();

        // CS reports version 2 -> successfully added
        master.handleReplicaStateReport(new ReplicaStateReport(csId, meta.handle(), new ChunkVersion(2), 0L));
        assertThat(master.getChunkMap().get(meta.handle()).replicas()).contains(csId);
    }

    @Test
    void testDeadServerRemoval() throws IOException {
        ChunkserverId cs1 = ChunkserverId.of("127.0.0.1", 5001);
        ChunkserverId cs2 = ChunkserverId.of("127.0.0.1", 5002);
        
        master.handleHeartbeat(new Heartbeat(cs1, 1000L, 0, List.of(), Map.of(), false, List.of()));
        master.handleHeartbeat(new Heartbeat(cs2, 1000L, 0, List.of(), Map.of(), false, List.of()));

        assertThat(master.createFile("/file")).isTrue();
        ChunkMetadata meta = master.allocateChunkForFile("/file");

        // Verify replica set contains both
        assertThat(master.getChunkMap().get(meta.handle()).replicas()).contains(cs1, cs2);

        // Advance 20 seconds, only cs2 heartbeats
        clock.advanceSeconds(20);
        master.handleHeartbeat(new Heartbeat(cs2, 1000L, 1, List.of(), Map.of(meta.handle(), 0L), false, List.of()));

        // Run scanner to identify dead servers
        master.checkDeadChunkservers();

        // cs1 is dead (no heartbeat for 20s), so it should be removed from registries and replica sets
        assertThat(master.getChunkservers()).doesNotContainKey(cs1);
        assertThat(master.getChunkMap().get(meta.handle()).replicas()).containsOnly(cs2);
    }

    @Test
    void testReaperGCAndReReplication() throws Exception {
        ChunkserverId cs1 = ChunkserverId.of("127.0.0.1", 5001);
        ChunkserverId cs2 = ChunkserverId.of("127.0.0.1", 5002);
        ChunkserverId cs3 = ChunkserverId.of("127.0.0.1", 5003);

        master.handleHeartbeat(new Heartbeat(cs1, 1000L, 0, List.of(), Map.of(), false, List.of()));
        master.handleHeartbeat(new Heartbeat(cs2, 1000L, 0, List.of(), Map.of(), false, List.of()));

        assertThat(master.createFile("/gc-file")).isTrue();
        ChunkMetadata meta = master.allocateChunkForFile("/gc-file");

        // 1. Re-replication check: replicas count is 2 (cs1, cs2), target 3.
        // If we register cs3 as healthy, it should be selected for copy.
        master.handleHeartbeat(new Heartbeat(cs3, 1000L, 0, List.of(), Map.of(), false, List.of()));

        master.runScan();

        List<CopyCommand> cs3Copies = master.getPendingCopies().get(cs3);
        assertThat(cs3Copies).isNotNull().hasSize(1);
        assertThat(cs3Copies.get(0).handle()).isEqualTo(meta.handle());
        assertThat(cs3Copies.get(0).source()).isIn(cs1, cs2);

        // 2. GC check
        assertThat(master.deleteFile("/gc-file")).isTrue();
        
        // 2 days later - still retained
        clock.advanceSeconds(2 * 24 * 3600);
        master.runScan();
        // Check file is still present under /__deleted/...
        Optional<String> deletedKey = master.getNamespace().keySet().stream()
                .filter(k -> k.startsWith("/__deleted/"))
                .findFirst();
        assertThat(deletedKey).isPresent();

        // 4 days later (total 6 days) - permanently purged
        clock.advanceSeconds(4 * 24 * 3600);
        master.runScan();

        deletedKey = master.getNamespace().keySet().stream()
                .filter(k -> k.startsWith("/__deleted/"))
                .findFirst();
        assertThat(deletedKey).isEmpty();
        // Chunks are orphaned
        assertThat(master.getChunkMap()).doesNotContainKey(meta.handle());
    }

    @Test
    void testMasterRpcCommunication() throws Exception {
        try (MasterRpcServer rpcServer = new MasterRpcServer(0, master)) {
            rpcServer.start();
            int port = rpcServer.getPort();

            try (Socket socket = new Socket("127.0.0.1", port);
                 OutputStream out = socket.getOutputStream();
                 InputStream in = socket.getInputStream()) {

                // Send create file request
                WireMessage req = new WireMessage(
                        101,
                        MessageType.CREATE_FILE_REQUEST,
                        new CreateFileRequest("/rpc-file")
                );
                WireCodec.encode(req, out);

                // Decode response
                WireMessage resp = WireCodec.decode(in);
                assertThat(resp.reqId()).isEqualTo(101);
                assertThat(resp.type()).isEqualTo(MessageType.CREATE_FILE_RESPONSE);
                assertThat(((CreateFileResponse) resp.payload()).success()).isTrue();

                // Stat the file
                WireMessage statReq = new WireMessage(
                        102,
                        MessageType.STAT_REQUEST,
                        new StatRequest("/rpc-file")
                );
                WireCodec.encode(statReq, out);

                WireMessage statResp = WireCodec.decode(in);
                assertThat(statResp.reqId()).isEqualTo(102);
                assertThat(statResp.type()).isEqualTo(MessageType.STAT_RESPONSE);
                StatResponse payload = (StatResponse) statResp.payload();
                assertThat(payload.path()).isEqualTo("/rpc-file");
                assertThat(payload.isDirectory()).isFalse();
            }
        }
    }

    @Test
    void testMasterCrashRecovery() throws Exception {
        Path checkpointDir = tempDir.resolve("checkpoints");
        Path logFile = tempDir.resolve("master_recovery.log");

        OperationLog myOpLog = new OperationLog(logFile, List.of());
        MasterConfig myConfig = new MasterConfig(
                3,
                64 * 1024 * 1024L,
                Duration.ofSeconds(60),
                Duration.ofSeconds(15),
                Duration.ofDays(3),
                checkpointDir
        );

        Master activeMaster = new Master(clock, myOpLog, myConfig);

        // Mutate state
        assertThat(activeMaster.mkdir("/dir1")).isTrue();
        assertThat(activeMaster.createFile("/dir1/file1")).isTrue();

        // Register a chunkserver and allocate chunk
        ChunkserverId cs1 = ChunkserverId.of("127.0.0.1", 6001);
        activeMaster.handleHeartbeat(new Heartbeat(cs1, 10000L, 0, List.of(), Map.of(), false, List.of()));
        ChunkMetadata meta1 = activeMaster.allocateChunkForFile("/dir1/file1");

        // Grant lease
        activeMaster.grantLease(meta1.handle());

        // Write checkpoint
        activeMaster.writeCheckpoint();

        // Mutate state further
        assertThat(activeMaster.createFile("/dir1/file2")).isTrue();
        ChunkMetadata meta2 = activeMaster.allocateChunkForFile("/dir1/file2");

        // Let's close this master (represents crash)
        activeMaster.close();

        // Restart master with same opLog and config
        // Upon start, it should scan checkpointDir, load checkpoint, and replay remaining log.
        OperationLog recoveryOpLog = new OperationLog(logFile, List.of());
        Master recoveredMaster = new Master(clock, recoveryOpLog, myConfig);

        // Assertions: check state is identical
        // 1. Files are present
        assertThat(recoveredMaster.stat("/dir1")).isNotNull();
        assertThat(recoveredMaster.stat("/dir1/file1")).isNotNull();
        assertThat(recoveredMaster.stat("/dir1/file2")).isNotNull();

        // 2. Allocated chunks are present
        assertThat(recoveredMaster.getChunkMap()).containsKey(meta1.handle());
        assertThat(recoveredMaster.getChunkMap()).containsKey(meta2.handle());

        // 3. Lease is present and active
        assertThat(recoveredMaster.isLeaseActive(meta1.handle())).isTrue();

        recoveredMaster.close();
    }

    @Test
    void testShadowMaster() throws Exception {
        Path checkpointDir = tempDir.resolve("checkpoints_shadow");
        Path logFile = tempDir.resolve("shadow.log");

        OperationLog liveOpLog = new OperationLog(logFile, List.of());
        MasterConfig myConfig = new MasterConfig(
                3,
                64 * 1024 * 1024L,
                Duration.ofSeconds(60),
                Duration.ofSeconds(15),
                Duration.ofDays(3),
                checkpointDir
        );

        Master liveMaster = new Master(clock, liveOpLog, myConfig);

        // Mutate live master
        assertThat(liveMaster.mkdir("/dir1")).isTrue();
        assertThat(liveMaster.createFile("/dir1/file1")).isTrue();

        // Start shadow master
        ShadowMaster shadow = new ShadowMaster(clock, checkpointDir, logFile, myConfig);

        // Before catchUp, shadow has recovered state (which should match the mutations up to shadow's boot)
        assertThat(shadow.stat("/dir1/file1")).isNotNull();

        // Mutate live master further
        assertThat(liveMaster.createFile("/dir1/file2")).isTrue();

        // Shadow doesn't have file2 yet because it hasn't caught up
        assertThat(shadow.getNamespace().containsKey("/dir1/file2")).isFalse();

        // Catch up shadow master
        shadow.catchUp();
        assertThat(shadow.stat("/dir1/file2")).isNotNull();

        // Now test lag
        // 1. Mutate live master to generate new log entries (increases file size)
        assertThat(liveMaster.createFile("/dir1/file3")).isTrue();
        // Shadow is not catching up yet.

        // 2. Advance shadow's clock by 31 seconds.
        clock.advanceSeconds(31);

        // 3. Shadow should now be lagging
        assertThat(shadow.isLagging()).isTrue();
        assertThatThrownBy(() -> shadow.stat("/dir1/file3"))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Shadow master is lagging");

        // 4. Catch up shadow master, it should no longer be lagging
        shadow.catchUp();
        assertThat(shadow.isLagging()).isFalse();
        assertThat(shadow.stat("/dir1/file3")).isNotNull();

        liveMaster.close();
        shadow.close();
    }

    @Test
    void testLazyGarbageCollectionHeartbeat() throws Exception {
        Path checkpointDir = tempDir.resolve("checkpoints_gc");
        Path logFile = tempDir.resolve("gc.log");

        OperationLog myOpLog = new OperationLog(logFile, List.of());
        MasterConfig myConfig = new MasterConfig(
                1,
                64 * 1024 * 1024L,
                Duration.ofSeconds(60),
                Duration.ofSeconds(15),
                Duration.ofSeconds(10), // 10s GC retention
                checkpointDir
        );

        Master myMaster = new Master(clock, myOpLog, myConfig);

        // Register a chunkserver
        ChunkserverId csId = ChunkserverId.of("127.0.0.1", 7001);
        myMaster.handleHeartbeat(new Heartbeat(csId, 10000L, 0, List.of(), Map.of(), false, List.of()));

        // Create a file and allocate a chunk
        assertThat(myMaster.createFile("/file_to_delete")).isTrue();
        ChunkMetadata meta = myMaster.allocateChunkForFile("/file_to_delete");

        // The chunkserver reports it has the chunk
        myMaster.handleHeartbeat(new Heartbeat(csId, 10000L, 1, List.of(meta.handle()), Map.of(meta.handle(), 1024L), false, List.of()));

        // Delete the file
        assertThat(myMaster.deleteFile("/file_to_delete")).isTrue();

        // 5 seconds later: file is still in /__deleted/ and chunk is NOT orphaned yet
        clock.advanceSeconds(5);
        myMaster.runScan();
        assertThat(myMaster.getChunkMap()).containsKey(meta.handle());

        // Heartbeat ack shouldn't delete the chunk yet
        HeartbeatAck ack1 = myMaster.handleHeartbeat(new Heartbeat(csId, 10000L, 1, List.of(meta.handle()), Map.of(meta.handle(), 1024L), false, List.of()));
        assertThat(ack1.chunksToDelete()).doesNotContain(meta.handle());

        // Advance past 10 seconds retention limit (e.g. 6 more seconds, total 11s)
        clock.advanceSeconds(6);
        myMaster.runScan();

        // File and chunk should be permanently purged on master
        assertThat(myMaster.getChunkMap()).doesNotContainKey(meta.handle());

        // Heartbeat should now report the chunk, and master should respond with DELETE_CHUNK
        HeartbeatAck ack2 = myMaster.handleHeartbeat(new Heartbeat(csId, 10000L, 1, List.of(meta.handle()), Map.of(meta.handle(), 1024L), false, List.of()));
        assertThat(ack2.chunksToDelete()).contains(meta.handle());

        myMaster.close();
    }
}
