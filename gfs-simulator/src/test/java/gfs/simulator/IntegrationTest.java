package gfs.simulator;

import gfs.common.*;
import gfs.master.Master;
import gfs.master.MasterConfig;
import gfs.master.MutableClock;
import gfs.master.ShadowMaster;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class IntegrationTest {

    @TempDir
    Path tempDir;

    private MutableClock clock;
    private MasterConfig config;
    private Cluster cluster;
    private FaultInjector faultInjector;

    @BeforeEach
    void setUp() throws Exception {
        clock = new MutableClock(Instant.parse("2026-05-24T00:00:00Z"));
        config = new MasterConfig(
                3,
                64 * 1024 * 1024L,
                Duration.ofSeconds(60),
                Duration.ofSeconds(15),
                Duration.ofSeconds(10)
        );
        cluster = new Cluster(tempDir, clock, config, 3);
        faultInjector = new FaultInjector(cluster);
    }

    @AfterEach
    void tearDown() throws Exception {
        if (cluster != null) {
            cluster.close();
        }
    }

    @Test
    void testMasterRecoveryCatchUp() throws Exception {
        var client = cluster.getClient();
        
        assertThat(client.mkdir("/dir1")).isTrue();
        assertThat(client.create("/dir1/file1")).isTrue();
        
        byte[] data1 = "Hello GFS".getBytes();
        client.write("/dir1/file1", 0L, data1);
        
        cluster.getMaster().writeCheckpoint();
        
        assertThat(client.create("/dir1/file2")).isTrue();
        byte[] data2 = "More Data".getBytes();
        client.write("/dir1/file2", 0L, data2);
        
        cluster.restartMaster();
        
        var recoveredClient = cluster.getClient();
        assertThat(recoveredClient.stat("/dir1")).isNotNull();
        assertThat(recoveredClient.stat("/dir1/file1")).isNotNull();
        assertThat(recoveredClient.stat("/dir1/file2")).isNotNull();
        
        assertThat(recoveredClient.read("/dir1/file1", 0L, data1.length)).containsExactly(data1);
        assertThat(recoveredClient.read("/dir1/file2", 0L, data2.length)).containsExactly(data2);
    }

    @Test
    void testShadowMasterLagAndQueries() throws Exception {
        var client = cluster.getClient();
        assertThat(client.mkdir("/dir1")).isTrue();
        assertThat(client.create("/dir1/file1")).isTrue();

        Path checkpointDir = tempDir.resolve("checkpoints");
        Path logFile = tempDir.resolve("master.log");

        ShadowMaster shadow = new ShadowMaster(clock, checkpointDir, logFile, config);

        assertThat(shadow.stat("/dir1/file1")).isNotNull();

        assertThat(client.create("/dir1/file2")).isTrue();

        assertThat(shadow.getNamespace().containsKey("/dir1/file2")).isFalse();

        shadow.catchUp();
        assertThat(shadow.stat("/dir1/file2")).isNotNull();

        assertThat(client.create("/dir1/file3")).isTrue();

        clock.advanceSeconds(31);

        assertThat(shadow.isLagging()).isTrue();
        assertThatThrownBy(() -> shadow.stat("/dir1/file3"))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Shadow master is lagging");

        shadow.catchUp();
        assertThat(shadow.isLagging()).isFalse();
        assertThat(shadow.stat("/dir1/file3")).isNotNull();

        shadow.close();
    }

    @Test
    void testLazyGarbageCollection() throws Exception {
        var client = cluster.getClient();
        assertThat(client.create("/file_gc")).isTrue();
        byte[] data = "GC data".getBytes();
        client.write("/file_gc", 0L, data);

        List<ChunkserverId> csIds = cluster.getChunkserverIds();
        boolean chunkExists = false;
        for (ChunkserverId id : csIds) {
            var store = cluster.getChunkserver(id).getChunkStore();
            if (!store.listChunks().isEmpty()) {
                chunkExists = true;
                break;
            }
        }
        assertThat(chunkExists).isTrue();

        assertThat(client.delete("/file_gc")).isTrue();

        Optional<String> deletedPath = cluster.getMaster().getNamespace().keySet().stream()
                .filter(k -> k.startsWith("/__deleted/"))
                .findFirst();
        assertThat(deletedPath).isPresent();

        clock.advanceSeconds(5);
        cluster.getMaster().runScan();

        assertThat(cluster.getMaster().getNamespace()).containsKey(deletedPath.get());

        clock.advanceSeconds(6);
        cluster.getMaster().runScan();

        assertThat(cluster.getMaster().getNamespace()).doesNotContainKey(deletedPath.get());

        for (ChunkserverId id : csIds) {
            cluster.getChunkserver(id).sendHeartbeat();
        }

        for (ChunkserverId id : csIds) {
            var store = cluster.getChunkserver(id).getChunkStore();
            assertThat(store.listChunks()).isEmpty();
        }
    }

    @Test
    void testAutoReReplication() throws Exception {
        var client = cluster.getClient();
        assertThat(client.create("/replication_file")).isTrue();
        byte[] data = "replication test".getBytes();
        client.write("/replication_file", 0L, data);

        List<LocatedChunk> locations = client.getOrFetchLocations("/replication_file", 0L, 1);
        LocatedChunk chunk = locations.get(0);
        assertThat(chunk.replicas()).hasSize(3);

        ChunkserverId crashedId = chunk.replicas().get(0);
        faultInjector.crashChunkserver(crashedId);

        clock.advanceSeconds(20);
        cluster.getMaster().checkDeadChunkservers();

        cluster.getMaster().runScan();

        faultInjector.restartChunkserver(crashedId, true);

        Thread.sleep(500);

        cluster.getMaster().runScan();

        Thread.sleep(500);

        var newLocations = client.getOrFetchLocations("/replication_file", 0L, 1);
        assertThat(newLocations.get(0).replicas()).hasSize(3);
    }

    @Test
    void testWritePipelineResilienceToTransientErrors() throws Exception {
        var client = cluster.getClient();
        assertThat(client.create("/resilience_file")).isTrue();

        byte[] data = "Resilient Write".getBytes();

        List<LocatedChunk> locations = client.getOrFetchLocations("/resilience_file", 0L, 1);
        LocatedChunk chunk = locations.get(0);
        
        ChunkserverId secondary = chunk.replicas().get(1);

        faultInjector.crashChunkserver(secondary);

        Thread restarterThread = new Thread(() -> {
            try {
                Thread.sleep(100);
                faultInjector.restartChunkserver(secondary, false);
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
        restarterThread.start();

        client.write("/resilience_file", 0L, data);

        restarterThread.join();

        byte[] readData = client.read("/resilience_file", 0L, data.length);
        assertThat(readData).containsExactly(data);
    }
}
