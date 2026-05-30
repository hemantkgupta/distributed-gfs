package gfs.client;

import gfs.chunkserver.ChunkStore;
import gfs.chunkserver.Chunkserver;
import gfs.chunkserver.ChunkserverRpcServer;
import gfs.common.ChunkserverId;
import gfs.common.StatResponse;
import gfs.master.Master;
import gfs.master.MasterConfig;
import gfs.master.MasterRpcServer;
import gfs.oplog.OperationLog;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.Path;
import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.assertj.core.api.Assertions.assertThat;

class GfsClientTest {

    @TempDir
    Path tempDir;

    private Clock clock;
    private Master master;
    private MasterRpcServer masterRpc;
    private ChunkserverId masterId;

    private Chunkserver cs1;
    private Chunkserver cs2;
    private Chunkserver cs3;

    private ChunkserverRpcServer rpcCS1;
    private ChunkserverRpcServer rpcCS2;
    private ChunkserverRpcServer rpcCS3;

    private GfsClient client;

    private int getFreePort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    @BeforeEach
    void setUp() throws Exception {
        clock = Clock.systemDefaultZone();

        // 1. Start Master
        Path masterLog = tempDir.resolve("master.log");
        OperationLog opLog = new OperationLog(masterLog, List.of());
        MasterConfig config = MasterConfig.defaultTestConfig();
        master = new Master(clock, opLog, config);
        masterRpc = new MasterRpcServer(0, master);
        masterRpc.start();
        masterId = ChunkserverId.of("127.0.0.1", masterRpc.getPort());

        // 2. Start three Chunkservers
        int port1 = getFreePort();
        int port2 = getFreePort();
        int port3 = getFreePort();

        cs1 = new Chunkserver(ChunkserverId.of("127.0.0.1", port1), new ChunkStore(tempDir.resolve("cs1")), clock, masterId);
        cs2 = new Chunkserver(ChunkserverId.of("127.0.0.1", port2), new ChunkStore(tempDir.resolve("cs2")), clock, masterId);
        cs3 = new Chunkserver(ChunkserverId.of("127.0.0.1", port3), new ChunkStore(tempDir.resolve("cs3")), clock, masterId);

        rpcCS1 = new ChunkserverRpcServer(port1, cs1);
        rpcCS2 = new ChunkserverRpcServer(port2, cs2);
        rpcCS3 = new ChunkserverRpcServer(port3, cs3);

        rpcCS1.start();
        rpcCS2.start();
        rpcCS3.start();

        cs1.start();
        cs2.start();
        cs3.start();

        // Give heartbeats a chance to register chunkservers to master
        Thread.sleep(300);

        // 3. Initialize GfsClient
        client = new GfsClient(masterId, "127.0.0.1", getFreePort());
    }

    @AfterEach
    void tearDown() throws Exception {
        if (client != null) {
            client.close();
        }
        if (rpcCS1 != null) rpcCS1.close();
        if (rpcCS2 != null) rpcCS2.close();
        if (rpcCS3 != null) rpcCS3.close();
        if (cs1 != null) cs1.close();
        if (cs2 != null) cs2.close();
        if (cs3 != null) cs3.close();
        if (masterRpc != null) masterRpc.close();
        if (master != null) master.close();
    }

    @Test
    void testCreateDeleteMkdirStat() throws IOException {
        String dir = "/test_dir";
        String file = "/test_dir/file1.bin";

        // Mkdir
        assertThat(client.mkdir(dir)).isTrue();
        StatResponse statDir = client.stat(dir);
        assertThat(statDir).isNotNull();
        assertThat(statDir.isDirectory()).isTrue();

        // Create
        assertThat(client.create(file)).isTrue();
        StatResponse statFile = client.stat(file);
        assertThat(statFile).isNotNull();
        assertThat(statFile.isDirectory()).isFalse();

        // Delete
        assertThat(client.delete(file)).isTrue();
        assertThat(client.stat(file)).isNull();
    }

    @Test
    void testMultiChunkReadWrite() throws IOException {
        String file = "/multichunk.bin";
        assertThat(client.create(file)).isTrue();

        // Write 200 bytes across the first chunk boundary
        long boundaryOffset = OffsetMath.CHUNK_SIZE - 100;
        byte[] writeData = new byte[200];
        for (int i = 0; i < 200; i++) {
            writeData[i] = (byte) (i + 1);
        }

        client.write(file, boundaryOffset, writeData);

        // Read it back
        byte[] readData = client.read(file, boundaryOffset, 200);
        assertThat(readData).containsExactly(writeData);
    }

    @Test
    void testConcurrentRecordAppends() throws Exception {
        String file = "/appendfile.bin";
        assertThat(client.create(file)).isTrue();

        int numThreads = 4;
        int appendsPerThread = 5;
        int recordSize = 128;

        ExecutorService executor = Executors.newFixedThreadPool(numThreads);
        List<Future<List<Long>>> futures = new ArrayList<>();

        for (int t = 0; t < numThreads; t++) {
            final byte val = (byte) (t + 1);
            futures.add(executor.submit(() -> {
                List<Long> offsets = new ArrayList<>();
                byte[] data = new byte[recordSize];
                for (int i = 0; i < recordSize; i++) {
                    data[i] = val;
                }
                for (int a = 0; a < appendsPerThread; a++) {
                    long offset = client.recordAppend(file, data);
                    offsets.add(offset);
                }
                return offsets;
            }));
        }

        List<Long> allOffsets = new ArrayList<>();
        ConcurrentHashMap<Long, Byte> offsetToExpectedVal = new ConcurrentHashMap<>();

        for (int t = 0; t < numThreads; t++) {
            List<Long> threadOffsets = futures.get(t).get();
            assertThat(threadOffsets).hasSize(appendsPerThread);
            byte expectedVal = (byte) (t + 1);
            for (long offset : threadOffsets) {
                allOffsets.add(offset);
                offsetToExpectedVal.put(offset, expectedVal);
            }
        }

        executor.shutdown();

        // Verify each record was written atomically and matches the expected byte value
        for (long offset : allOffsets) {
            byte[] readBytes = client.read(file, offset, recordSize);
            byte expectedVal = offsetToExpectedVal.get(offset);
            for (byte b : readBytes) {
                assertThat(b).isEqualTo(expectedVal);
            }
        }
    }
}
