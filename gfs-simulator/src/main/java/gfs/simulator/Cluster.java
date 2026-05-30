package gfs.simulator;

import gfs.chunkserver.ChunkStore;
import gfs.chunkserver.Chunkserver;
import gfs.chunkserver.ChunkserverRpcServer;
import gfs.client.GfsClient;
import gfs.common.ChunkserverId;
import gfs.master.Master;
import gfs.master.MasterConfig;
import gfs.master.MasterRpcServer;
import gfs.oplog.OperationLog;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Cluster implements AutoCloseable {
    private final Path rootDir;
    private final Clock clock;
    private final MasterConfig masterConfig;

    private Master master;
    private MasterRpcServer masterRpc;
    private ChunkserverId masterId;

    private final Map<ChunkserverId, Chunkserver> chunkservers = new ConcurrentHashMap<>();
    private final Map<ChunkserverId, ChunkserverRpcServer> chunkserverRpcs = new ConcurrentHashMap<>();
    private final Map<ChunkserverId, Path> chunkserverDirs = new ConcurrentHashMap<>();
    private final List<ChunkserverId> chunkserverIds = new ArrayList<>();

    private final Map<ChunkserverId, TcpProxy> proxies = new ConcurrentHashMap<>();
    private final Map<ChunkserverId, Integer> realPorts = new ConcurrentHashMap<>();

    private GfsClient client;

    public Cluster(Path rootDir, Clock clock, MasterConfig masterConfig, int numChunkservers) throws Exception {
        this.rootDir = rootDir;
        this.clock = clock;
        this.masterConfig = masterConfig;

        // 1. Setup Master Proxy
        int realMasterPort = getFreePort();
        int proxyMasterPort = getFreePort();
        this.masterId = ChunkserverId.of("127.0.0.1", proxyMasterPort);

        TcpProxy masterProxy = new TcpProxy(proxyMasterPort, realMasterPort);
        this.proxies.put(masterId, masterProxy);
        this.realPorts.put(masterId, realMasterPort);

        // Start Master RPC Server
        Path masterLog = rootDir.resolve("master.log");
        OperationLog opLog = new OperationLog(masterLog, List.of());
        
        Path checkpointDir = rootDir.resolve("checkpoints");
        MasterConfig configWithCheckpoint = new MasterConfig(
                masterConfig.replicationFactor(),
                masterConfig.chunkSizeBytes(),
                masterConfig.leaseDuration(),
                masterConfig.heartbeatTimeout(),
                masterConfig.gcRetentionDuration(),
                checkpointDir
        );

        this.master = new Master(clock, opLog, configWithCheckpoint);
        this.masterRpc = new MasterRpcServer(realMasterPort, this.master);
        this.masterRpc.start();

        // 2. Setup Chunkserver Proxies and Start Chunkservers
        for (int i = 0; i < numChunkservers; i++) {
            int realCsPort = getFreePort();
            int proxyCsPort = getFreePort();
            ChunkserverId csId = ChunkserverId.of("127.0.0.1", proxyCsPort);
            
            Path csDir = rootDir.resolve("cs_" + i);
            Files.createDirectories(csDir);
            this.chunkserverDirs.put(csId, csDir);
            this.chunkserverIds.add(csId);
            this.realPorts.put(csId, realCsPort);

            TcpProxy csProxy = new TcpProxy(proxyCsPort, realCsPort);
            this.proxies.put(csId, csProxy);

            Chunkserver cs = new Chunkserver(csId, new ChunkStore(csDir), clock, masterId);
            ChunkserverRpcServer rpc = new ChunkserverRpcServer(realCsPort, cs);

            this.chunkservers.put(csId, cs);
            this.chunkserverRpcs.put(csId, rpc);

            rpc.start();
            cs.start();
        }

        // Wait for registration
        Thread.sleep(500);

        // 3. Start Client
        int clientPort = getFreePort();
        this.client = new GfsClient(this.masterId, "127.0.0.1", clientPort);
    }

    private static int getFreePort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            socket.setReuseAddress(true);
            return socket.getLocalPort();
        }
    }

    public Master getMaster() { return master; }
    public GfsClient getClient() { return client; }
    public ChunkserverId getMasterId() { return masterId; }
    public List<ChunkserverId> getChunkserverIds() { return chunkserverIds; }
    public Chunkserver getChunkserver(ChunkserverId id) { return chunkservers.get(id); }
    public Path getRootDir() { return rootDir; }
    public Map<ChunkserverId, TcpProxy> getProxies() { return proxies; }

    public synchronized void crashChunkserver(ChunkserverId id) throws Exception {
        ChunkserverRpcServer rpc = chunkserverRpcs.remove(id);
        if (rpc != null) rpc.close();
        Chunkserver cs = chunkservers.remove(id);
        if (cs != null) cs.close();
        
        TcpProxy proxy = proxies.get(id);
        if (proxy != null) {
            proxy.setBlocked(true);
        }
    }

    public synchronized void restartChunkserver(ChunkserverId id, boolean wipeStorage) throws Exception {
        crashChunkserver(id);

        Path csDir = chunkserverDirs.get(id);
        if (wipeStorage) {
            deleteDirectory(csDir);
            Files.createDirectories(csDir);
        }

        int realPort = realPorts.get(id);
        
        TcpProxy proxy = proxies.get(id);
        if (proxy != null) {
            proxy.setBlocked(false);
        }

        Chunkserver cs = new Chunkserver(id, new ChunkStore(csDir), clock, masterId);
        ChunkserverRpcServer rpc = new ChunkserverRpcServer(realPort, cs);

        chunkservers.put(id, cs);
        chunkserverRpcs.put(id, rpc);

        rpc.start();
        cs.start();
    }

    public synchronized void restartMaster() throws Exception {
        if (masterRpc != null) {
            masterRpc.close();
        }
        if (master != null) {
            master.close();
        }
        
        TcpProxy proxy = proxies.get(masterId);
        if (proxy != null) {
            proxy.setBlocked(true);
        }
        
        int realPort = realPorts.get(masterId);
        
        Thread.sleep(100);
        
        if (proxy != null) {
            proxy.setBlocked(false);
        }
        
        Path masterLog = rootDir.resolve("master.log");
        OperationLog opLog = new OperationLog(masterLog, List.of());
        Path checkpointDir = rootDir.resolve("checkpoints");
        MasterConfig configWithCheckpoint = new MasterConfig(
                masterConfig.replicationFactor(),
                masterConfig.chunkSizeBytes(),
                masterConfig.leaseDuration(),
                masterConfig.heartbeatTimeout(),
                masterConfig.gcRetentionDuration(),
                checkpointDir
        );
        
        this.master = new Master(clock, opLog, configWithCheckpoint);
        this.masterRpc = new MasterRpcServer(realPort, this.master);
        this.masterRpc.start();
    }

    private void deleteDirectory(Path path) throws IOException {
        if (Files.exists(path)) {
            try (var walk = Files.walk(path)) {
                walk.sorted(Comparator.reverseOrder())
                    .map(Path::toFile)
                    .forEach(java.io.File::delete);
            }
        }
    }

    @Override
    public void close() throws Exception {
        if (client != null) {
            try { client.close(); } catch (Exception ignored) {}
        }
        for (ChunkserverRpcServer rpc : chunkserverRpcs.values()) {
            try { rpc.close(); } catch (Exception ignored) {}
        }
        for (Chunkserver cs : chunkservers.values()) {
            try { cs.close(); } catch (Exception ignored) {}
        }
        if (masterRpc != null) {
            try { masterRpc.close(); } catch (Exception ignored) {}
        }
        if (master != null) {
            try { master.close(); } catch (Exception ignored) {}
        }
        for (TcpProxy proxy : proxies.values()) {
            try { proxy.close(); } catch (Exception ignored) {}
        }
    }

    public static class TcpProxy implements AutoCloseable {
        private final int proxyPort;
        private final int targetPort;
        private final ServerSocket serverSocket;
        private final ExecutorService executor = Executors.newCachedThreadPool();
        private volatile boolean running = true;
        private volatile boolean blocked = false;
        private volatile long delayMs = 0;

        public TcpProxy(int proxyPort, int targetPort) throws IOException {
            this.proxyPort = proxyPort;
            this.targetPort = targetPort;
            this.serverSocket = new ServerSocket(proxyPort);
            executor.submit(this::acceptConnections);
        }

        private void acceptConnections() {
            while (running) {
                try {
                    Socket clientSocket = serverSocket.accept();
                    if (blocked) {
                        clientSocket.close();
                        continue;
                    }
                    executor.submit(() -> handleConnection(clientSocket));
                } catch (IOException e) {
                    // Closed
                }
            }
        }

        private void handleConnection(Socket clientSocket) {
            try {
                if (delayMs > 0) {
                    Thread.sleep(delayMs);
                }
                if (blocked) {
                    clientSocket.close();
                    return;
                }

                Socket targetSocket = new Socket("127.0.0.1", targetPort);

                executor.submit(() -> bridge(clientSocket, targetSocket));
                executor.submit(() -> bridge(targetSocket, clientSocket));

            } catch (Exception e) {
                try { clientSocket.close(); } catch (IOException ignored) {}
            }
        }

        private void bridge(Socket from, Socket to) {
            try (InputStream in = from.getInputStream();
                 OutputStream out = to.getOutputStream()) {
                byte[] buf = new byte[4096];
                int n;
                while (running && !blocked && (n = in.read(buf)) != -1) {
                    if (delayMs > 0) {
                        Thread.sleep(delayMs);
                    }
                    out.write(buf, 0, n);
                    out.flush();
                }
            } catch (Exception e) {
                // Closed
            } finally {
                try { from.close(); } catch (IOException ignored) {}
                try { to.close(); } catch (IOException ignored) {}
            }
        }

        public void setBlocked(boolean blocked) {
            this.blocked = blocked;
        }

        public void setDelay(long delayMs) {
            this.delayMs = delayMs;
        }

        @Override
        public void close() throws Exception {
            running = false;
            if (serverSocket != null) {
                serverSocket.close();
            }
            executor.shutdownNow();
        }
    }
}
