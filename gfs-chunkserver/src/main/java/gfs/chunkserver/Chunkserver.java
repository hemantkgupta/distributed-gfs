package gfs.chunkserver;

import gfs.common.*;
import gfs.common.ErrorResponse;

import java.io.*;
import java.net.Socket;
import java.time.Clock;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

public class Chunkserver implements AutoCloseable {
    private final ChunkserverId id;
    private final ChunkStore chunkStore;
    private final Clock clock;
    private final ChunkserverId masterId;

    private final ConcurrentHashMap<ChunkHandle, LeaseToken> heldLeases = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<ChunkHandle, byte[]> bufferedBytesMap = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<ChunkHandle, AtomicLong> chunkSerials = new ConcurrentHashMap<>();
    private final List<ChunkserverId> staleReplicaReports = new CopyOnWriteArrayList<>();
    private final Set<ChunkHandle> recentlyAddedChunks = ConcurrentHashMap.newKeySet();

    private final ScheduledExecutorService heartbeatScheduler = Executors.newSingleThreadScheduledExecutor();
    private final ExecutorService taskExecutor = Executors.newCachedThreadPool();
    private volatile boolean running = true;

    public Chunkserver(ChunkserverId id, ChunkStore chunkStore, Clock clock, ChunkserverId masterId) {
        this.id = id;
        this.chunkStore = chunkStore;
        this.clock = clock;
        this.masterId = masterId;
    }

    public void start() {
        heartbeatScheduler.scheduleAtFixedRate(this::sendHeartbeat, 0, 5, TimeUnit.SECONDS);
    }

    public void sendHeartbeat() {
        try {
            List<ChunkHandle> chunksHeld = chunkStore.listChunks();
            Map<ChunkHandle, Long> chunkSizes = new HashMap<>();
            for (ChunkHandle h : chunksHeld) {
                try {
                    chunkSizes.put(h, chunkStore.lengthOf(h));
                } catch (IOException e) {
                    // Ignored
                }
            }

            boolean leaseRenewalRequested = !heldLeases.isEmpty();
            heldLeases.entrySet().removeIf(e -> !e.getValue().isValid(clock.instant()));

            List<ChunkHandle> recentlyAddedList = new ArrayList<>(recentlyAddedChunks);
            recentlyAddedChunks.removeAll(recentlyAddedList);

            List<ChunkHandle> staleReports = new ArrayList<>();
            for (ChunkserverId cs : staleReplicaReports) {
                // If a secondary failed, we report it. Since staleReplicaReports has ChunkserverIds,
                // and GFS heartbeat expects List<ChunkHandle> for staleReplicaReports?
                // Wait! Let's check Heartbeat constructor in gfs-common.
                // In gfs-common, Heartbeat has: List<ChunkHandle> staleReplicaReports.
                // Ah! The list is of ChunkHandles, not ChunkserverIds!
                // Wait, if it's List<ChunkHandle> staleReplicaReports, then how does Master know WHICH replica is stale?
                // In Master.java, handleHeartbeat (line 446):
                // for (ChunkHandle handle : hb.staleReplicaReports()) {
                //     ChunkMetadata metadata = chunkMap.get(handle);
                //     if (metadata != null) {
                //         ... removes all OTHER replicas and marks them stale, leaving only the reporting chunkserver as primary!
                //     }
                // }
                // So the chunkserver reports the ChunkHandle of the chunk that failed.
                // Let's check: yes! In Heartbeat.java, it is List<ChunkHandle> staleReplicaReports.
            }
        } catch (Exception e) {
            // Ignored
        }

        // Real heartbeat implementation:
        try {
            List<ChunkHandle> chunksHeld = chunkStore.listChunks();
            Map<ChunkHandle, Long> chunkSizes = new HashMap<>();
            for (ChunkHandle h : chunksHeld) {
                try {
                    chunkSizes.put(h, chunkStore.lengthOf(h));
                } catch (IOException e) {
                    // Ignored
                }
            }

            boolean leaseRenewalRequested = !heldLeases.isEmpty();
            heldLeases.entrySet().removeIf(e -> !e.getValue().isValid(clock.instant()));

            List<ChunkHandle> recentlyAddedList = new ArrayList<>(recentlyAddedChunks);
            recentlyAddedChunks.removeAll(recentlyAddedList);

            // We need to report the ChunkHandles that had stale replicas.
            // Let's create a thread-safe list of stale ChunkHandles we want to report.
            List<ChunkHandle> staleHandlesToReport = new ArrayList<>();
            // We'll populate this if there are failures. Let's make a set or list for it.
            staleHandlesToReport.addAll(staleHandles);
            staleHandles.removeAll(staleHandlesToReport);

            Heartbeat hb = new Heartbeat(
                    id,
                    1024L * 1024 * 1024 * 10, // 10 GB free space
                    chunksHeld.size(),
                    recentlyAddedList,
                    chunkSizes,
                    leaseRenewalRequested,
                    staleHandlesToReport
            );

            try (Socket socket = connectWithTimeout(masterId, 1000);
                 OutputStream out = socket.getOutputStream();
                 InputStream in = socket.getInputStream()) {
                
                WireMessage req = new WireMessage(0, MessageType.HEARTBEAT, hb);
                WireCodec.encode(req, out);

                WireMessage resp = WireCodec.decode(in);
                if (resp.payload() instanceof HeartbeatAck ack) {
                    processHeartbeatAck(ack);
                }
            }
        } catch (Exception e) {
            // Ignored
        }
    }

    private final Set<ChunkHandle> staleHandles = ConcurrentHashMap.newKeySet();

    private void processHeartbeatAck(HeartbeatAck ack) {
        if (ack.chunksToDelete() != null) {
            for (ChunkHandle h : ack.chunksToDelete()) {
                try {
                    chunkStore.delete(h);
                } catch (IOException e) {
                    // Ignored
                }
            }
        }

        if (ack.chunksToCopy() != null) {
            for (CopyCommand cmd : ack.chunksToCopy()) {
                taskExecutor.submit(() -> runCopyTask(cmd.handle(), cmd.source()));
            }
        }
    }

    public void runCopyTask(ChunkHandle handle, ChunkserverId source) {
        try {
            chunkStore.delete(handle);
            chunkStore.createChunk(handle);

            long currentOffset = 0;
            while (true) {
                byte[] blockData = tryReadBlock(source, handle, currentOffset, 65536);
                if (blockData != null && blockData.length > 0) {
                    chunkStore.writeAt(handle, currentOffset, blockData);
                    currentOffset += blockData.length;
                    if (blockData.length < 65536) {
                        break;
                    }
                } else {
                    int size = binarySearchSize(source, handle, currentOffset);
                    if (size > 0) {
                        byte[] partialData = tryReadBlock(source, handle, currentOffset, size);
                        if (partialData != null) {
                            chunkStore.writeAt(handle, currentOffset, partialData);
                        }
                    }
                    break;
                }
            }

            recentlyAddedChunks.add(handle);
            sendReplicaStateReport(handle);

        } catch (Exception e) {
            // Failed
        }
    }

    private byte[] tryReadBlock(ChunkserverId source, ChunkHandle handle, long offset, int size) {
        try (Socket socket = connectWithTimeout(source, 1000);
             OutputStream out = socket.getOutputStream();
             InputStream in = socket.getInputStream()) {
            
            WireMessage req = new WireMessage(0, MessageType.READ_CHUNK_REQUEST,
                    new ReadChunkRequest(handle, offset, size));
            WireCodec.encode(req, out);

            WireMessage resp = WireCodec.decode(in);
            if (resp.payload() instanceof ReadChunkResponse rcr && rcr.success()) {
                return rcr.data().toByteArray();
            }
        } catch (IOException e) {
            // Ignored
        }
        return null;
    }

    private int binarySearchSize(ChunkserverId source, ChunkHandle handle, long offset) {
        int low = 0;
        int high = 65535;
        int size = 0;
        while (low <= high) {
            int mid = (low + high) / 2;
            if (mid == 0) {
                break;
            }
            byte[] data = tryReadBlock(source, handle, offset, mid);
            if (data != null) {
                size = mid;
                low = mid + 1;
            } else {
                high = mid - 1;
            }
        }
        return size;
    }

    private void sendReplicaStateReport(ChunkHandle handle) {
        try {
            long size = chunkStore.lengthOf(handle);
            ReplicaStateReport report = new ReplicaStateReport(id, handle, ChunkVersion.ZERO, size);
            try (Socket socket = connectWithTimeout(masterId, 1000);
                 OutputStream out = socket.getOutputStream();
                 InputStream in = socket.getInputStream()) {
                WireMessage msg = new WireMessage(0, MessageType.REPLICA_STATE_REPORT, report);
                WireCodec.encode(msg, out);
                WireCodec.decode(in); // Read response to properly complete TCP cycle
            }
        } catch (Exception e) {
            // Ignored
        }
    }

    public boolean hasActiveLease(ChunkHandle handle) {
        LeaseToken lease = heldLeases.get(handle);
        return lease != null && lease.isValid(clock.instant());
    }

    public void grantLeaseLocally(LeaseToken token) {
        heldLeases.put(token.chunk(), token);
    }

    public void revokeLeaseLocally(ChunkHandle handle) {
        heldLeases.remove(handle);
    }

    public byte[] getBufferedBytes(ChunkHandle handle) {
        return bufferedBytesMap.get(handle);
    }

    public void bufferBytes(ChunkHandle handle, byte[] bytes) {
        bufferedBytesMap.put(handle, bytes);
    }

    public WireMessage handleRpc(WireMessage msg) {
        MessageType type = msg.type();
        int reqId = msg.reqId();
        Object payload = msg.payload();

        try {
            switch (type) {
                case READ_CHUNK_REQUEST -> {
                    ReadChunkRequest req = (ReadChunkRequest) payload;
                    try {
                        byte[] data = chunkStore.read(req.handle(), req.offset(), req.size());
                        return new WireMessage(reqId, MessageType.READ_CHUNK_RESPONSE,
                                new ReadChunkResponse(true, new Bytes(data), null));
                    } catch (IOException e) {
                        return new WireMessage(reqId, MessageType.READ_CHUNK_RESPONSE,
                                new ReadChunkResponse(false, new Bytes(new byte[0]), e.getMessage()));
                    }
                }
                case PUSH_BYTES_REQUEST -> {
                    PushBytesRequest req = (PushBytesRequest) payload;
                    bufferedBytesMap.put(req.handle(), req.data().toByteArray());
                    return new WireMessage(reqId, MessageType.PUSH_BYTES_RESPONSE,
                            new PushBytesResponse(true, null));
                }
                case REPLICATE_BYTES -> {
                    ReplicateBytes req = (ReplicateBytes) payload;
                    bufferedBytesMap.put(req.handle(), req.data().toByteArray());
                    if (req.remainingChain() != null && !req.remainingChain().isEmpty()) {
                        ChunkserverId nextTarget = req.remainingChain().get(0);
                        List<ChunkserverId> nextChain = req.remainingChain().subList(1, req.remainingChain().size());
                        try (Socket socket = connectWithTimeout(nextTarget, 1000);
                             OutputStream out = socket.getOutputStream();
                             InputStream in = socket.getInputStream()) {
                            
                            WireMessage forwardMsg = new WireMessage(reqId, MessageType.REPLICATE_BYTES,
                                    new ReplicateBytes(req.handle(), req.data(), nextChain));
                            WireCodec.encode(forwardMsg, out);

                            WireMessage resp = WireCodec.decode(in);
                            if (resp.payload() instanceof PushBytesResponse pbr && pbr.success()) {
                                return new WireMessage(reqId, MessageType.PUSH_BYTES_RESPONSE,
                                        new PushBytesResponse(true, null));
                            } else {
                                String err = (resp.payload() instanceof PushBytesResponse pbr) ? pbr.errorMessage() : "Unknown error forwarding";
                                return new WireMessage(reqId, MessageType.PUSH_BYTES_RESPONSE,
                                        new PushBytesResponse(false, err));
                            }
                        } catch (IOException e) {
                            return new WireMessage(reqId, MessageType.PUSH_BYTES_RESPONSE,
                                    new PushBytesResponse(false, "Forwarding failed to " + nextTarget + ": " + e.getMessage()));
                        }
                    } else {
                        return new WireMessage(reqId, MessageType.PUSH_BYTES_RESPONSE,
                                new PushBytesResponse(true, null));
                    }
                }
                case APPLY_MUTATION -> {
                    ApplyMutation req = (ApplyMutation) payload;
                    try {
                        byte[] buffered = bufferedBytesMap.get(req.handle());
                        byte[] bytesToWrite;
                        if (buffered == null || req.size() != buffered.length) {
                            bytesToWrite = new byte[req.size()]; // padding/zeroes
                        } else {
                            bytesToWrite = buffered;
                        }
                        chunkStore.writeAt(req.handle(), req.offset(), bytesToWrite);
                        return new WireMessage(reqId, MessageType.APPLY_ACK,
                                new ApplyAck(req.handle(), req.serial(), true, null));
                    } catch (IOException e) {
                        return new WireMessage(reqId, MessageType.APPLY_ACK,
                                new ApplyAck(req.handle(), req.serial(), false, e.getMessage()));
                    }
                }
                case COMMIT_WRITE_REQUEST -> {
                    CommitWriteRequest req = (CommitWriteRequest) payload;
                    if (!hasActiveLease(req.handle())) {
                        return new WireMessage(reqId, MessageType.COMMIT_RESPONSE,
                                new CommitResponse(false, 0L, "Not the lease holder or lease expired"));
                    }

                    long serial = chunkSerials.computeIfAbsent(req.handle(), h -> new AtomicLong(0)).incrementAndGet();

                    if (req.secondaries() != null) {
                        for (ChunkserverId sec : req.secondaries()) {
                            try (Socket socket = connectWithTimeout(sec, 1000);
                                 OutputStream out = socket.getOutputStream();
                                 InputStream in = socket.getInputStream()) {
                                
                                WireMessage applyMsg = new WireMessage(reqId, MessageType.APPLY_MUTATION,
                                        new ApplyMutation(req.handle(), serial, req.offset(), req.size()));
                                WireCodec.encode(applyMsg, out);

                                WireMessage ackMsg = WireCodec.decode(in);
                                if (ackMsg.payload() instanceof ApplyAck ack) {
                                    if (!ack.success()) {
                                        staleHandles.add(req.handle());
                                        return new WireMessage(reqId, MessageType.COMMIT_RESPONSE,
                                                new CommitResponse(false, 0L, "Secondary failed to apply: " + ack.errorMessage()));
                                    }
                                } else {
                                    return new WireMessage(reqId, MessageType.COMMIT_RESPONSE,
                                            new CommitResponse(false, 0L, "Invalid ack from secondary"));
                                }
                            } catch (IOException e) {
                                staleHandles.add(req.handle());
                                return new WireMessage(reqId, MessageType.COMMIT_RESPONSE,
                                        new CommitResponse(false, 0L, "Failed to connect to secondary: " + sec));
                            }
                        }
                    }

                    try {
                        byte[] buffered = bufferedBytesMap.get(req.handle());
                        byte[] bytesToWrite;
                        if (buffered == null || req.size() != buffered.length) {
                            bytesToWrite = new byte[req.size()];
                        } else {
                            bytesToWrite = buffered;
                        }
                        chunkStore.writeAt(req.handle(), req.offset(), bytesToWrite);
                        return new WireMessage(reqId, MessageType.COMMIT_RESPONSE,
                                new CommitResponse(true, req.offset(), null));
                    } catch (IOException e) {
                        return new WireMessage(reqId, MessageType.COMMIT_RESPONSE,
                                new CommitResponse(false, 0L, "Primary failed to write: " + e.getMessage()));
                    }
                }
                case COMMIT_RECORD_APPEND_REQUEST -> {
                    CommitRecordAppendRequest req = (CommitRecordAppendRequest) payload;
                    if (!hasActiveLease(req.handle())) {
                        return new WireMessage(reqId, MessageType.COMMIT_RESPONSE,
                                new CommitResponse(false, 0L, "Not the lease holder or lease expired"));
                    }

                    byte[] buffered = bufferedBytesMap.get(req.handle());
                    if (buffered == null) {
                        return new WireMessage(reqId, MessageType.COMMIT_RESPONSE,
                                new CommitResponse(false, 0L, "No buffered bytes found for append"));
                    }

                    long offset;
                    try {
                        if (!chunkStore.exists(req.handle())) {
                            chunkStore.createChunk(req.handle());
                        }
                        offset = chunkStore.lengthOf(req.handle());
                    } catch (IOException e) {
                        return new WireMessage(reqId, MessageType.COMMIT_RESPONSE,
                                new CommitResponse(false, 0L, "Failed to get chunk length: " + e.getMessage()));
                    }

                    long serial = chunkSerials.computeIfAbsent(req.handle(), h -> new AtomicLong(0)).incrementAndGet();
                    int recordSize = buffered.length;

                    if (offset + recordSize > 64L * 1024 * 1024) {
                        int paddingSize = (int) (64L * 1024 * 1024 - offset);
                        
                        if (req.secondaries() != null) {
                            for (ChunkserverId sec : req.secondaries()) {
                                try (Socket socket = connectWithTimeout(sec, 1000);
                                     OutputStream out = socket.getOutputStream();
                                     InputStream in = socket.getInputStream()) {
                                    
                                    WireMessage applyMsg = new WireMessage(reqId, MessageType.APPLY_MUTATION,
                                            new ApplyMutation(req.handle(), serial, offset, paddingSize));
                                    WireCodec.encode(applyMsg, out);

                                    WireMessage ackMsg = WireCodec.decode(in);
                                    if (ackMsg.payload() instanceof ApplyAck ack) {
                                        if (!ack.success()) {
                                            staleHandles.add(req.handle());
                                        }
                                    }
                                } catch (IOException e) {
                                    staleHandles.add(req.handle());
                                }
                            }
                        }

                        try {
                            chunkStore.writeAt(req.handle(), offset, new byte[paddingSize]);
                        } catch (IOException e) {
                            return new WireMessage(reqId, MessageType.COMMIT_RESPONSE,
                                    new CommitResponse(false, 0L, "Failed to apply padding locally: " + e.getMessage()));
                        }

                        return new WireMessage(reqId, MessageType.COMMIT_RESPONSE,
                                new CommitResponse(false, offset, "RETRY_ON_NEXT_CHUNK"));
                    }

                    if (req.secondaries() != null) {
                        for (ChunkserverId sec : req.secondaries()) {
                             try (Socket socket = connectWithTimeout(sec, 1000);
                                  OutputStream out = socket.getOutputStream();
                                  InputStream in = socket.getInputStream()) {
                                
                                WireMessage applyMsg = new WireMessage(reqId, MessageType.APPLY_MUTATION,
                                        new ApplyMutation(req.handle(), serial, offset, recordSize));
                                WireCodec.encode(applyMsg, out);

                                WireMessage ackMsg = WireCodec.decode(in);
                                if (ackMsg.payload() instanceof ApplyAck ack) {
                                    if (!ack.success()) {
                                        staleHandles.add(req.handle());
                                        return new WireMessage(reqId, MessageType.COMMIT_RESPONSE,
                                                new CommitResponse(false, 0L, "Secondary failed to append: " + ack.errorMessage()));
                                    }
                                } else {
                                    return new WireMessage(reqId, MessageType.COMMIT_RESPONSE,
                                            new CommitResponse(false, 0L, "Invalid ack from secondary"));
                                }
                            } catch (IOException e) {
                                staleHandles.add(req.handle());
                                return new WireMessage(reqId, MessageType.COMMIT_RESPONSE,
                                        new CommitResponse(false, 0L, "Failed to connect to secondary: " + sec));
                            }
                        }
                    }

                    try {
                        chunkStore.writeAt(req.handle(), offset, buffered);
                        return new WireMessage(reqId, MessageType.COMMIT_RESPONSE,
                                new CommitResponse(true, offset, null));
                    } catch (IOException e) {
                        return new WireMessage(reqId, MessageType.COMMIT_RESPONSE,
                                new CommitResponse(false, 0L, "Primary failed to append locally: " + e.getMessage()));
                    }
                }
                case GRANT_LEASE -> {
                    GrantLease gl = (GrantLease) payload;
                    Instant grantedAt = Instant.ofEpochMilli(gl.grantedAtEpochMillis());
                    Instant expiresAt = Instant.ofEpochMilli(gl.expiresAtEpochMillis());
                    LeaseToken token = new LeaseToken(gl.chunk(), gl.primary(), grantedAt, expiresAt);
                    heldLeases.put(gl.chunk(), token);
                    return null;
                }
                case REVOKE_LEASE -> {
                    RevokeLease rl = (RevokeLease) payload;
                    heldLeases.remove(rl.chunk());
                    return null;
                }
                case COPY_CHUNK -> {
                    CopyChunk cc = (CopyChunk) payload;
                    taskExecutor.submit(() -> runCopyTask(cc.chunk(), cc.source()));
                    return null;
                }
                case DELETE_CHUNK -> {
                    DeleteChunk dc = (DeleteChunk) payload;
                    try {
                        chunkStore.delete(dc.chunk());
                    } catch (IOException e) {
                        // Ignored
                    }
                    return null;
                }
                default -> {
                    return new WireMessage(reqId, MessageType.ERROR_RESPONSE,
                            new ErrorResponse("Unsupported chunkserver RPC message type: " + type));
                }
            }
        } catch (Exception e) {
            return new WireMessage(reqId, MessageType.ERROR_RESPONSE,
                    new ErrorResponse(e.getMessage() != null ? e.getMessage() : e.toString()));
        }
    }

    public ChunkserverId getId() {
        return id;
    }

    public ChunkStore getChunkStore() {
        return chunkStore;
    }

    private Socket connectWithTimeout(ChunkserverId target, int timeoutMs) throws IOException {
        Socket socket = new Socket();
        try {
            socket.connect(new java.net.InetSocketAddress(target.host(), target.port()), timeoutMs);
            socket.setSoTimeout(timeoutMs);
            return socket;
        } catch (IOException e) {
            socket.close();
            throw e;
        }
    }

    @Override
    public void close() throws Exception {
        running = false;
        heartbeatScheduler.shutdownNow();
        taskExecutor.shutdownNow();
    }
}
