package gfs.master;

import gfs.common.*;
import gfs.oplog.Checkpoint;
import gfs.oplog.LogEntry;
import gfs.oplog.OperationLog;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class ShadowMaster implements AutoCloseable {
    private final ConcurrentHashMap<String, NamespaceEntry> namespace = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<ChunkHandle, ChunkMetadata> chunkMap = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<ChunkHandle, LeaseToken> leases = new ConcurrentHashMap<>();

    private final AtomicLong nextChunkHandle = new AtomicLong(1);
    private final AtomicInteger opLogSequence = new AtomicInteger(1);

    private final Clock clock;
    private final Path logFile;
    private final Path checkpointDir;
    private final MasterConfig config;

    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
    private long lastLogFileLength = 0;
    private Instant lastAppliedTimestamp;

    public ShadowMaster(Clock clock, Path checkpointDir, Path logFile, MasterConfig config) throws IOException {
        this.clock = clock;
        this.checkpointDir = checkpointDir;
        this.logFile = logFile;
        this.config = config;
        this.lastAppliedTimestamp = clock.instant();

        // Initialize root directory "/"
        Instant now = clock.instant();
        namespace.put("/", new NamespaceEntry("/", true, null, 0, now, now, Optional.empty()));

        recover();
    }

    public synchronized int recover() throws IOException {
        if (checkpointDir == null || !Files.exists(checkpointDir)) {
            replayLog(0);
            return 0;
        }
        Path latestCheckpoint = Master.findLatestCheckpoint(checkpointDir);
        if (latestCheckpoint == null) {
            replayLog(0);
            return 0;
        }
        Checkpoint.CheckpointState state = Checkpoint.load(latestCheckpoint);
        namespace.clear();
        chunkMap.clear();
        leases.clear();

        for (NamespaceEntry entry : state.namespace()) {
            namespace.put(entry.path(), entry);
        }
        for (ChunkMetadata chunk : state.chunks()) {
            chunkMap.put(chunk.handle(), chunk);
        }
        for (LeaseToken lease : state.leases()) {
            leases.put(lease.chunk(), lease);
        }

        long maxChunkId = 0;
        for (ChunkMetadata chunk : state.chunks()) {
            maxChunkId = Math.max(maxChunkId, chunk.handle().id());
        }
        nextChunkHandle.set(maxChunkId + 1);
        opLogSequence.set(state.lastSequence() + 1);

        replayLog(state.lastSequence());

        if (!namespace.containsKey("/")) {
            Instant now = clock.instant();
            namespace.put("/", new NamespaceEntry("/", true, null, 0, now, now, Optional.empty()));
        }

        return state.lastSequence();
    }

    public synchronized void catchUp() throws IOException {
        int prevSeq = opLogSequence.get() - 1;
        replayLog(prevSeq);
    }

    public synchronized void replayLog(int checkpointSeq) throws IOException {
        if (!Files.exists(logFile)) {
            lastLogFileLength = 0;
            return;
        }
        lastLogFileLength = Files.size(logFile);

        try (OperationLog opLogReader = new OperationLog(logFile, List.of())) {
            opLogReader.replay(entry -> {
                if (entry.sequence() <= checkpointSeq) {
                    opLogSequence.set(Math.max(opLogSequence.get(), entry.sequence() + 1));
                    if (entry instanceof LogEntry.AllocateChunk e) {
                        nextChunkHandle.set(Math.max(nextChunkHandle.get(), e.chunk().id() + 1));
                    }
                    return;
                }
                Instant ts = entry.timestamp();
                opLogSequence.set(Math.max(opLogSequence.get(), entry.sequence() + 1));
                lastAppliedTimestamp = ts;

                if (entry instanceof LogEntry.CreateFile e) {
                    NamespaceEntry newEntry = new NamespaceEntry(
                            e.path(),
                            false,
                            new ArrayList<>(),
                            0,
                            ts,
                            ts,
                            Optional.empty()
                    );
                    namespace.put(e.path(), newEntry);
                } else if (entry instanceof LogEntry.Mkdir e) {
                    NamespaceEntry newEntry = new NamespaceEntry(
                            e.path(),
                            true,
                            null,
                            0,
                            ts,
                            ts,
                            Optional.empty()
                    );
                    namespace.put(e.path(), newEntry);
                } else if (entry instanceof LogEntry.DeleteFile e) {
                    applyDeleteFile(e.path(), ts);
                } else if (entry instanceof LogEntry.AllocateChunk e) {
                    ChunkMetadata chunkMetadata = new ChunkMetadata(
                            e.chunk(),
                            e.version(),
                            e.replicas(),
                            0L,
                            Optional.empty(),
                            Optional.empty()
                    );
                    chunkMap.put(e.chunk(), chunkMetadata);
                    nextChunkHandle.set(Math.max(nextChunkHandle.get(), e.chunk().id() + 1));

                    NamespaceEntry fileEntry = namespace.get(e.path());
                    if (fileEntry != null) {
                        List<ChunkHandle> updatedChunks = new ArrayList<>(fileEntry.chunks());
                        updatedChunks.add(e.chunk());
                        NamespaceEntry newFileEntry = new NamespaceEntry(
                                fileEntry.path(),
                                fileEntry.isDirectory(),
                                updatedChunks,
                                fileEntry.sizeBytes(),
                                fileEntry.ctime(),
                                ts,
                                fileEntry.deletedAt()
                        );
                        namespace.put(e.path(), newFileEntry);
                    }
                } else if (entry instanceof LogEntry.SetChunkReplicas e) {
                    ChunkMetadata chunkMetadata = chunkMap.get(e.chunk());
                    if (chunkMetadata != null) {
                        ChunkMetadata updated = new ChunkMetadata(
                                chunkMetadata.handle(),
                                chunkMetadata.version(),
                                e.replicas(),
                                chunkMetadata.sizeBytes(),
                                chunkMetadata.primary(),
                                chunkMetadata.leaseExpiresAt()
                        );
                        chunkMap.put(e.chunk(), updated);
                    }
                } else if (entry instanceof LogEntry.GrantLease e) {
                    LeaseToken lease = new LeaseToken(e.chunk(), e.primary(), ts, e.expiresAt());
                    leases.put(e.chunk(), lease);

                    ChunkMetadata chunkMetadata = chunkMap.get(e.chunk());
                    if (chunkMetadata != null) {
                        ChunkMetadata updated = new ChunkMetadata(
                                chunkMetadata.handle(),
                                chunkMetadata.version(),
                                chunkMetadata.replicas(),
                                chunkMetadata.sizeBytes(),
                                Optional.of(e.primary()),
                                Optional.of(e.expiresAt())
                        );
                        chunkMap.put(e.chunk(), updated);
                    }
                } else if (entry instanceof LogEntry.RenewLease e) {
                    LeaseToken lease = leases.get(e.chunk());
                    if (lease != null) {
                        LeaseToken renewed = new LeaseToken(lease.chunk(), lease.primary(), lease.grantedAt(), e.newExpiresAt());
                        leases.put(e.chunk(), renewed);

                        ChunkMetadata chunkMetadata = chunkMap.get(e.chunk());
                        if (chunkMetadata != null) {
                            ChunkMetadata updated = new ChunkMetadata(
                                    chunkMetadata.handle(),
                                    chunkMetadata.version(),
                                    chunkMetadata.replicas(),
                                    chunkMetadata.sizeBytes(),
                                    chunkMetadata.primary(),
                                    Optional.of(e.newExpiresAt())
                            );
                            chunkMap.put(e.chunk(), updated);
                        }
                    }
                } else if (entry instanceof LogEntry.RevokeLease e) {
                    leases.remove(e.chunk());
                    ChunkMetadata chunkMetadata = chunkMap.get(e.chunk());
                    if (chunkMetadata != null) {
                        ChunkMetadata updated = new ChunkMetadata(
                                chunkMetadata.handle(),
                                chunkMetadata.version(),
                                chunkMetadata.replicas(),
                                chunkMetadata.sizeBytes(),
                                Optional.empty(),
                                Optional.empty()
                        );
                        chunkMap.put(e.chunk(), updated);
                    }
                } else if (entry instanceof LogEntry.MarkReplicaStale e) {
                    ChunkMetadata chunkMetadata = chunkMap.get(e.chunk());
                    if (chunkMetadata != null) {
                        Set<ChunkserverId> newReplicas = new HashSet<>(chunkMetadata.replicas());
                        newReplicas.remove(e.stale());

                        ChunkMetadata updated = new ChunkMetadata(
                                chunkMetadata.handle(),
                                chunkMetadata.version(),
                                newReplicas,
                                chunkMetadata.sizeBytes(),
                                chunkMetadata.primary().isPresent() && chunkMetadata.primary().get().equals(e.stale()) ? Optional.empty() : chunkMetadata.primary(),
                                chunkMetadata.leaseExpiresAt()
                        );
                        chunkMap.put(e.chunk(), updated);
                    }
                } else if (entry instanceof LogEntry.DeleteChunk e) {
                    chunkMap.remove(e.chunk());
                    leases.remove(e.chunk());
                }
            });
        }
    }

    private void applyDeleteFile(String path, Instant timestamp) {
        long ts = timestamp.toEpochMilli();
        List<NamespaceEntry> toRename = new ArrayList<>();
        for (Map.Entry<String, NamespaceEntry> e : namespace.entrySet()) {
            String k = e.getKey();
            if (k.equals(path) || k.startsWith(path + "/")) {
                if (!e.getValue().deletedAt().isPresent()) {
                    toRename.add(e.getValue());
                }
            }
        }

        for (NamespaceEntry oldEntry : toRename) {
            String oldPath = oldEntry.path();
            String newPath = "/__deleted/" + ts + oldPath;

            NamespaceEntry newEntry = new NamespaceEntry(
                    newPath,
                    oldEntry.isDirectory(),
                    oldEntry.chunks() == null ? null : new ArrayList<>(oldEntry.chunks()),
                    oldEntry.sizeBytes(),
                    oldEntry.ctime(),
                    timestamp,
                    Optional.of(timestamp)
            );

            namespace.remove(oldPath);
            namespace.put(newPath, newEntry);
        }
    }

    public void startTailing(long intervalMs) {
        scheduler.scheduleAtFixedRate(() -> {
            try {
                catchUp();
            } catch (Exception e) {
                // Ignore error in bg thread
            }
        }, intervalMs, intervalMs, TimeUnit.MILLISECONDS);
    }

    public synchronized boolean isLagging() {
        Instant now = clock.instant();
        long currentLength = 0;
        try {
            if (Files.exists(logFile)) {
                currentLength = Files.size(logFile);
            }
        } catch (IOException e) {
            return true;
        }

        if (currentLength > lastLogFileLength) {
            return now.isAfter(lastAppliedTimestamp.plusSeconds(30));
        }

        return false;
    }

    public synchronized NamespaceEntry stat(String rawPath) throws IOException {
        if (isLagging()) {
            throw new IOException("Shadow master is lagging by more than 30 seconds");
        }
        String path = Master.normalizePath(rawPath);
        NamespaceEntry entry = namespace.get(path);
        if (entry == null || entry.deletedAt().isPresent()) {
            return null;
        }
        return entry;
    }

    public synchronized List<LocatedChunk> getChunkLocations(String rawPath, long offset, long size) throws IOException {
        if (isLagging()) {
            throw new IOException("Shadow master is lagging by more than 30 seconds");
        }
        String path = Master.normalizePath(rawPath);
        NamespaceEntry fileEntry = namespace.get(path);
        if (fileEntry == null || fileEntry.isDirectory() || fileEntry.deletedAt().isPresent()) {
            throw new IllegalArgumentException("File not found or is directory: " + path);
        }

        long chunkSizeBytes = config.chunkSizeBytes();
        int startIdx = (int) (offset / chunkSizeBytes);
        int endIdx = (int) ((offset + size - 1) / chunkSizeBytes);

        List<LocatedChunk> locatedChunks = new ArrayList<>();
        for (int i = startIdx; i <= Math.min(endIdx, fileEntry.chunks().size() - 1); i++) {
            ChunkHandle handle = fileEntry.chunks().get(i);
            ChunkMetadata metadata = chunkMap.get(handle);
            if (metadata != null) {
                List<ChunkserverId> replicasOrdered = new ArrayList<>();
                if (metadata.primary().isPresent()) {
                    replicasOrdered.add(metadata.primary().get());
                }
                for (ChunkserverId replica : metadata.replicas()) {
                    if (metadata.primary().isEmpty() || !replica.equals(metadata.primary().get())) {
                        replicasOrdered.add(replica);
                    }
                }
                locatedChunks.add(new LocatedChunk(
                        metadata.handle(),
                        metadata.version(),
                        replicasOrdered
                ));
            }
        }
        return locatedChunks;
    }

    public synchronized WireMessage handleRpc(WireMessage msg) {
        MessageType type = msg.type();
        int reqId = msg.reqId();
        Object payload = msg.payload();

        try {
            if (isLagging()) {
                return new WireMessage(reqId, MessageType.ERROR_RESPONSE, new ErrorResponse("Shadow master is lagging by more than 30 seconds"));
            }
            switch (type) {
                case STAT_REQUEST -> {
                    var req = (StatRequest) payload;
                    var entry = stat(req.path());
                    if (entry == null) {
                        return new WireMessage(reqId, MessageType.ERROR_RESPONSE, new ErrorResponse("Path not found: " + req.path()));
                    }
                    long ctime = entry.ctime().toEpochMilli();
                    long mtime = entry.mtime().toEpochMilli();
                    return new WireMessage(reqId, MessageType.STAT_RESPONSE, new StatResponse(
                            entry.path(),
                            entry.isDirectory(),
                            entry.sizeBytes(),
                            ctime,
                            mtime
                    ));
                }
                case GET_CHUNK_LOCATIONS_REQUEST -> {
                    var req = (GetChunkLocationsRequest) payload;
                    var locations = getChunkLocations(req.path(), req.offset(), req.size());
                    return new WireMessage(reqId, MessageType.GET_CHUNK_LOCATIONS_RESPONSE, new GetChunkLocationsResponse(locations));
                }
                default -> {
                    return new WireMessage(reqId, MessageType.ERROR_RESPONSE, new ErrorResponse("Unsupported shadow master RPC: " + type));
                }
            }
        } catch (Exception e) {
            return new WireMessage(reqId, MessageType.ERROR_RESPONSE, new ErrorResponse(e.getMessage() != null ? e.getMessage() : e.toString()));
        }
    }

    public ConcurrentHashMap<String, NamespaceEntry> getNamespace() {
        return namespace;
    }

    public ConcurrentHashMap<ChunkHandle, ChunkMetadata> getChunkMap() {
        return chunkMap;
    }

    @Override
    public void close() throws Exception {
        scheduler.shutdownNow();
    }
}
