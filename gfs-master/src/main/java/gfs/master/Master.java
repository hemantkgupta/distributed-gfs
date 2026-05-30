package gfs.master;

import gfs.common.*;
import gfs.oplog.Checkpoint;
import gfs.oplog.LogEntry;
import gfs.oplog.OperationLog;

import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class Master implements AutoCloseable {
    private final ConcurrentHashMap<String, NamespaceEntry> namespace = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<ChunkHandle, ChunkMetadata> chunkMap = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<ChunkserverId, ChunkserverState> chunkservers = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<ChunkHandle, LeaseToken> leases = new ConcurrentHashMap<>();

    private final AtomicLong nextChunkHandle = new AtomicLong(1);
    private final AtomicInteger opLogSequence = new AtomicInteger(1);

    private final Clock clock;
    private final OperationLog opLog;
    private final MasterConfig config;

    private final ConcurrentHashMap<ChunkserverId, List<ChunkHandle>> pendingDeletes = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<ChunkserverId, List<CopyCommand>> pendingCopies = new ConcurrentHashMap<>();
    private final Set<ChunkHandle> pendingReplications = ConcurrentHashMap.newKeySet();

    private final ScheduledExecutorService reaperScheduler = Executors.newSingleThreadScheduledExecutor();

    public Master(Clock clock, OperationLog opLog, MasterConfig config) throws IOException {
        this.clock = clock;
        this.opLog = opLog;
        this.config = config;

        // Initialize root directory "/"
        Instant now = clock.instant();
        namespace.put("/", new NamespaceEntry("/", true, null, 0, now, now, Optional.empty()));

        recover();
    }

    public synchronized boolean createFile(String rawPath) throws IOException {
        String path = normalizePath(rawPath);
        if (path.equals("/")) {
            return false;
        }
        if (namespace.containsKey(path)) {
            return false;
        }
        String parent = getParentPath(path);
        NamespaceEntry parentEntry = namespace.get(parent);
        if (parentEntry == null || !parentEntry.isDirectory() || parentEntry.deletedAt().isPresent()) {
            return false;
        }

        Instant now = clock.instant();
        NamespaceEntry entry = new NamespaceEntry(
                path,
                false,
                new ArrayList<>(),
                0,
                now,
                now,
                Optional.empty()
        );
        namespace.put(path, entry);

        LogEntry.CreateFile logEntry = new LogEntry.CreateFile(
                opLogSequence.getAndIncrement(),
                now,
                path
        );
        if (opLog != null) {
            opLog.append(logEntry, true);
        }
        return true;
    }

    public synchronized boolean mkdir(String rawPath) throws IOException {
        String path = normalizePath(rawPath);
        if (path.equals("/")) {
            return false;
        }
        if (namespace.containsKey(path)) {
            return false;
        }
        String parent = getParentPath(path);
        NamespaceEntry parentEntry = namespace.get(parent);
        if (parentEntry == null || !parentEntry.isDirectory() || parentEntry.deletedAt().isPresent()) {
            return false;
        }

        Instant now = clock.instant();
        NamespaceEntry entry = new NamespaceEntry(
                path,
                true,
                null,
                0,
                now,
                now,
                Optional.empty()
        );
        namespace.put(path, entry);

        LogEntry.Mkdir logEntry = new LogEntry.Mkdir(
                opLogSequence.getAndIncrement(),
                now,
                path
        );
        if (opLog != null) {
            opLog.append(logEntry, true);
        }
        return true;
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

    public synchronized boolean deleteFile(String rawPath) throws IOException {
        String path = normalizePath(rawPath);
        if (path.equals("/")) {
            return false;
        }
        NamespaceEntry entry = namespace.get(path);
        if (entry == null || entry.deletedAt().isPresent()) {
            return false;
        }

        Instant now = clock.instant();
        applyDeleteFile(path, now);

        LogEntry.DeleteFile logEntry = new LogEntry.DeleteFile(
                opLogSequence.getAndIncrement(),
                now,
                path
        );
        if (opLog != null) {
            opLog.append(logEntry, true);
        }
        return true;
    }

    public synchronized NamespaceEntry stat(String rawPath) {
        String path = normalizePath(rawPath);
        NamespaceEntry entry = namespace.get(path);
        if (entry == null || entry.deletedAt().isPresent()) {
            return null;
        }
        return entry;
    }

    public synchronized ChunkMetadata allocateChunkForFile(String rawPath) throws IOException {
        String path = normalizePath(rawPath);
        NamespaceEntry fileEntry = namespace.get(path);
        if (fileEntry == null || fileEntry.isDirectory() || fileEntry.deletedAt().isPresent()) {
            throw new IllegalArgumentException("Invalid file path for chunk allocation: " + path);
        }

        Instant now = clock.instant();
        List<ChunkserverState> healthyServers = chunkservers.values().stream()
                .filter(cs -> cs.lastHeartbeat() != null &&
                        !now.isAfter(cs.lastHeartbeat().plusSeconds(15)))
                .sorted((a, b) -> Long.compare(b.diskFreeBytes(), a.diskFreeBytes()))
                .toList();

        int repFactor = config.replicationFactor();
        Set<ChunkserverId> replicas = new HashSet<>();
        for (int i = 0; i < Math.min(repFactor, healthyServers.size()); i++) {
            replicas.add(healthyServers.get(i).id());
        }

        ChunkHandle handle = new ChunkHandle(nextChunkHandle.getAndIncrement());
        ChunkVersion version = ChunkVersion.ZERO;

        ChunkMetadata metadata = new ChunkMetadata(
                handle,
                version,
                replicas,
                0L,
                Optional.empty(),
                Optional.empty()
        );

        chunkMap.put(handle, metadata);

        List<ChunkHandle> updatedChunks = new ArrayList<>(fileEntry.chunks());
        updatedChunks.add(handle);
        NamespaceEntry newFileEntry = new NamespaceEntry(
                fileEntry.path(),
                fileEntry.isDirectory(),
                updatedChunks,
                fileEntry.sizeBytes(),
                fileEntry.ctime(),
                now,
                fileEntry.deletedAt()
        );
        namespace.put(path, newFileEntry);

        LogEntry.AllocateChunk logEntry = new LogEntry.AllocateChunk(
                opLogSequence.getAndIncrement(),
                now,
                path,
                handle,
                version,
                replicas
        );
        if (opLog != null) {
            opLog.append(logEntry, true);
        }

        return metadata;
    }

    public synchronized List<LocatedChunk> getChunkLocations(String rawPath, long offset, long size) throws IOException {
        String path = normalizePath(rawPath);
        NamespaceEntry fileEntry = namespace.get(path);
        if (fileEntry == null || fileEntry.isDirectory() || fileEntry.deletedAt().isPresent()) {
            throw new IllegalArgumentException("File not found or is directory: " + path);
        }

        long chunkSizeBytes = config.chunkSizeBytes();
        int startIdx = (int) (offset / chunkSizeBytes);
        int endIdx = (int) ((offset + size - 1) / chunkSizeBytes);

        while (endIdx >= fileEntry.chunks().size()) {
            allocateChunkForFile(path);
            fileEntry = namespace.get(path);
        }

        List<LocatedChunk> locatedChunks = new ArrayList<>();
        for (int i = startIdx; i <= endIdx; i++) {
            ChunkHandle handle = fileEntry.chunks().get(i);
            grantLease(handle);
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

    public synchronized LeaseToken grantLease(ChunkHandle chunkHandle) throws IOException {
        ChunkMetadata metadata = chunkMap.get(chunkHandle);
        if (metadata == null) {
            throw new IllegalArgumentException("Chunk not found: " + chunkHandle);
        }

        Instant now = clock.instant();
        LeaseToken activeLease = leases.get(chunkHandle);
        if (activeLease != null && activeLease.isValid(now)) {
            return activeLease;
        }

        List<ChunkserverId> healthyReplicas = metadata.replicas().stream()
                .filter(id -> {
                    ChunkserverState cs = chunkservers.get(id);
                    return cs != null && cs.lastHeartbeat() != null &&
                            !now.isAfter(cs.lastHeartbeat().plusSeconds(15));
                })
                .toList();

        if (healthyReplicas.isEmpty()) {
            if (metadata.replicas().isEmpty()) {
                return null;
            }
            healthyReplicas = new ArrayList<>(metadata.replicas());
        }

        ChunkserverId primary = healthyReplicas.get(0);
        Instant expiresAt = now.plusSeconds(config.leaseDuration().toSeconds());

        LeaseToken lease = new LeaseToken(chunkHandle, primary, now, expiresAt);
        leases.put(chunkHandle, lease);

        ChunkMetadata updatedMetadata = new ChunkMetadata(
                metadata.handle(),
                metadata.version(),
                metadata.replicas(),
                metadata.sizeBytes(),
                Optional.of(primary),
                Optional.of(expiresAt)
        );
        chunkMap.put(chunkHandle, updatedMetadata);

        LogEntry.GrantLease logEntry = new LogEntry.GrantLease(
                opLogSequence.getAndIncrement(),
                now,
                chunkHandle,
                primary,
                expiresAt
        );
        if (opLog != null) {
            opLog.append(logEntry, true);
        }

        GrantLease grantLeaseMsg = new GrantLease(
                chunkHandle,
                primary,
                now.toEpochMilli(),
                expiresAt.toEpochMilli()
        );
        sendMessageToChunkserver(primary, MessageType.GRANT_LEASE, grantLeaseMsg);

        return lease;
    }

    private void renewLeasesForPrimary(ChunkserverId csId) throws IOException {
        Instant now = clock.instant();
        for (LeaseToken lease : leases.values()) {
            if (lease.primary().equals(csId) && lease.isValid(now)) {
                Instant newExpiresAt = now.plusSeconds(config.leaseDuration().toSeconds());
                LeaseToken renewed = new LeaseToken(lease.chunk(), csId, lease.grantedAt(), newExpiresAt);
                leases.put(lease.chunk(), renewed);

                ChunkMetadata metadata = chunkMap.get(lease.chunk());
                if (metadata != null) {
                    ChunkMetadata updated = new ChunkMetadata(
                            metadata.handle(),
                            metadata.version(),
                            metadata.replicas(),
                            metadata.sizeBytes(),
                            metadata.primary(),
                            Optional.of(newExpiresAt)
                    );
                    chunkMap.put(lease.chunk(), updated);
                }

                LogEntry.RenewLease logEntry = new LogEntry.RenewLease(
                        opLogSequence.getAndIncrement(),
                        now,
                        lease.chunk(),
                        newExpiresAt
                );
                if (opLog != null) {
                    opLog.append(logEntry, true);
                }
            }
        }
    }

    public synchronized boolean isLeaseActive(ChunkHandle handle) {
        LeaseToken lease = leases.get(handle);
        return lease != null && lease.isValid(clock.instant());
    }

    public synchronized LeaseToken getLease(ChunkHandle handle) {
        return leases.get(handle);
    }

    public synchronized HeartbeatAck handleHeartbeat(Heartbeat hb) throws IOException {
        ChunkserverId csId = hb.id();
        Instant now = clock.instant();

        Set<ChunkHandle> reportedChunks = hb.chunkSizes().keySet();
        ChunkserverState state = new ChunkserverState(
                csId,
                now,
                hb.diskFreeBytes(),
                reportedChunks
        );
        chunkservers.put(csId, state);

        for (ChunkMetadata metadata : chunkMap.values()) {
            if (metadata.replicas().contains(csId) && !reportedChunks.contains(metadata.handle())) {
                Set<ChunkserverId> newReplicas = new HashSet<>(metadata.replicas());
                newReplicas.remove(csId);

                ChunkMetadata updated = new ChunkMetadata(
                        metadata.handle(),
                        metadata.version(),
                        newReplicas,
                        metadata.sizeBytes(),
                        metadata.primary().isPresent() && metadata.primary().get().equals(csId) ? Optional.empty() : metadata.primary(),
                        metadata.leaseExpiresAt()
                );
                chunkMap.put(metadata.handle(), updated);

                LogEntry.MarkReplicaStale logEntry = new LogEntry.MarkReplicaStale(
                        opLogSequence.getAndIncrement(),
                        now,
                        metadata.handle(),
                        csId
                );
                if (opLog != null) {
                    opLog.append(logEntry, true);
                }
            }
        }

        List<ChunkHandle> toDelete = new ArrayList<>();
        for (ChunkHandle handle : reportedChunks) {
            if (!chunkMap.containsKey(handle)) {
                toDelete.add(handle);
            }
        }

        if (hb.staleReplicaReports() != null) {
            for (ChunkHandle handle : hb.staleReplicaReports()) {
                ChunkMetadata metadata = chunkMap.get(handle);
                if (metadata != null) {
                    Set<ChunkserverId> newReplicas = new HashSet<>();
                    newReplicas.add(csId);

                    for (ChunkserverId replicaId : metadata.replicas()) {
                        if (!replicaId.equals(csId)) {
                            LogEntry.MarkReplicaStale logEntry = new LogEntry.MarkReplicaStale(
                                    opLogSequence.getAndIncrement(),
                                    now,
                                    handle,
                                    replicaId
                            );
                            if (opLog != null) {
                                opLog.append(logEntry, true);
                            }
                        }
                    }

                    ChunkMetadata updated = new ChunkMetadata(
                            metadata.handle(),
                            metadata.version(),
                            newReplicas,
                            metadata.sizeBytes(),
                            Optional.of(csId),
                            metadata.leaseExpiresAt()
                    );
                    chunkMap.put(handle, updated);
                }
            }
        }

        if (hb.leaseRenewalRequested()) {
            renewLeasesForPrimary(csId);
        }

        List<ChunkHandle> deletes = pendingDeletes.remove(csId);
        if (deletes == null) {
            deletes = new ArrayList<>();
        }
        deletes.addAll(toDelete);

        List<CopyCommand> copies = pendingCopies.remove(csId);
        if (copies == null) {
            copies = new ArrayList<>();
        }

        return new HeartbeatAck(deletes, copies);
    }

    public synchronized void handleReplicaStateReport(ReplicaStateReport report) throws IOException {
        ChunkHandle handle = report.handle();
        ChunkMetadata metadata = chunkMap.get(handle);
        if (metadata == null) {
            return;
        }

        Instant now = clock.instant();
        if (report.version().v() < metadata.version().v()) {
            if (metadata.replicas().contains(report.id())) {
                Set<ChunkserverId> newReplicas = new HashSet<>(metadata.replicas());
                newReplicas.remove(report.id());

                ChunkMetadata updated = new ChunkMetadata(
                        metadata.handle(),
                        metadata.version(),
                        newReplicas,
                        metadata.sizeBytes(),
                        metadata.primary().isPresent() && metadata.primary().get().equals(report.id()) ? Optional.empty() : metadata.primary(),
                        metadata.leaseExpiresAt()
                );
                chunkMap.put(handle, updated);

                LogEntry.MarkReplicaStale logEntry = new LogEntry.MarkReplicaStale(
                        opLogSequence.getAndIncrement(),
                        now,
                        handle,
                        report.id()
                );
                if (opLog != null) {
                    opLog.append(logEntry, true);
                }
            }
        } else {
            ChunkVersion newVersion = metadata.version();
            if (report.version().v() > metadata.version().v()) {
                newVersion = report.version();
            }

            if (!metadata.replicas().contains(report.id()) || newVersion.v() > metadata.version().v()) {
                Set<ChunkserverId> newReplicas = new HashSet<>(metadata.replicas());
                newReplicas.add(report.id());

                long newSize = Math.max(metadata.sizeBytes(), report.sizeBytes());

                ChunkMetadata updated = new ChunkMetadata(
                        metadata.handle(),
                        newVersion,
                        newReplicas,
                        newSize,
                        metadata.primary(),
                        metadata.leaseExpiresAt()
                );
                chunkMap.put(handle, updated);

                LogEntry.SetChunkReplicas logEntry = new LogEntry.SetChunkReplicas(
                        opLogSequence.getAndIncrement(),
                        now,
                        handle,
                        newReplicas
                );
                if (opLog != null) {
                    opLog.append(logEntry, true);
                }
            }
        }
        pendingReplications.remove(handle);
    }

    public synchronized void checkDeadChunkservers() throws IOException {
        Instant now = clock.instant();
        List<ChunkserverId> deadServers = new ArrayList<>();
        for (ChunkserverState cs : chunkservers.values()) {
            if (cs.lastHeartbeat() != null && now.isAfter(cs.lastHeartbeat().plusSeconds(15))) {
                deadServers.add(cs.id());
            }
        }

        for (ChunkserverId deadId : deadServers) {
            chunkservers.remove(deadId);
            pendingDeletes.remove(deadId);
            pendingCopies.remove(deadId);

            for (ChunkMetadata metadata : chunkMap.values()) {
                if (metadata.replicas().contains(deadId)) {
                    Set<ChunkserverId> newReplicas = new HashSet<>(metadata.replicas());
                    newReplicas.remove(deadId);

                    ChunkMetadata updated = new ChunkMetadata(
                            metadata.handle(),
                            metadata.version(),
                            newReplicas,
                            metadata.sizeBytes(),
                            metadata.primary().isPresent() && metadata.primary().get().equals(deadId) ? Optional.empty() : metadata.primary(),
                            metadata.leaseExpiresAt()
                    );
                    chunkMap.put(metadata.handle(), updated);

                    LogEntry.MarkReplicaStale logEntry = new LogEntry.MarkReplicaStale(
                            opLogSequence.getAndIncrement(),
                            now,
                            metadata.handle(),
                            deadId
                    );
                    if (opLog != null) {
                        opLog.append(logEntry, true);
                    }
                }
            }
        }
    }

    public synchronized void runScan() throws IOException {
        Instant now = clock.instant();

        checkDeadChunkservers();

        int repFactor = config.replicationFactor();
        for (ChunkMetadata metadata : chunkMap.values()) {
            int currentReplicasCount = metadata.replicas().size();
            if (currentReplicasCount < repFactor) {
                if (pendingReplications.contains(metadata.handle())) {
                    continue;
                }

                List<ChunkserverId> healthyReplicas = metadata.replicas().stream()
                        .filter(id -> {
                            ChunkserverState cs = chunkservers.get(id);
                            return cs != null && cs.lastHeartbeat() != null &&
                                    !now.isAfter(cs.lastHeartbeat().plusSeconds(15));
                        })
                        .toList();

                if (healthyReplicas.isEmpty()) {
                    continue;
                }
                ChunkserverId source = healthyReplicas.get(0);

                List<ChunkserverState> candidateTargets = chunkservers.values().stream()
                        .filter(cs -> cs.lastHeartbeat() != null &&
                                !now.isAfter(cs.lastHeartbeat().plusSeconds(15)))
                        .filter(cs -> !metadata.replicas().contains(cs.id()))
                        .sorted((a, b) -> Long.compare(b.diskFreeBytes(), a.diskFreeBytes()))
                        .toList();

                if (candidateTargets.isEmpty()) {
                    continue;
                }
                ChunkserverId target = candidateTargets.get(0).id();

                CopyCommand cmd = new CopyCommand(metadata.handle(), source);
                pendingCopies.computeIfAbsent(target, k -> new ArrayList<>()).add(cmd);
                pendingReplications.add(metadata.handle());
            }
        }

        Duration retention = config.gcRetentionDuration();
        List<NamespaceEntry> expiredEntries = new ArrayList<>();
        for (NamespaceEntry entry : namespace.values()) {
            if (entry.deletedAt().isPresent()) {
                Instant deletedTime = entry.deletedAt().get();
                if (now.isAfter(deletedTime.plus(retention))) {
                    expiredEntries.add(entry);
                }
            }
        }

        for (NamespaceEntry entry : expiredEntries) {
            namespace.remove(entry.path());

            LogEntry.DeleteFile logEntry = new LogEntry.DeleteFile(
                    opLogSequence.getAndIncrement(),
                    now,
                    entry.path()
            );
            if (opLog != null) {
                opLog.append(logEntry, true);
            }

            if (entry.chunks() != null) {
                for (ChunkHandle handle : entry.chunks()) {
                    chunkMap.remove(handle);
                    leases.remove(handle);

                    LogEntry.DeleteChunk delChunkEntry = new LogEntry.DeleteChunk(
                            opLogSequence.getAndIncrement(),
                            now,
                            handle
                    );
                    if (opLog != null) {
                        opLog.append(delChunkEntry, true);
                    }
                }
            }
        }
    }

    public synchronized void replayLog() throws IOException {
        replayLog(0);
    }

    public synchronized void replayLog(int checkpointSeq) throws IOException {
        if (opLog == null) {
            return;
        }
        opLog.replay(entry -> {
            if (entry.sequence() <= checkpointSeq) {
                opLogSequence.set(Math.max(opLogSequence.get(), entry.sequence() + 1));
                if (entry instanceof LogEntry.AllocateChunk e) {
                    nextChunkHandle.set(Math.max(nextChunkHandle.get(), e.chunk().id() + 1));
                }
                return;
            }
            Instant ts = entry.timestamp();
            opLogSequence.set(Math.max(opLogSequence.get(), entry.sequence() + 1));

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

    public synchronized int recover() throws IOException {
        Path checkpointDir = config.checkpointDir();
        if (checkpointDir == null || !Files.exists(checkpointDir)) {
            replayLog(0);
            return 0;
        }
        Path latestCheckpoint = findLatestCheckpoint(checkpointDir);
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

    public static Path findLatestCheckpoint(Path checkpointDir) throws IOException {
        if (checkpointDir == null || !Files.exists(checkpointDir)) {
            return null;
        }
        try (var stream = Files.list(checkpointDir)) {
            return stream
                    .filter(p -> p.getFileName().toString().matches("checkpoint\\.\\d+\\.bin"))
                    .max(Comparator.comparingInt(p -> {
                        String name = p.getFileName().toString();
                        String seqStr = name.substring(name.indexOf('.') + 1, name.lastIndexOf('.'));
                        return Integer.parseInt(seqStr);
                    }))
                    .orElse(null);
        }
    }

    public synchronized void writeCheckpoint() throws IOException {
        Path checkpointDir = config.checkpointDir();
        if (checkpointDir == null) {
            return;
        }
        Files.createDirectories(checkpointDir);
        int seq = opLogSequence.get() - 1;
        Path checkpointFile = checkpointDir.resolve("checkpoint." + seq + ".bin");
        Checkpoint.write(
                checkpointFile,
                seq,
                namespace.values(),
                chunkMap.values(),
                leases.values()
        );
    }

    public void startReaper(long intervalMs) {
        reaperScheduler.scheduleAtFixedRate(() -> {
            try {
                runScan();
            } catch (Exception e) {
                // Log exception or handle it
            }
        }, intervalMs, intervalMs, TimeUnit.MILLISECONDS);
    }

    public void stopReaper() {
        reaperScheduler.shutdownNow();
    }

    private void sendMessageToChunkserver(ChunkserverId target, MessageType type, Object payload) {
        try (Socket socket = new Socket(target.host(), target.port());
             OutputStream out = socket.getOutputStream()) {
            WireMessage msg = new WireMessage(0, type, payload);
            WireCodec.encode(msg, out);
        } catch (IOException e) {
            // Ignore socket errors for testing/simulation simplicity
        }
    }

    public synchronized WireMessage handleRpc(WireMessage msg) {
        MessageType type = msg.type();
        int reqId = msg.reqId();
        Object payload = msg.payload();

        try {
            switch (type) {
                case GET_CHUNK_LOCATIONS_REQUEST -> {
                    var req = (GetChunkLocationsRequest) payload;
                    var locations = getChunkLocations(req.path(), req.offset(), req.size());
                    return new WireMessage(reqId, MessageType.GET_CHUNK_LOCATIONS_RESPONSE, new GetChunkLocationsResponse(locations));
                }
                case CREATE_FILE_REQUEST -> {
                    var req = (CreateFileRequest) payload;
                    boolean success = createFile(req.path());
                    return new WireMessage(reqId, MessageType.CREATE_FILE_RESPONSE, new CreateFileResponse(success));
                }
                case DELETE_FILE_REQUEST -> {
                    var req = (DeleteFileRequest) payload;
                    boolean success = deleteFile(req.path());
                    return new WireMessage(reqId, MessageType.DELETE_FILE_RESPONSE, new DeleteFileResponse(success));
                }
                case MKDIR_REQUEST -> {
                    var req = (MkdirRequest) payload;
                    boolean success = mkdir(req.path());
                    return new WireMessage(reqId, MessageType.MKDIR_RESPONSE, new MkdirResponse(success));
                }
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
                case HEARTBEAT -> {
                    var hb = (Heartbeat) payload;
                    var ack = handleHeartbeat(hb);
                    return new WireMessage(reqId, MessageType.HEARTBEAT_ACK, ack);
                }
                case REPLICA_STATE_REPORT -> {
                    var report = (ReplicaStateReport) payload;
                    handleReplicaStateReport(report);
                    return new WireMessage(reqId, MessageType.REPLICA_STATE_REPORT, report);
                }
                default -> {
                    return new WireMessage(reqId, MessageType.ERROR_RESPONSE, new ErrorResponse("Unsupported master RPC message type: " + type));
                }
            }
        } catch (Exception e) {
            return new WireMessage(reqId, MessageType.ERROR_RESPONSE, new ErrorResponse(e.getMessage() != null ? e.getMessage() : e.toString()));
        }
    }

    private String getParentPath(String path) {
        int lastSlash = path.lastIndexOf('/');
        if (lastSlash <= 0) {
            return "/";
        }
        return path.substring(0, lastSlash);
    }

    public static String normalizePath(String path) {
        if (path == null) {
            throw new IllegalArgumentException("Path cannot be null");
        }
        path = path.trim();
        if (path.isEmpty()) {
            throw new IllegalArgumentException("Path cannot be empty");
        }
        if (!path.startsWith("/")) {
            throw new IllegalArgumentException("Path must be absolute (start with '/')");
        }
        if (path.equals("/")) {
            return "/";
        }
        if (path.endsWith("/")) {
            path = path.substring(0, path.length() - 1);
        }
        return path;
    }

    public ConcurrentHashMap<String, NamespaceEntry> getNamespace() {
        return namespace;
    }

    public ConcurrentHashMap<ChunkHandle, ChunkMetadata> getChunkMap() {
        return chunkMap;
    }

    public ConcurrentHashMap<ChunkserverId, ChunkserverState> getChunkservers() {
        return chunkservers;
    }

    public ConcurrentHashMap<ChunkHandle, LeaseToken> getLeases() {
        return leases;
    }

    public ConcurrentHashMap<ChunkserverId, List<ChunkHandle>> getPendingDeletes() {
        return pendingDeletes;
    }

    public ConcurrentHashMap<ChunkserverId, List<CopyCommand>> getPendingCopies() {
        return pendingCopies;
    }

    public Set<ChunkHandle> getPendingReplications() {
        return pendingReplications;
    }

    public int getOpLogSequence() {
        return opLogSequence.get();
    }

    @Override
    public synchronized void close() throws Exception {
        stopReaper();
        if (opLog != null) {
            opLog.close();
        }
    }
}
