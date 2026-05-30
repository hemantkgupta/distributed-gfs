package gfs.oplog;

import gfs.common.ChunkHandle;
import gfs.common.ChunkVersion;
import gfs.common.ChunkserverId;
import java.time.Instant;
import java.util.Set;

public sealed interface LogEntry permits
    LogEntry.CreateFile,
    LogEntry.DeleteFile,
    LogEntry.Mkdir,
    LogEntry.AllocateChunk,
    LogEntry.SetChunkReplicas,
    LogEntry.GrantLease,
    LogEntry.RenewLease,
    LogEntry.RevokeLease,
    LogEntry.MarkReplicaStale,
    LogEntry.DeleteChunk {

    int sequence();
    Instant timestamp();

    record CreateFile(int sequence, Instant timestamp, String path) implements LogEntry {}
    record DeleteFile(int sequence, Instant timestamp, String path) implements LogEntry {}
    record Mkdir(int sequence, Instant timestamp, String path) implements LogEntry {}
    
    record AllocateChunk(
        int sequence,
        Instant timestamp,
        String path,
        ChunkHandle chunk,
        ChunkVersion version,
        Set<ChunkserverId> replicas
    ) implements LogEntry {}
    
    record SetChunkReplicas(
        int sequence,
        Instant timestamp,
        ChunkHandle chunk,
        Set<ChunkserverId> replicas
    ) implements LogEntry {}
    
    record GrantLease(
        int sequence,
        Instant timestamp,
        ChunkHandle chunk,
        ChunkserverId primary,
        Instant expiresAt
    ) implements LogEntry {}
    
    record RenewLease(
        int sequence,
        Instant timestamp,
        ChunkHandle chunk,
        Instant newExpiresAt
    ) implements LogEntry {}
    
    record RevokeLease(
        int sequence,
        Instant timestamp,
        ChunkHandle chunk
    ) implements LogEntry {}
    
    record MarkReplicaStale(
        int sequence,
        Instant timestamp,
        ChunkHandle chunk,
        ChunkserverId stale
    ) implements LogEntry {}
    
    record DeleteChunk(
        int sequence,
        Instant timestamp,
        ChunkHandle chunk
    ) implements LogEntry {}
}
