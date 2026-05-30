package gfs.common;

import java.util.List;
import java.util.Map;

public record Heartbeat(
    ChunkserverId id,
    long diskFreeBytes,
    int numChunksHeld,
    List<ChunkHandle> recentlyAddedOrChangedChunks,
    Map<ChunkHandle, Long> chunkSizes,
    boolean leaseRenewalRequested,
    List<ChunkHandle> staleReplicaReports
) {}
