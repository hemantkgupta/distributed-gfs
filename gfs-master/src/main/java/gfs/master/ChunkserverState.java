package gfs.master;

import gfs.common.ChunkHandle;
import gfs.common.ChunkserverId;

import java.time.Instant;
import java.util.Set;

public record ChunkserverState(
        ChunkserverId id,
        Instant lastHeartbeat,
        long diskFreeBytes,
        Set<ChunkHandle> reportedChunks
) {}
