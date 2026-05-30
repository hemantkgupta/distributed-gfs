package gfs.master;

import java.nio.file.Path;
import java.time.Duration;

public class MasterConfig {
    private final int replicationFactor;
    private final long chunkSizeBytes;
    private final Duration leaseDuration;
    private final Duration heartbeatTimeout;
    private final Duration gcRetentionDuration;
    private final Path checkpointDir;

    public MasterConfig(
            int replicationFactor,
            long chunkSizeBytes,
            Duration leaseDuration,
            Duration heartbeatTimeout,
            Duration gcRetentionDuration
    ) {
        this(replicationFactor, chunkSizeBytes, leaseDuration, heartbeatTimeout, gcRetentionDuration, null);
    }

    public MasterConfig(
            int replicationFactor,
            long chunkSizeBytes,
            Duration leaseDuration,
            Duration heartbeatTimeout,
            Duration gcRetentionDuration,
            Path checkpointDir
    ) {
        this.replicationFactor = replicationFactor;
        this.chunkSizeBytes = chunkSizeBytes;
        this.leaseDuration = leaseDuration;
        this.heartbeatTimeout = heartbeatTimeout;
        this.gcRetentionDuration = gcRetentionDuration;
        this.checkpointDir = checkpointDir;
    }

    public static MasterConfig defaultTestConfig() {
        return new MasterConfig(
                3,
                64 * 1024 * 1024L,
                Duration.ofSeconds(60),
                Duration.ofSeconds(15),
                Duration.ofDays(3),
                null
        );
    }

    public int replicationFactor() { return replicationFactor; }
    public long chunkSizeBytes() { return chunkSizeBytes; }
    public Duration leaseDuration() { return leaseDuration; }
    public Duration heartbeatTimeout() { return heartbeatTimeout; }
    public Duration gcRetentionDuration() { return gcRetentionDuration; }
    public Path checkpointDir() { return checkpointDir; }
}
