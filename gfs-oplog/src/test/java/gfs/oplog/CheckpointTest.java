package gfs.oplog;

import gfs.common.ChunkHandle;
import gfs.common.ChunkMetadata;
import gfs.common.ChunkVersion;
import gfs.common.ChunkserverId;
import gfs.common.LeaseToken;
import gfs.common.NamespaceEntry;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

class CheckpointTest {

    @TempDir
    Path tempDir;

    @Test
    void testCheckpointWriteAndLoad() throws IOException {
        Path checkpointFile = tempDir.resolve("checkpoint.bin");

        Instant now = Instant.now().truncatedTo(ChronoUnit.MILLIS);

        // 1. Build original mock master state
        List<NamespaceEntry> originalNamespace = List.of(
            new NamespaceEntry("/", true, null, 0, now, now, Optional.empty()),
            new NamespaceEntry("/file.txt", false,
                List.of(new ChunkHandle(100L), new ChunkHandle(200L)),
                50_000_000L, now.minusSeconds(3600), now, Optional.empty()
            ),
            new NamespaceEntry("/deleted.txt", false,
                List.of(), 0, now.minusSeconds(7200), now.minusSeconds(3600),
                Optional.of(now)
            )
        );

        List<ChunkMetadata> originalChunks = List.of(
            new ChunkMetadata(
                new ChunkHandle(100L),
                new ChunkVersion(5),
                Set.of(ChunkserverId.of("localhost", 9001), ChunkserverId.of("localhost", 9002)),
                64 * 1024 * 1024L,
                Optional.of(ChunkserverId.of("localhost", 9001)),
                Optional.of(now.plusSeconds(60))
            ),
            new ChunkMetadata(
                new ChunkHandle(200L),
                new ChunkVersion(1),
                Set.of(ChunkserverId.of("localhost", 9002), ChunkserverId.of("localhost", 9003)),
                1024L,
                Optional.empty(),
                Optional.empty()
            )
        );

        List<LeaseToken> originalLeases = List.of(
            new LeaseToken(
                new ChunkHandle(100L),
                ChunkserverId.of("localhost", 9001),
                now,
                now.plusSeconds(60)
            )
        );

        int lastSequence = 42;

        // 2. Write checkpoint
        Checkpoint.write(checkpointFile, lastSequence, originalNamespace, originalChunks, originalLeases);

        // Verify that the tmp file was cleaned up and only the final checkpoint file exists
        assertThat(checkpointFile).exists();
        assertThat(tempDir.resolve("checkpoint.bin.tmp")).doesNotExist();

        // 3. Load checkpoint
        Checkpoint.CheckpointState loadedState = Checkpoint.load(checkpointFile);

        // 4. Assert deep-equal state
        assertThat(loadedState.lastSequence()).isEqualTo(lastSequence);
        assertThat(loadedState.namespace()).containsExactlyInAnyOrderElementsOf(originalNamespace);
        assertThat(loadedState.chunks()).containsExactlyInAnyOrderElementsOf(originalChunks);
        assertThat(loadedState.leases()).containsExactlyInAnyOrderElementsOf(originalLeases);
    }
}
