package gfs.oplog;

import gfs.common.ChunkHandle;
import gfs.common.ChunkVersion;
import gfs.common.ChunkserverId;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

class OperationLogTest {

    @TempDir
    Path tempDir;

    @Test
    void testAppendAndReplay() throws IOException {
        Path logFile = tempDir.resolve("active.log");
        List<Path> mirrors = List.of(
            tempDir.resolve("mirror1"),
            tempDir.resolve("mirror2")
        );

        Instant baseTime = Instant.now().truncatedTo(ChronoUnit.MILLIS);

        List<LogEntry> expectedEntries = List.of(
            new LogEntry.CreateFile(1, baseTime, "/file1.txt"),
            new LogEntry.Mkdir(2, baseTime.plusMillis(10), "/dir1"),
            new LogEntry.AllocateChunk(3, baseTime.plusMillis(20), "/file1.txt",
                new ChunkHandle(1001), new ChunkVersion(1),
                Set.of(ChunkserverId.of("localhost", 9001), ChunkserverId.of("localhost", 9002))
            ),
            new LogEntry.GrantLease(4, baseTime.plusMillis(30), new ChunkHandle(1001),
                ChunkserverId.of("localhost", 9001), baseTime.plusSeconds(60)
            ),
            new LogEntry.RenewLease(5, baseTime.plusMillis(40), new ChunkHandle(1001),
                baseTime.plusSeconds(120)
            ),
            new LogEntry.SetChunkReplicas(6, baseTime.plusMillis(50), new ChunkHandle(1001),
                Set.of(ChunkserverId.of("localhost", 9001), ChunkserverId.of("localhost", 9003))
            ),
            new LogEntry.MarkReplicaStale(7, baseTime.plusMillis(60), new ChunkHandle(1001),
                ChunkserverId.of("localhost", 9002)
            ),
            new LogEntry.RevokeLease(8, baseTime.plusMillis(70), new ChunkHandle(1001)),
            new LogEntry.DeleteChunk(9, baseTime.plusMillis(80), new ChunkHandle(1001)),
            new LogEntry.DeleteFile(10, baseTime.plusMillis(90), "/file1.txt")
        );

        // 1. Write the log entries
        try (OperationLog opLog = new OperationLog(logFile, mirrors)) {
            for (LogEntry entry : expectedEntries) {
                opLog.append(entry, true);
            }
        }

        // 2. Replay and verify from the main log file
        List<LogEntry> replayedEntries = new ArrayList<>();
        try (OperationLog opLog = new OperationLog(logFile, List.of())) {
            opLog.replay(replayedEntries::add);
        }

        assertThat(replayedEntries).hasSize(expectedEntries.size());
        for (int i = 0; i < expectedEntries.size(); i++) {
            assertThat(replayedEntries.get(i)).isEqualTo(expectedEntries.get(i));
        }

        // 3. Verify that mirror directories contain the exact same log content
        for (Path mirror : mirrors) {
            Path mirrorLogFile = mirror.resolve("active.log");
            assertThat(mirrorLogFile).exists();
            
            List<LogEntry> mirrorReplayed = new ArrayList<>();
            try (OperationLog opLog = new OperationLog(mirrorLogFile, List.of())) {
                opLog.replay(mirrorReplayed::add);
            }
            assertThat(mirrorReplayed).containsExactlyElementsOf(expectedEntries);
        }
    }
}
