package gfs.chunkserver;

import gfs.common.ChunkHandle;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ChunkStoreTest {

    @TempDir
    Path tempDir;

    private ChunkStore chunkStore;

    @BeforeEach
    void setUp() throws IOException {
        chunkStore = new ChunkStore(tempDir);
    }

    @Test
    void testWriteAndReadRoundtrip() throws IOException {
        ChunkHandle handle = new ChunkHandle(123L);
        byte[] data = "Hello, GFS World!".getBytes();

        chunkStore.writeAt(handle, 0, data);

        assertThat(chunkStore.exists(handle)).isTrue();
        assertThat(chunkStore.lengthOf(handle)).isEqualTo(data.length);

        byte[] readData = chunkStore.read(handle, 0, data.length);
        assertThat(readData).isEqualTo(data);
    }

    @Test
    void testPartialWritesAndExtensions() throws IOException {
        ChunkHandle handle = new ChunkHandle(456L);

        // Write first part
        byte[] part1 = new byte[100];
        Arrays.fill(part1, (byte) 'A');
        chunkStore.writeAt(handle, 0, part1);

        // Write second part with a gap
        byte[] part2 = new byte[200];
        Arrays.fill(part2, (byte) 'B');
        // gap of 50 bytes filled with zeroes
        chunkStore.writeAt(handle, 150, part2);

        assertThat(chunkStore.lengthOf(handle)).isEqualTo(350);

        // Read part1
        byte[] readPart1 = chunkStore.read(handle, 0, 100);
        assertThat(readPart1).isEqualTo(part1);

        // Read the gap
        byte[] readGap = chunkStore.read(handle, 100, 50);
        assertThat(readGap).containsOnly((byte) 0);

        // Read part2
        byte[] readPart2 = chunkStore.read(handle, 150, 200);
        assertThat(readPart2).isEqualTo(part2);
    }

    @Test
    void testChecksumFailureDetection() throws IOException {
        ChunkHandle handle = new ChunkHandle(789L);
        byte[] data = new byte[70000]; // spans across two blocks (64 KB = 65536)
        Arrays.fill(data, (byte) 'X');

        chunkStore.writeAt(handle, 0, data);

        // Read before corruption works fine
        byte[] readBefore = chunkStore.read(handle, 0, data.length);
        assertThat(readBefore).isEqualTo(data);

        // Direct corruption of the chunk file without updating CRC
        Path chunkPath = tempDir.resolve("789.chunk");
        try (FileChannel fc = FileChannel.open(chunkPath, StandardOpenOption.WRITE)) {
            fc.position(500); // within block 0
            fc.write(ByteBuffer.wrap(new byte[]{(byte) 'Y'}));
        }

        // Now reading should fail block 0 validation
        assertThatThrownBy(() -> chunkStore.read(handle, 0, 100))
                .isInstanceOf(ChunkStore.ChecksumMismatchException.class);
    }

    @Test
    void testDelete() throws IOException {
        ChunkHandle handle = new ChunkHandle(999L);
        chunkStore.writeAt(handle, 0, new byte[]{1, 2, 3});

        assertThat(chunkStore.exists(handle)).isTrue();
        chunkStore.delete(handle);
        assertThat(chunkStore.exists(handle)).isFalse();
    }
}
