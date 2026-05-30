package gfs.client;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class OffsetMathTest {

    @Test
    void testChunkIndex() {
        assertThat(OffsetMath.chunkIndex(0)).isEqualTo(0);
        assertThat(OffsetMath.chunkIndex(64L * 1024 * 1024 - 1)).isEqualTo(0);
        assertThat(OffsetMath.chunkIndex(64L * 1024 * 1024)).isEqualTo(1);
        assertThat(OffsetMath.chunkIndex(128L * 1024 * 1024 + 500)).isEqualTo(2);

        assertThatThrownBy(() -> OffsetMath.chunkIndex(-1))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testOffsetInChunk() {
        assertThat(OffsetMath.offsetInChunk(0)).isEqualTo(0);
        assertThat(OffsetMath.offsetInChunk(100)).isEqualTo(100);
        assertThat(OffsetMath.offsetInChunk(64L * 1024 * 1024)).isEqualTo(0);
        assertThat(OffsetMath.offsetInChunk(64L * 1024 * 1024 + 1024)).isEqualTo(1024);

        assertThatThrownBy(() -> OffsetMath.offsetInChunk(-1))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testBytesNeededAcrossChunks() {
        // Empty size
        assertThat(OffsetMath.bytesNeededAcrossChunks(100L, 0L)).isEmpty();

        // Fits in a single chunk (within first chunk)
        List<Integer> case1 = OffsetMath.bytesNeededAcrossChunks(10L, 100L);
        assertThat(case1).containsExactly(100);

        // Fits in a single chunk (aligns exactly with chunk boundary)
        long boundaryOffset = 64L * 1024 * 1024 - 100L;
        List<Integer> case2 = OffsetMath.bytesNeededAcrossChunks(boundaryOffset, 100L);
        assertThat(case2).containsExactly(100);

        // Spans two chunks
        List<Integer> case3 = OffsetMath.bytesNeededAcrossChunks(boundaryOffset, 150L);
        assertThat(case3).containsExactly(100, 50);

        // Spans three chunks
        long boundaryOffset2 = 64L * 1024 * 1024 - 10L;
        // Chunk 0: 10 bytes remaining
        // Chunk 1: 64 MB (67108864 bytes)
        // Chunk 2: 15 bytes
        long size = 10L + 64L * 1024 * 1024 + 15L;
        List<Integer> case4 = OffsetMath.bytesNeededAcrossChunks(boundaryOffset2, size);
        assertThat(case4).containsExactly(10, 64 * 1024 * 1024, 15);

        assertThatThrownBy(() -> OffsetMath.bytesNeededAcrossChunks(-1L, 100L))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> OffsetMath.bytesNeededAcrossChunks(100L, -1L))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
