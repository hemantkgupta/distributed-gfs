package gfs.common;

import org.junit.jupiter.api.Test;
import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ValueTypesTest {

    @Test
    void testChunkHandle() {
        ChunkHandle h1 = ChunkHandle.of(100L);
        ChunkHandle h2 = new ChunkHandle(100L);
        ChunkHandle h3 = ChunkHandle.of(200L);

        assertThat(h1).isEqualTo(h2);
        assertThat(h1.hashCode()).isEqualTo(h2.hashCode());
        assertThat(h1).isNotEqualTo(h3);

        assertThat(h1).isLessThan(h3);
        assertThat(h3).isGreaterThan(h1);
        assertThat(h1.compareTo(h2)).isZero();
    }

    @Test
    void testChunkVersion() {
        ChunkVersion v0 = ChunkVersion.ZERO;
        assertThat(v0.v()).isZero();

        ChunkVersion v1 = v0.next();
        assertThat(v1.v()).isEqualTo(1);

        ChunkVersion v1_dup = new ChunkVersion(1);
        assertThat(v1).isEqualTo(v1_dup);
        assertThat(v1.hashCode()).isEqualTo(v1_dup.hashCode());
        assertThat(v0).isNotEqualTo(v1);
    }

    @Test
    void testChunkserverId() {
        ChunkserverId id = ChunkserverId.of("localhost", 8080);
        assertThat(id.hostPort()).isEqualTo("localhost:8080");
        assertThat(id.host()).isEqualTo("localhost");
        assertThat(id.port()).isEqualTo(8080);

        ChunkserverId id2 = new ChunkserverId("127.0.0.1:9090");
        assertThat(id2.host()).isEqualTo("127.0.0.1");
        assertThat(id2.port()).isEqualTo(9090);

        assertThatThrownBy(() -> new ChunkserverId(null))
                .isInstanceOf(NullPointerException.class);

        assertThatThrownBy(() -> new ChunkserverId("invalidhostport"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testFilePath() {
        FilePath fp1 = new FilePath("/foo/bar/baz");
        assertThat(fp1.isAbsolute()).isTrue();
        assertThat(fp1.parts()).containsExactly("foo", "bar", "baz");

        FilePath fp2 = new FilePath("/");
        assertThat(fp2.isAbsolute()).isTrue();
        assertThat(fp2.parts()).isEmpty();

        FilePath fp3 = new FilePath("relative/path");
        assertThat(fp3.isAbsolute()).isFalse();
        assertThat(fp3.parts()).containsExactly("relative", "path");

        assertThatThrownBy(() -> new FilePath(null))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void testLeaseToken() {
        ChunkHandle handle = ChunkHandle.of(1L);
        ChunkserverId primary = ChunkserverId.of("localhost", 8080);
        Instant now = Instant.now();
        Instant expires = now.plusSeconds(60);

        LeaseToken token = new LeaseToken(handle, primary, now, expires);
        assertThat(token.chunk()).isEqualTo(handle);
        assertThat(token.primary()).isEqualTo(primary);
        assertThat(token.grantedAt()).isEqualTo(now);
        assertThat(token.expiresAt()).isEqualTo(expires);

        assertThat(token.isValid(now.plusSeconds(30))).isTrue();
        assertThat(token.isValid(now.plusSeconds(60))).isFalse();
        assertThat(token.isValid(now.plusSeconds(90))).isFalse();
    }

    @Test
    void testBytesEncapsulationAndEquality() {
        byte[] original = new byte[]{1, 2, 3};
        Bytes bytes = new Bytes(original);

        // Mutating the original array should not affect the Bytes object
        original[0] = 9;
        assertThat(bytes.toByteArray()).containsExactly(1, 2, 3);

        // Mutating the returned array should not affect the Bytes object
        byte[] returned = bytes.toByteArray();
        returned[1] = 9;
        assertThat(bytes.toByteArray()).containsExactly(1, 2, 3);

        // Value-based equality and hashCode
        Bytes bytes2 = new Bytes(new byte[]{1, 2, 3});
        Bytes bytes3 = new Bytes(new byte[]{1, 2, 4});

        assertThat(bytes).isEqualTo(bytes2);
        assertThat(bytes.hashCode()).isEqualTo(bytes2.hashCode());
        assertThat(bytes).isNotEqualTo(bytes3);
        assertThat(bytes.length()).isEqualTo(3);
    }
}
