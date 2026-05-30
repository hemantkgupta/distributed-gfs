package gfs.client;

import gfs.common.ChunkserverId;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class ChainReplicationDriverTest {

    @Test
    void testChainOrdering() {
        ChunkserverId replica1 = ChunkserverId.of("127.0.0.1", 5001);
        ChunkserverId replica2 = ChunkserverId.of("192.168.1.100", 5002);
        ChunkserverId replica3 = ChunkserverId.of("127.0.0.1", 5005);

        List<ChunkserverId> replicas = List.of(replica1, replica2, replica3);

        // Client on same host (127.0.0.1) and port 5002
        // Same-host nodes: replica1 (port diff = 1), replica3 (port diff = 3)
        // Different-host nodes: replica2
        // Sorted: replica1, replica3, replica2
        List<ChunkserverId> sorted = ChainReplicationDriver.sortServers("127.0.0.1", 5002, replicas);
        assertThat(sorted).containsExactly(replica1, replica3, replica2);
    }
}
