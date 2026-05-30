package gfs.client;

import gfs.common.ChunkserverId;

import java.util.ArrayList;
import java.util.List;

public class ChainReplicationDriver {

    private ChainReplicationDriver() {}

    /**
     * Sorts the chunkservers to build an optimal chain replication pipeline.
     * The first server in the sorted list is the closest/lowest latency to the client.
     */
    public static List<ChunkserverId> sortServers(String clientHost, int clientPort, List<ChunkserverId> replicas) {
        List<ChunkserverId> sorted = new ArrayList<>(replicas);
        sorted.sort((a, b) -> {
            boolean aSameHost = a.host().equals(clientHost);
            boolean bSameHost = b.host().equals(clientHost);
            if (aSameHost && !bSameHost) {
                return -1;
            }
            if (!aSameHost && bSameHost) {
                return 1;
            }
            // As a proxy for network distance/latency in simulated cluster, use port difference
            int diffA = Math.abs(a.port() - clientPort);
            int diffB = Math.abs(b.port() - clientPort);
            return Integer.compare(diffA, diffB);
        });
        return sorted;
    }
}
