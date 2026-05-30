package gfs.simulator;

import gfs.common.ChunkserverId;

public class FaultInjector {
    private final Cluster cluster;

    public FaultInjector(Cluster cluster) {
        this.cluster = cluster;
    }

    public void crashChunkserver(ChunkserverId id) throws Exception {
        cluster.crashChunkserver(id);
    }

    public void restartChunkserver(ChunkserverId id, boolean wipeStorage) throws Exception {
        cluster.restartChunkserver(id, wipeStorage);
    }

    public void blockNode(ChunkserverId id) {
        Cluster.TcpProxy proxy = cluster.getProxies().get(id);
        if (proxy != null) {
            proxy.setBlocked(true);
        }
    }

    public void unblockNode(ChunkserverId id) {
        Cluster.TcpProxy proxy = cluster.getProxies().get(id);
        if (proxy != null) {
            proxy.setBlocked(false);
        }
    }

    public void setDelay(ChunkserverId id, long delayMs) {
        Cluster.TcpProxy proxy = cluster.getProxies().get(id);
        if (proxy != null) {
            proxy.setDelay(delayMs);
        }
    }

    public void clearFaults() {
        for (Cluster.TcpProxy proxy : cluster.getProxies().values()) {
            proxy.setBlocked(false);
            proxy.setDelay(0);
        }
    }
}
