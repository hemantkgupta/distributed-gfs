package gfs.common;

import java.util.Objects;

public record ChunkserverId(String hostPort) {
    public ChunkserverId {
        Objects.requireNonNull(hostPort, "hostPort must not be null");
        if (!hostPort.contains(":")) {
            throw new IllegalArgumentException("hostPort must be in host:port format: " + hostPort);
        }
    }

    public static ChunkserverId of(String host, int port) {
        return new ChunkserverId(host + ":" + port);
    }

    public String host() {
        int idx = hostPort.lastIndexOf(':');
        if (idx == -1) return hostPort;
        return hostPort.substring(0, idx);
    }

    public int port() {
        int idx = hostPort.lastIndexOf(':');
        if (idx == -1) return 0;
        try {
            return Integer.parseInt(hostPort.substring(idx + 1));
        } catch (NumberFormatException e) {
            return 0;
        }
    }
}
