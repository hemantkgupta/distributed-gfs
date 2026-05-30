package gfs.common;

public record WireMessage(
    int reqId,
    MessageType type,
    Object payload
) {}
