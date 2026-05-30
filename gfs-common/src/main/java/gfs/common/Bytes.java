package gfs.common;

import java.util.Arrays;

public final class Bytes {
    private final byte[] data;

    public Bytes(byte[] data) {
        this.data = data == null ? new byte[0] : data.clone();
    }

    public byte[] toByteArray() {
        return data.clone();
    }

    public int length() {
        return data.length;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        return o instanceof Bytes b && Arrays.equals(data, b.data);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(data);
    }

    @Override
    public String toString() {
        return "Bytes{length=" + data.length + "}";
    }
}
