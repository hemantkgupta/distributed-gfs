package gfs.common;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

public record FilePath(String path) {
    public FilePath {
        Objects.requireNonNull(path, "path must not be null");
    }

    public boolean isAbsolute() {
        return path.startsWith("/");
    }

    public List<String> parts() {
        if (path.equals("/")) {
            return List.of();
        }
        String[] split = path.split("/");
        return Arrays.stream(split)
                .filter(s -> !s.isEmpty())
                .toList();
    }
}
