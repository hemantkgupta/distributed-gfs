package gfs.chunkserver;

import gfs.common.WireCodec;
import gfs.common.WireMessage;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ChunkserverRpcServer implements AutoCloseable {
    private final ServerSocket serverSocket;
    private final ExecutorService threadPool;
    private final Chunkserver chunkserver;
    private volatile boolean running = true;

    public ChunkserverRpcServer(int port, Chunkserver chunkserver) throws IOException {
        this.serverSocket = new ServerSocket(port);
        this.threadPool = Executors.newCachedThreadPool();
        this.chunkserver = chunkserver;
    }

    public void start() {
        threadPool.submit(this::acceptLoop);
    }

    private void acceptLoop() {
        while (running) {
            try {
                Socket socket = serverSocket.accept();
                threadPool.submit(() -> handleClient(socket));
            } catch (IOException e) {
                if (!running) break;
            }
        }
    }

    private void handleClient(Socket socket) {
        try (socket;
             InputStream in = socket.getInputStream();
             OutputStream out = socket.getOutputStream()) {
            while (running) {
                try {
                    WireMessage req = WireCodec.decode(in);
                    WireMessage resp = chunkserver.handleRpc(req);
                    if (resp != null) {
                        WireCodec.encode(resp, out);
                    }
                } catch (EOFException e) {
                    break;
                }
            }
        } catch (IOException e) {
            // Connection closed or failed
        }
    }

    public int getPort() {
        return serverSocket.getLocalPort();
    }

    @Override
    public void close() throws IOException {
        running = false;
        serverSocket.close();
        threadPool.shutdownNow();
    }
}
