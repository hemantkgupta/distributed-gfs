package gfs.master;

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

public class MasterRpcServer implements AutoCloseable {
    private final ServerSocket serverSocket;
    private final ExecutorService threadPool;
    private final Master master;
    private volatile boolean running = true;

    public MasterRpcServer(int port, Master master) throws IOException {
        this.serverSocket = new ServerSocket(port);
        this.threadPool = Executors.newCachedThreadPool();
        this.master = master;
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
                // Log or handle error
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
                    WireMessage resp = master.handleRpc(req);
                    WireCodec.encode(resp, out);
                } catch (EOFException e) {
                    // Normal disconnect
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
