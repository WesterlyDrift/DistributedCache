package com.kvstore.raft.rpc;

import com.kvstore.raft.message.AppendEntriesRequest;
import com.kvstore.raft.message.RaftMessage;
import com.kvstore.raft.message.VoteRequest;
import com.kvstore.raft.node.RaftNode;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class RaftRpcServer implements Runnable {
    private final int port;
    private final RaftNode node;
    private ServerSocket serverSocket;
    private volatile boolean running;
    private final ExecutorService threadPool;

    public RaftRpcServer(int port, RaftNode node) {
        this.port = port;
        this.node = node;
        this.threadPool = Executors.newFixedThreadPool(4);
        this.running = true;
    }

    @Override
    public void run() {
        try {
            serverSocket = new ServerSocket(port);
            while (running) {
                Socket socket = serverSocket.accept();
                threadPool.submit(() -> handleRequest(socket));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void handleRequest(Socket socket) {
        try (ObjectInputStream in = new ObjectInputStream(socket.getInputStream());
             ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream())) {

            RaftMessage message = (RaftMessage) in.readObject();
            RaftMessage response = null;

            if(message instanceof VoteRequest) {
                response = node.handleVoteRequest((VoteRequest) message);
            } else if(message instanceof AppendEntriesRequest) {
                response = node.handleAppendEntriesRequest((AppendEntriesRequest) message);
            }

            if(response != null) {
                out.writeObject(response);
            }
        } catch(IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    public void stop() {
        running = false;
        try {
            if(serverSocket != null) {
                serverSocket.close();
            }
        } catch(IOException e) {
            e.printStackTrace();
        }
        threadPool.shutdown();
    }
}
