package com.kvstore.raft.rpc;

import com.google.common.collect.Maps;
import com.kvstore.raft.common.NodeAddress;
import com.kvstore.raft.message.RaftMessage;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class RaftRpcClient {
    private final Map<String, NodeAddress> peerAddresses;
    private final Map<String, Socket> connections;
    private final int connectionTimeout = 3000;

    public RaftRpcClient(Map<String, NodeAddress> peerAddresses) {
        this.peerAddresses = new ConcurrentHashMap<>(peerAddresses);
        this.connections = new HashMap<>();
    }

    public RaftMessage sendMessage(String peerId, RaftMessage message) {
        NodeAddress address = peerAddresses.get(peerId);
        if(address == null) {
            throw new IllegalArgumentException("Unknown peer id: " + peerId);
        }

        try {
            Socket socket = getConnection(peerId, address);

            ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
            out.writeObject(message);

            ObjectInputStream in = new ObjectInputStream(socket.getInputStream());
            return (RaftMessage) in.readObject();
        } catch(IOException | ClassNotFoundException e) {
            e.printStackTrace();
            closeConnection(peerId);
            return null;
        }
    }

    private synchronized Socket getConnection(String peerId, NodeAddress address) throws IOException {
        Socket socket = connections.get(peerId);
        if(socket == null || socket.isClosed()) {
            socket = new Socket();
            socket.setSoTimeout(connectionTimeout);
            socket.connect(new InetSocketAddress(address.getHost(), address.getPort()), connectionTimeout);
            connections.put(peerId, socket);
        }
        return socket;
    }

    private synchronized void closeConnection(String peerId) {
        Socket socket = connections.remove(peerId);
        if(socket != null) {
            try {
                socket.close();
            } catch(IOException e) {
                e.printStackTrace();
            }
        }
    }

    public void stop() {
        for(String peerId : connections.keySet()) {
            closeConnection(peerId);
        }
    }

    public void updatePeerAddress(String peerId, NodeAddress address) {
        closeConnection(peerId);
        peerAddresses.put(peerId, address);
    }
}
