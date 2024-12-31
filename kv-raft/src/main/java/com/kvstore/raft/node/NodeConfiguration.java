package com.kvstore.raft.node;

import com.kvstore.raft.common.NodeAddress;
import com.kvstore.raft.rpc.RaftRpcClient;
import com.kvstore.raft.rpc.RaftRpcServer;
import lombok.Getter;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Getter
public class NodeConfiguration {
    private final String nodeId;
    private final Map<String, NodeAddress> peerAddresses;
    private final RaftRpcClient rpcClient;
    private final RaftRpcServer rpcServer;
    private final int electionTimeoutMin = 150;
    private final int electionTimeoutMax = 300;
    private final int heartbeatInterval = 50;

    public NodeConfiguration(String nodeId, Map<String, NodeAddress> peerAddresses, int localPort, RaftNode node) {
        this.nodeId = nodeId;
        this.peerAddresses = new ConcurrentHashMap<>(peerAddresses);
        this.rpcClient = new RaftRpcClient(this.peerAddresses);
        this.rpcServer = new RaftRpcServer(localPort, node);

        new Thread(this.rpcServer).start();
    }

    public void updatePeerAddresses(String peerId, NodeAddress address) {
        peerAddresses.put(peerId, address);
        rpcClient.updatePeerAddress(peerId, address);
    }

    public void stop() {
        rpcServer.stop();
        rpcClient.stop();
    }
}
