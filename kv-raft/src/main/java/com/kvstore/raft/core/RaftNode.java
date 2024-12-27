package com.kvstore.raft.core;

import com.kvstore.raft.core.model.RaftRole;

import java.util.concurrent.CompletableFuture;

public interface RaftNode {

    void start();

    void stop();

    CompletableFuture<Message> propose(Message message);

    void addNode(NodeEndpoint endpoint);

    void removeNode(NodeEndpoint endpoint);

    RaftRole getRole();

    long getCurrentTerm();

    NodeStatus getStatus();

}
