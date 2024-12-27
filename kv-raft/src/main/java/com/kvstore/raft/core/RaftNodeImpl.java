package com.kvstore.raft.core;

import com.kvstore.raft.core.model.RaftRole;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class RaftNodeImpl implements RaftNode {
    private final NodeConfig config;
    private final StateMachine stateMachine;
    private final RaftLog raftLog;
    private final RaftRpcService rpcService;
    private final ElectionTimer electionTimer;

    private volatile RaftRole role = RaftRole.FOLLOWER;
    private volatile long currentTerm = 0;
    private volatile String votedFor = null;
    private volatile String leaderId = null;

    private final Map<String, NodeEndpoint> otherNodes = new ConcurrentHashMap<>();

    public RaftNodeImpl(NodeConfig config,
                        StateMachine stateMachine,
                        RaftLog raftLog,
                        RaftRpcService rpcService) {
        this.config = config;
        this.stateMachine = stateMachine;
        this.raftLog = raftLog;
        this.rpcService = rpcService;
        this.electionTimer = new ElectionTimer(this::startElection);
    }

    @Override
    public void start() {
        rpcService.start();
        electionTimer.start();

        becomeFollower(currentTerm);
    }

}
