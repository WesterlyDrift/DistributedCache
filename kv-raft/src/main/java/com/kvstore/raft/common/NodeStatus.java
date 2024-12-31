// File: raftkv/raft/common/NodeStatus.java
package com.kvstore.raft.common;

/**
 * 节点状态类，用于存储和展示Raft节点的当前状态信息
 */
public class NodeStatus {
    private final String nodeId;
    private final NodeState state;
    private final long currentTerm;
    private final String votedFor;
    private final String currentLeader;
    private final int logSize;
    private final int commitIndex;
    private final int lastApplied;

    public NodeStatus(String nodeId, NodeState state, long currentTerm,
                      String votedFor, String currentLeader, int logSize,
                      int commitIndex, int lastApplied) {
        this.nodeId = nodeId;
        this.state = state;
        this.currentTerm = currentTerm;
        this.votedFor = votedFor;
        this.currentLeader = currentLeader;
        this.logSize = logSize;
        this.commitIndex = commitIndex;
        this.lastApplied = lastApplied;
    }

    // Getters
    public String getNodeId() {
        return nodeId;
    }

    public NodeState getState() {
        return state;
    }

    public long getCurrentTerm() {
        return currentTerm;
    }

    public String getVotedFor() {
        return votedFor;
    }

    public String getCurrentLeader() {
        return currentLeader;
    }

    public int getLogSize() {
        return logSize;
    }

    public int getCommitIndex() {
        return commitIndex;
    }

    public int getLastApplied() {
        return lastApplied;
    }

    @Override
    public String toString() {
        return String.format(
                "NodeStatus{\n" +
                        "  nodeId='%s'\n" +
                        "  state=%s\n" +
                        "  currentTerm=%d\n" +
                        "  votedFor='%s'\n" +
                        "  currentLeader='%s'\n" +
                        "  logSize=%d\n" +
                        "  commitIndex=%d\n" +
                        "  lastApplied=%d\n" +
                        "}",
                nodeId, state, currentTerm, votedFor,
                currentLeader != null ? currentLeader : "none",
                logSize, commitIndex, lastApplied
        );
    }
}