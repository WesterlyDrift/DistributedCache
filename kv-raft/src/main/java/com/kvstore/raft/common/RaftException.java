package com.kvstore.raft.common;

import lombok.Data;

public class RaftException extends Exception {

    public static class NotLeaderException extends RaftException {
        private final String leaderHint;

        public NotLeaderException(String leaderHint) {
            super("Not the leader");
            this.leaderHint = leaderHint;
        }

        public String getLeaderHint() {
            return leaderHint;
        }
    }

    public static class ConsensusException extends RaftException {
        public ConsensusException(String message) {
            super(message);
        }
    }

    protected RaftException(String message) {
        super(message);
    }
}
