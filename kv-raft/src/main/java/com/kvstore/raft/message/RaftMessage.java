package com.kvstore.raft.message;

import java.io.Serializable;

public interface RaftMessage extends Serializable {
    long getTerm();
    void setTerm(long term);
}
