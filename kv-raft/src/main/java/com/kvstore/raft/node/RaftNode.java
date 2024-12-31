package com.kvstore.raft.node;

import com.kvstore.raft.common.NodeState;
import com.kvstore.raft.common.NodeStatus;
import com.kvstore.raft.common.RaftException;
import com.kvstore.raft.message.AppendEntriesRequest;
import com.kvstore.raft.message.AppendEntriesResponse;
import com.kvstore.raft.message.VoteRequest;
import com.kvstore.raft.message.VoteResponse;
import com.kvstore.raft.snapshot.Snapshot;
import com.kvstore.raft.snapshot.SnapshotManager;
import com.kvstore.raft.storage.LogEntry;
import com.kvstore.raft.storage.RaftState;
import com.kvstore.raft.storage.RaftStorage;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

public class RaftNode {
    //config
    private final NodeConfiguration config;
    private final KVStore kvStore;
    private final RaftStorage storage;
    private final SnapshotManager snapshotManager;

    //node status
    private volatile NodeState state;
    private final ReentrantLock stateLock;

    //raft status
    private volatile long currentTerm;
    private volatile String votedFor;
    private volatile String currentLeader;
    private final List<LogEntry> log;
    private volatile int commitIndex;
    private volatile int lastApplied;

    //leader status
    private final Map<String, Integer> nextIndex;
    private final Map<String, Integer> matchIndex;

    //scheduler
    private final ScheduledExecutorService scheduler;
    private volatile ScheduledFuture<?> electionTimer;
    private volatile ScheduledFuture<?> heartbeatTimer;

    public RaftNode(NodeConfiguration config, KVStore kvStore,
                    RaftStorage storage, SnapshotManager snapshotManager) {

        this.config = config;
        this.kvStore = kvStore;
        this.storage = storage;
        this.snapshotManager = snapshotManager;

        this.state = NodeState.FOLLOWER;
        this.stateLock = new ReentrantLock();
        this.currentTerm = 0;
        this.votedFor = null;
        this.log = Collections.synchronizedList(new ArrayList<>());
        this.commitIndex = 0;
        this.lastApplied = 0;

        this.nextIndex = new ConcurrentHashMap<>();
        this.matchIndex = new ConcurrentHashMap<>();

        this.scheduler = Executors.newScheduledThreadPool(2);

        loadPersistedState();
        resetElectionTimer();
    }

    public CompletableFuture<String> processRequest(String operation, String key, String value) {
        CompletableFuture<String> future = new CompletableFuture<>();

        if(state != NodeState.LEADER) {
            future.completeExceptionally(
                    new RaftException.NotLeaderException(currentLeader)
            );
            return future;
        }

        //create log entry
        LogEntry entry = new LogEntry(currentTerm, log.size(), operation, key, value);
        log.add(entry);
        storage.appendLog(entry);

        //replicate to other peers
        replicateLog(entry.getIndex()).thenAccept(success -> {
            if(success) {
                String result = applyLogEntry(entry);
                future.complete(result);
            } else {
                future.completeExceptionally(
                        new RaftException.ConsensusException("Failed to replicate log.")
                );
            }
        });

        return future;
    }

    private CompletableFuture<Boolean> replicateLog(int index) {
        if(config.getPeerAddresses().isEmpty()) {
            return CompletableFuture.completedFuture(true);
        }

        CompletableFuture<Boolean> future = new CompletableFuture<>();
        AtomicInteger replicationCount = new AtomicInteger(1);

        config.getPeerAddresses().keySet().forEach(peerId -> {
            replicateLogToPeer(peerId, index).thenAccept(success -> {
                if(success && (replicationCount.incrementAndGet() > config.getPeerAddresses().size() / 2 + 1)) {
                    future.complete(true);
                }
            });
        });

        return future;
    }

    private CompletableFuture<Boolean> replicateLogToPeer(String peerId, int index) {
        CompletableFuture<Boolean> future = new CompletableFuture<>();

        int prevLogIndex = nextIndex.get(peerId) - 1;
        long prevLogTerm = prevLogIndex >= 0 ? log.get(prevLogIndex).getTerm() : 0;
        List<LogEntry> entries = log.subList(nextIndex.get(peerId), index + 1);

        AppendEntriesRequest request = new AppendEntriesRequest(
                currentTerm, config.getNodeId(), prevLogIndex, prevLogTerm, entries, commitIndex
        );

        CompletableFuture.supplyAsync(() ->
                config.getRpcClient().sendMessage(peerId, request)
        ).thenAccept(response -> {
            if(response instanceof AppendEntriesResponse) {
                AppendEntriesResponse appendResponse = (AppendEntriesResponse) response;

                if(appendResponse.isSuccess()) {
                    nextIndex.put(peerId, index + 1);
                    matchIndex.put(peerId, index);
                    future.complete(true);
                } else {
                    nextIndex.put(peerId, nextIndex.get(peerId) - 1);
                    replicateLogToPeer(peerId, index)
                            .thenAccept(future::complete);
                }
            }
        });

        return future;
    }

    public VoteResponse handleVoteRequest(VoteRequest request) {
        if(request.getTerm() < currentTerm) {
            return new VoteResponse(currentTerm, false);
        }

        if(request.getTerm() > currentTerm) {
            updateTerm(request.getTerm());
        }

        boolean voteGranted = false;
        if((votedFor == null || votedFor.equals(request.getCandidateId())) &&
            isLogUpToDate(request.getLastLogIndex(), request.getLastLogTerm())) {

            votedFor = request.getCandidateId();
            voteGranted = true;
            resetElectionTimer();
        }

        return new VoteResponse(currentTerm, voteGranted);
    }

    public AppendEntriesResponse handleAppendEntriesRequest(AppendEntriesRequest request) {
        if(request.getTerm() < currentTerm) {
            return new AppendEntriesResponse(currentTerm, false);
        }

        if(request.getTerm() > currentTerm) {
            updateTerm(request.getTerm());
        }

        resetElectionTimer();
        currentLeader = request.getLeaderId();

        if(request.getPrevLogIndex() >= 0) {
            if(log.size() <= request.getPrevLogIndex() ||
               log.get(request.getPrevLogIndex()).getTerm() != request.getPrevLogTerm()) {

                return new AppendEntriesResponse(currentTerm, false);

            }
        }

        if(!request.getEntries().isEmpty()) {
            for(int i = 0; i < request.getEntries().size(); i++) {
                int index = request.getPrevLogIndex() + 1 + i;
                if(index < log.size()) {
                    if(log.get(index).getTerm() != request.getEntries().get(i).getTerm()) {
                        while(log.size() > index) {
                            log.remove(log.size() - 1);
                        }
                        log.add(request.getEntries().get(i));
                    }
                } else {
                    log.add(request.getEntries().get(i));
                }
            }
            storage.saveLog(log);
        }

        if(request.getLeaderCommit() > commitIndex) {
            commitIndex = Math.min(request.getLeaderCommit(), log.size() - 1);
            applyCommittedEntries();
        }

        return new AppendEntriesResponse(currentTerm, true);
    }

    public void applyCommittedEntries() {
        while(lastApplied < commitIndex) {
            lastApplied++;
            applyLogEntry(log.get(lastApplied));
        }
    }

    private String applyLogEntry(LogEntry entry) {
        String result;
        switch(entry.getOperation()) {
            case "PUT" :
                kvStore.put(entry.getKey(), entry.getValue());
                result = "OK";
                break;
            case "GET" :
                result = kvStore.get(entry.getKey());
                break;
            case "DELETE" :
                kvStore.delete(entry.getKey());
                result = "OK";
                break;
            default :
                throw new IllegalArgumentException("Unknown Operation : " + entry.getOperation());
        }

        lastApplied = entry.getIndex();

        // 检查是否需要创建快照
        if (snapshotManager.shouldTakeSnapshot(log.size())) {
            createSnapshot();
        }

        return result;
    }

    // 创建快照
    private void createSnapshot() {
        // 获取需要保存的日志条目
        List<LogEntry> logEntries = new ArrayList<>(log);
        int lastIndex = log.size() - 1;
        long lastTerm = log.get(lastIndex).getTerm();

        // 保存快照
        snapshotManager.saveSnapshot(logEntries, lastIndex, lastTerm);

        // 清理旧日志
        while (log.size() > 0) {
            log.remove(0);
        }
        storage.clearLog();
    }

    private void loadPersistedState() {
        // 加载快照
        snapshotManager.loadSnapshot().ifPresent(snapshot -> {
            // 恢复状态机
            snapshotManager.restoreFromSnapshot(snapshot, kvStore);
            lastApplied = snapshot.getLastIncludedIndex();
            commitIndex = lastApplied;
        });

        // 加载Raft状态
        RaftState state = storage.loadState();
        currentTerm = state.getCurrentTerm();
        votedFor = state.getVotedFor();

        // 加载剩余日志（快照之后的部分）
        List<LogEntry> remainingLog = storage.loadLog();
        log.addAll(remainingLog);

        // 应用快照后的日志
        for (LogEntry entry : remainingLog) {
            if (entry.getIndex() > lastApplied) {
                applyLogEntry(entry);
            }
        }
    }

    // 更新任期
    private void updateTerm(long newTerm) {
        if (newTerm > currentTerm) {
            currentTerm = newTerm;
            votedFor = null;
            state = NodeState.FOLLOWER;
            currentLeader = null;
            storage.saveState(new RaftState(currentTerm, votedFor));
        }
    }

    private void resetElectionTimer() {
        if (electionTimer != null) {
            electionTimer.cancel(false);
        }

        int timeout = ThreadLocalRandom.current().nextInt(
                config.getElectionTimeoutMin(),
                config.getElectionTimeoutMax()
        );

        electionTimer = scheduler.schedule(
                this::startElection,
                timeout,
                TimeUnit.MILLISECONDS
        );
    }

    // 开始选举
    private void startElection() {
        stateLock.lock();
        try {
            if (state != NodeState.LEADER) {
                state = NodeState.CANDIDATE;
                currentTerm++;
                votedFor = config.getNodeId();
                currentLeader = null;
                storage.saveState(new RaftState(currentTerm, votedFor));
                requestVotes();
            }
        } finally {
            stateLock.unlock();
        }
    }

    // 请求投票
    private void requestVotes() {

        AtomicInteger votes = new AtomicInteger(1); // 包含自己的一票

        VoteRequest request = new VoteRequest(
                currentTerm,
                config.getNodeId(),
                log.size() - 1,
                log.isEmpty() ? 0 : log.get(log.size() - 1).getTerm()
        );

        for (String peerId : config.getPeerAddresses().keySet()) {
            CompletableFuture.supplyAsync(() ->
                    config.getRpcClient().sendMessage(peerId, request)
            ).thenAccept(response -> {
                if (response instanceof VoteResponse) {
                    VoteResponse voteResponse = (VoteResponse) response;
                    if (voteResponse.getTerm() > currentTerm) {
                        updateTerm(voteResponse.getTerm());
                    } else if (state == NodeState.CANDIDATE &&
                            voteResponse.isVoteGranted() &&
                            votes.incrementAndGet() > config.getPeerAddresses().size() / 2 + 1) {
                        becomeLeader();
                    }
                }
            });
        }
    }

    // 转变为Leader
    private void becomeLeader() {
        stateLock.lock();
        try {
            state = NodeState.LEADER;
            currentLeader = config.getNodeId();

            // 初始化Leader状态
            for (String peerId : config.getPeerAddresses().keySet()) {
                nextIndex.put(peerId, log.size());
                matchIndex.put(peerId, 0);
            }

            // 开始发送心跳
            startHeartbeat();
        } finally {
            stateLock.unlock();
        }
    }

    // 启动心跳
    private void startHeartbeat() {
        if (heartbeatTimer != null) {
            heartbeatTimer.cancel(false);
        }

        heartbeatTimer = scheduler.scheduleAtFixedRate(
                () -> {
                    if (state == NodeState.LEADER) {
                        for (String peerId : config.getPeerAddresses().keySet()) {
                            sendHeartbeat(peerId);
                        }
                    }
                },
                0,
                config.getHeartbeatInterval(),
                TimeUnit.MILLISECONDS
        );
    }


    // 发送心跳
    private void sendHeartbeat(String peerId) {
        int prevLogIndex = nextIndex.get(peerId) - 1;
        long prevLogTerm = prevLogIndex >= 0 ? log.get(prevLogIndex).getTerm() : 0;

        AppendEntriesRequest request = new AppendEntriesRequest(
                currentTerm,
                config.getNodeId(),
                prevLogIndex,
                prevLogTerm,
                Collections.emptyList(),
                commitIndex
        );

        CompletableFuture.supplyAsync(() ->
                config.getRpcClient().sendMessage(peerId, request)
        ).thenAccept(response -> {
            if (response instanceof AppendEntriesResponse) {
                AppendEntriesResponse appendResponse = (AppendEntriesResponse) response;
                if (appendResponse.getTerm() > currentTerm) {
                    updateTerm(appendResponse.getTerm());
                }
            }
        });
    }

    // 检查日志是否最新
    private boolean isLogUpToDate(int lastLogIndex, long lastLogTerm) {
        if (log.isEmpty()) {
            return true;
        }

        LogEntry lastEntry = log.get(log.size() - 1);
        if (lastEntry.getTerm() != lastLogTerm) {
            return lastLogTerm > lastEntry.getTerm();
        }

        return lastLogIndex >= log.size() - 1;
    }

    // 关闭节点
    public void shutdown() {
        if (electionTimer != null) {
            electionTimer.cancel(false);
        }
        if (heartbeatTimer != null) {
            heartbeatTimer.cancel(false);
        }
        scheduler.shutdown();
        config.stop();
    }

    // 获取节点状态（用于监控）
    public NodeStatus getStatus() {
        return new NodeStatus(
                config.getNodeId(),
                state,
                currentTerm,
                votedFor,
                currentLeader,
                log.size(),
                commitIndex,
                lastApplied
        );
    }
}
