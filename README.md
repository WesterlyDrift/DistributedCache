# Distributed K-V storage service

## Project Structure & Module Definition

## Todo List
- [ ] LSM-Tree : 现在读写数据使用单线程，落表和落磁盘都使用异步线程周期性执行。后续接入线程池进行处理。
- [ ] RPC : 接入RPC框架（Zookeeper + Netty）
- [ ] Client/Server : 实现Client/Server SDK
- [ ] Raft : 实现Raft分布式一致性协议
- [ ] SQL Engine : 包装一层SQL引擎，底层仍为分层内存-磁盘KV存储
