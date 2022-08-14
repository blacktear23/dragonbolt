# dragonbolt

dragonbolt 是一个实验性的项目，用来把 dragonboat 和 boltdb 整合成一个基于 Raft 一致性协议的多副本的 Key-Value 数据库。

dragonbolt 使用 boltdb 作为基础的 Key-Value 存储，用 dragonboat 实现 Raft 一致性副本，实现了 Redis 协议服务，并使用 MVCC 提供了 Repeatable Read 和 Read Committed 两种事务隔离级别。

# 特点

* dragonboat: 实现 Raft 一致性协议
* boltdb: 提供底层的 Key-Value 存储，并提供基本事务
* Redis 协议: 提供 Redis 协议兼容的服务，可以直接用 redis-cli 进行操作
* MVCC: 提供 Repeatable Read 和 Read Committed 两种事务隔离级别

# Redis 命令

### 基本命令

基本命令针对非事务性 Key-Value 操作。

| 命令  | 参数  | 说明  |
| ---- | ---- | ---- |
| command | 				| 只返回 OK |
| ping 	| 				| 返回 PONG |
| config 	| 				| 只返回 OK |
| get 	| [key] 			| 获取 Key 的 Value |
| set 	| [key] [value] 	| 设置 Key 的 Value |
| mget	| [key] ...			| 批量获取 Key 的 Value |
| mset	| [key1] [value1] ... | 批量设置 Key 的 Value |
| del		| [key]			| 删除 Key |
| incr	| [key]			| Key 自增 1 |
| decr	| [key]			| Key 自减 1 |
| scan	| [start] [end] limit [limit]	| 扫描从 start 开始，到 end 结尾的 Key，并列出 limit 个 Key. <br/> 其中 end 和 limit [limit] 为可选参数 |

### 数据库命令

一个数据库代表一个独立的 boltdb 库，所有的 Key-Value 操作都需要针对某个数据库进行操作。系统默认有一个 default 数据库，每个链接默认都会切换到 default 数据库，该数据库不能删除。

| 命令  | 参数  | 说明  |
| ---- | ---- | ---- |
| db.create	| [name] | 创建数据库 |
| db.use 		| [name]	| 切换数据库 |
| db.list		| 		| 列出所有数据库 |
| db.current  |		| 列出当前使用的数据库 |
| db.curr 	|		| 列出当前使用的数据库 |
| db.delete	| [name]	| 删除数据库 |
| db.del		| [name]	| 删除数据库 |

### 事务命令

事务命令操作的 Key-Value 会满足事务的隔离级别。

| 命令  | 参数  | 说明  |
| ---- | ---- | ---- |
| txn.begin, begin		| [isolation]			| 开始一个事务，隔离级别为 isolation. <br/>其中 isolation的值可以为 `rc` 或者 `rr`, 默认为 `rr` |
| txn.set, tset 		| [key] [value] 		| 设置 Key 的 Value |
| txn.mset, tmset		| [key1] [value1] ...	| 批量设置 Key 的 Value |
| txn.get, tget		| [key]				| 获取 Key 的 Value |
| txn.mget, tmget		| [key1] [key2] ...		| 批量获取 Key 的 Value |
| txn.scan, tscan		| [start] [end] limit [limit]	| 扫描从 start 开始，到 end 结尾的 Key，并列出 limit 个 Key. <br/> 其中 end 和 limit [limit] 为可选参数 |
| txn.commit, commit |					| 提交事务 |
| txn.rollback, rollback |					| 回滚事务 |
