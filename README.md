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
| config 	| 				| 只返回 OK |`

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
| set, txn.set, tset 		| [key] [value] 		| 设置 Key 的 Value |
| mset, txn.mset, tmset		| [key1] [value1] ...	| 批量设置 Key 的 Value |
| get, txn.get, tget		| [key]				| 获取 Key 的 Value |
| mget, txn.mget, tmget		| [key1] [key2] ...		| 批量获取 Key 的 Value |
| scan, txn.scan, tscan		| [start] [end] limit [limit]	| 扫描从 start 开始，到 end 结尾的 Key，并列出 limit 个 Key. <br/> 其中 end 和 limit [limit] 为可选参数 |
| incr, txn.incr            | [key]             | Key 自增 1 |
| decr, txn.decr            | [key]             | Key 自减 1 |
| query, txn.query          | [query]           | 执行查询语句 |
| explain, txn.explain      | [query]           | 显示查询计划 |
| txn.commit, commit |					| 提交事务 |
| txn.savepoint, savepoint | [savepoint] | 创建 savepoint，用于回滚到指定 savepoint |
| txn.rollback, rollback |	[savepoint]	| 回滚事务，或回滚到指定的 Savepoint |
| txn.lock, tlock		| [key]				| 锁定 Key |
| txn.unlock, tunlock	| [key]				| 解锁 Key |


# 关于 DB

一个 DB 为一个 boltdb 实例，不同的 DB 的数据分别存在不同的 boltdb 文件中。基本命令和事务命令所操作的数据都会存在一个 DB 中。

当创建一个新的 DB 时，系统会自动创建多副本的 boltdb 实例。不同的 DB 之间使用不同的 Raft ShardID，所以多 DB 模式等同于启用了 Multi-Raft。

# 关于 MVCC 和事务

MVCC 基于 boltdb 的 Bucket 功能实现的。一个 MVCC 空间使用了 2 个 boltdb 的 Bucket：`cf:mvcc:keys` 和 `cf:mvcc:vals`。其中，`cf:mvcc:keys` 用来保存 Key 列表，同时维护锁。 `cf:mvcc:vals` 用来保存版本数据。

MVCC 版本数据的编码为：

* Key:   `| key (n bytes) | version (8 bytes) |`
* Value: `| op (1 bytes) | data (n bytes) |`

其中 version 为 uint64 big-endian 编码的版本号，版本号做了一个特殊操作，用 `UINT64_MAX - 当前版本号` 这样可以保证最新的数据会出现在最前面。

Value 中的 op 用 1 字节来区分是数据还是删除。

当事务开始时，会获取一个版本号，用来确定读取的快照，事务内的写操作会被服务器缓存在内存中，只有在事务提交的时候才会写入到 boltdb 中。在事务提交时，会获取一个提交版本号，事务内的写操作会被打上提交的版本号，通过 batch 提交的方式提交到 boltdb 中。

RR 和 RC 隔离级别的差异只在于事务开始时，读快照的版本号是当前版本号，还是最大版本号（UINT64_MAX)。如果是最大版本号，就是 RC 隔离级别，如果是当前版本号就是 RR 隔离级别。

# 关于事务冲突

事务系统会通过锁和提交版本号做冲突验证。如果事务 A 锁定了某个 key，那么事务 B 在没有获取锁的前提下提交了对 Key 的修改会报告提交失败。任何一个事务在提交或者回滚之后都会自动解锁已锁定的 key。

另外，如果事务在提交时修改的 Key 在库中的最新提交版本号是在事务开始之后，说明两个事务同时修改了一个 key，系统会对后提交的事务报告提交失败，并回滚该事务做的修改。

# 查询 `query`

Dragonbolt 支持通过类似 SQL 的表达式过滤 Key Value 数据。语法：

```
query "[select (* | Fields)] where [WhereCondition] [order by OrderByParams] [limit LimitParams]"

Fields := (Field | FunctionCall | String | Expr) { "," (Field | FunctionCall | String | Expr) }

WhereCondition := Expr {LogicOp Expr}

String := '"' Chars '"'

LogicOp := "|" | "&"

NotOp := "!"

Field := "key" | "value"

BinaryOp := "=" | "!=" | "^=" | "~=" | ">" | ">=" | "<" | "<=" |
            "+" | "-" | "*" | "/"

FuncName := Chars

FuncArg := (Field | Number | String | Expr)

FunctionCall := FuncName "(" FuncArg { "," FuncArg } ")"

OpParam := (Field | String | FunctionCall | Number)

Expr := [NotOp] OpParam BinaryOp OpParam |
        "(" [NotOp] OpParam BinaryOp OpParam ")"

LimitParams := (Number | Number "," Number)

OrderByParams := OrderByField { "," OrderByField }

OrderByField := (Field | Expr) [("asc" | "desc")]
```

运算符：

* `|`: 逻辑或 
* `&`: 逻辑与
* `=`: 相等
* `!=`: 不等于
* `^=`: 前缀匹配
* `~=`: 正则表达式匹配
*  `>`: 大于
*  `<`: 小于
*  `>=`: 大于等于
*  `<=`: 小于等于
*  `+`: 加法
*  `-`: 减法
*  `*`: 乘法
*  `/`: 除法

字段：

* `key`: 表示 Key
* `value`: 表示 Value

函数：

* `int(val)`: 转换为数字
* `float(val)`: 转换为浮点数
* `str(val)`: 转换为字符串
* `upper(val)`: 转换成大写字母
* `lower(val)`: 转换成小写字母

例子：

```
> query "where key = 'key1'"
> query "where key = 'key1' & value = 'value1'"
> query "where (key = 'key1' | key = 'key2') & value ^= 'value_prefix'"
> query "select * where key = 'key1'"
> query "select key where key = 'key2'"
> query "select key, value, upper(value) where key ^= 'key'"
> query "select key, int(value) * 2 where key ^= 'prefix' & int(value) * 2 < 10"
> query "select * where key ^= 'k' limit 10"
> query "select * where key ^= 'k' limit 5, 10"
> query "select * where key ^= 'k' order by value desc"
> query "select * where key ^= 'k' order by key desc, value asc limit 10"
```
