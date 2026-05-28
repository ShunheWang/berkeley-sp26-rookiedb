# RookieDB 项目分析文档

## 项目概述

RookieDB 是一个由加州大学伯克利分校 CS186 课程开发的教学用小型数据库系统。该项目实现了一个完整的单节点关系型数据库管理系统(RDBMS)，旨在帮助学生理解数据库系统的核心原理和实现技术。

### 技术栈
- **语言**: Java 8
- **构建工具**: Maven
- **测试框架**: JUnit 4.13.1
- **源代码规模**: 195 个 Java 文件

---

## 核心架构

### 1. 存储层 (Storage Layer)

#### 1.1 磁盘空间管理 (Disk Space Manager)
- **文件**: `DiskSpaceManagerImpl.java`
- **功能**:
  - 管理磁盘空间分配
  - 将页面(partitions)映射到操作系统文件
  - 为每个页面分配虚拟页面号
  - 处理页面的加载和写入

#### 1.2 缓冲管理器 (Buffer Manager)
- **文件**: `BufferManagerImpl.java`, `BufferFrame.java`, `Page.java`
- **功能**:
  - 实现写回式缓冲缓存 (Write-back Buffer Cache)
  - 支持多种页面替换策略:
    - LRU (Least Recently Used)
    - Clock 算法
  - 提供页面固定/解固定 (pin/unpin) 机制
  - 页面大小: 默认 4KB

#### 1.3 页面目录 (Page Directory)
- **文件**: `PageDirectory.java`
- **功能**:
  - 实现堆文件 (Heap File)
  - 管理数据页面的分配和释放
  - 跟踪空页和数据页

### 2. 表管理 (Table Management)

#### 2.1 表 (Table)
- **文件**: `Table.java`
- **功能**:
  - 表的创建、读取、更新和删除 (CRUD)
  - 记录级操作
  - 支持全页记录模式 (Full Page Records)
- **存储格式**:
  ```
  每页布局:
  [位图 (n 字节)] [记录 0] [记录 1] ... [记录 m-1]
  ```
  - 位图指示哪些记录有效
  - 记录按顺序存储
  - 支持大小记录自适应

#### 2.2 模式 (Schema)
- **文件**: `Schema.java`
- **功能**:
  - 定义表的结构
  - 字段名称和类型管理
  - 支持类型隐式转换
  - 模式验证和序列化

#### 2.3 记录 (Record)
- **文件**: `Record.java`, `RecordId.java`
- **功能**:
  - 表示单行数据
  - 记录标识符
  - 支持序列化/反序列化

### 3. 数据类型 (Data Types)

#### 3.1 类型系统
- **文件**: `databox/` 目录
- **支持类型**:
  - `IntDataBox` - 32位整数
  - `LongDataBox` - 64位长整型
  - `FloatDataBox` - 单精度浮点数
  - `BoolDataBox` - 布尔值
  - `StringDataBox` - 变长字符串
  - `ByteArrayDataBox` - 字节数组

#### 3.2 类型系统特点
- 类型独立于 Java 类型系统
- 支持 SQL 风格的类型操作
- 提供类型大小计算
- 支持类型序列化

### 4. 索引管理 (Index Management)

#### 4.1 B+ 树索引
- **文件**: `index/BPlusTree.java`
- **功能**:
  - B+ 树结构实现
  - 支持范围查询
  - 叶节点到内部节点的链接
  - 键值对存储
- **节点类型**:
  - `InnerNode` - 内部节点
  - `LeafNode` - 叶子节点

### 5. 查询处理 (Query Processing)

#### 5.1 查询操作符
- **文件**: `query/` 目录
- **操作符类型**:
  - `SequentialScanOperator` - 顺序扫描
  - `IndexScanOperator` - 索引扫描
  - `SelectOperator` - 选择操作
  - `ProjectOperator` - 投影操作
  - `LimitOperator` - 限制结果
  - `SortOperator` - 排序操作
  - `GroupByOperator` - 分组聚合

#### 5.2 连接算法
- **文件**: `query/join/` 目录
- **支持的连接算法**:
  - `BNLJOperator` - 块嵌套循环连接
  - `PNLJOperator` - 页嵌套循环连接
  - `SNLJOperator` - 简单嵌套循环连接
  - `SMJOperator` - 排序归并连接
  - `GHJOperator` - Grace Hash 连接

#### 5.3 查询优化
- **文件**: `QueryPlan.java`, `query/disk/`
- **功能**:
  - 查询计划生成
  - 成本估算
  - 连接顺序优化

#### 5.4 表达式处理
- **文件**: `query/expr/`
- **功能**:
  - 表达式解析和求值
  - 支持算术、逻辑、比较运算符
  - CNF (Conjunctive Normal Form) 转换

### 6. 并发控制 (Concurrency Control)

#### 6.1 锁管理
- **文件**: `concurrency/LockManager.java`
- **功能**:
  - 实现多粒度锁 (Multigranularity Locking)
  - 锁类型:
    - 共享锁 (S - Shared)
    - 意向共享锁 (IS - Intention Shared)
    - 排他锁 (X - Exclusive)
    - 意向排他锁 (IX - Intention Exclusive)
    - 共享意向排他锁 (SIX - Shared Intention Exclusive)
  - 锁兼容性矩阵
  - 死锁检测和预防

#### 6.2 锁上下文
- **文件**: `concurrency/LockContext.java`
- **功能**:
  - 锁层级管理
  - 资源树结构
  - 锁升级和降级

### 7. 事务管理 (Transaction Management)

#### 7.1 事务接口
- **文件**: `Transaction.java`, `TransactionContext.java`
- **功能**:
  - 事务开始、提交、回滚
  - 保存点支持
  - 事务状态管理

#### 7.2 事务隔离级别
- 当前实现: 默认使用锁机制实现隔离
- 支持的状态: RUNNING, COMMITTING, ABORTING, COMPLETE

### 8. 恢复管理 (Recovery Management)

#### 8.1 ARIES 恢复算法
- **文件**: `recovery/ARIESRecoveryManager.java`
- **功能**:
  - WAL (Write-Ahead Logging)
  - 三个恢复阶段:
    1. **分析阶段 (Analysis)**: 重建事务表和脏页表
    2. **重做阶段 (Redo)**: 重放所有已提交事务
    3. **撤销阶段 (Undo)**: 回滚未完成事务
- **日志记录类型**:
  - Master Record
  - Begin/End Checkpoint
  - Update/Undo Update
  - Commit/Abort/End Transaction
  - Alloc/Free Partition
  - Alloc/Free Page

#### 8.2 日志管理
- **文件**: `recovery/LogManager.java`
- **功能**:
  - 日志追加和扫描
  - LSN (Log Sequence Number) 管理
  - 页面级 WAL
  - 日志持久化

### 9. 命令行接口 (CLI)

#### 9.1 SQL 解析器
- **文件**: `cli/parser/`, `cli/visitor/`
- **功能**:
  - SQL 语法解析 (使用 JavaCC)
  - 抽象语法树 (AST) 生成
  - SQL 语句访问者模式

#### 9.2 支持的 SQL 语句
- `CREATE TABLE`
- `DROP TABLE`
- `CREATE INDEX`
- `DROP INDEX`
- `SELECT`
- `INSERT`
- `UPDATE`
- `DELETE`
- `BEGIN`, `COMMIT`, `ROLLBACK`

### 10. 统计信息 (Statistics)

#### 10.1 表统计
- **文件**: `table/stats/TableStats.java`
- **功能**:
  - 记录计数
  - 列值分布统计
  - 用于查询优化

---

## 与 MySQL/PostgreSQL 的功能对比

### 当前实现

| 功能 | RookieDB | MySQL | PostgreSQL |
|------|----------|-------|------------|
| 基本数据类型 | ✓ (有限) | ✓ | ✓ |
| 主键约束 | ✗ | ✓ | ✓ |
| 外键约束 | ✗ | ✓ | ✓ |
| 唯一约束 | ✗ | ✓ | ✓ |
| 非空约束 | ✗ | ✓ | ✓ |
| 检查约束 | ✗ | ✗ | ✓ |
| B+ 树索引 | ✓ | ✓ | ✓ |
| 哈希索引 | ✗ | ✓ | ✓ |
| 全文索引 | ✗ | ✓ | ✓ |
| 复合索引 | ✓ | ✓ | ✓ |
| 多粒度锁 | ✓ | ✓ | ✓ |
| MVCC | ✗ | ✓ | ✓ |
| ARIES 恢复 | ✓ | ✓ | ✓ |
| 嵌套循环连接 | ✓ | ✓ | ✓ |
| 排序归并连接 | ✓ | ✓ | ✓ |
| Hash 连接 | ✓ | ✓ | ✓ |
| 查询优化器 | 基础 | 成熟 | 成熟 |
| 存储过程 | ✗ | ✓ | ✓ |
| 触发器 | ✗ | ✓ | ✓ |
| 视图 | ✗ | ✓ | ✓ |
| 事务隔离级别 | 基础 | 4 级 | 4 级 |
| 两阶段提交 | ✗ | ✓ | ✓ |
| 复制 | ✗ | ✓ | ✓ |
| 分区 | ✗ | ✓ | ✓ |

---

## 建议：如何使其更贴近 MySQL/PostgreSQL

### 1. 约束系统增强

#### 1.1 主键约束 (PRIMARY KEY)
**实现优先级**: 高

```java
public class Schema {
    private String primaryKey;

    public Schema setPrimaryKey(String fieldName) {
        this.primaryKey = fieldName;
        return this;
    }

    public String getPrimaryKey() {
        return primaryKey;
    }
}

// 在 Table 中验证
public RecordId addRecord(Record record) {
    // 验证主键唯一性
    if (schema.getPrimaryKey() != null) {
        int pkIndex = schema.findField(schema.getPrimaryKey());
        DataBox pkValue = record.getValues().get(pkIndex);
        if (existsPrimaryKey(pkValue)) {
            throw new DatabaseException("Duplicate primary key");
        }
    }
    // ...
}
```

#### 1.2 外键约束 (FOREIGN KEY)
**实现优先级**: 高

需要实现:
- 外键定义存储
- 引用完整性检查
- 级联操作 (CASCADE, SET NULL, SET DEFAULT, RESTRICT)

```java
public class ForeignKey {
    private String fromTable;
    private String fromColumn;
    private String toTable;
    private String toColumn;
    private ReferentialAction onDelete;
    private ReferentialAction onUpdate;
}

public enum ReferentialAction {
    CASCADE, SET_NULL, SET_DEFAULT, RESTRICT, NO_ACTION
}
```

#### 1.3 唯一约束 (UNIQUE)
**实现优先级**: 中

可以在索引层面实现，类似于主键但允许多个唯一约束。

#### 1.4 非空约束 (NOT NULL)
**实现优先级**: 中

```java
public class Schema {
    private Set<String> notNullColumns = new HashSet<>();

    public Schema addNotNull(String fieldName) {
        notNullColumns.add(fieldName);
        return this;
    }

    public boolean isNotNull(String fieldName) {
        return notNullColumns.contains(fieldName);
    }
}
```

### 2. 高级索引功能

#### 2.1 哈希索引 (Hash Index)
**实现优先级**: 中

```java
public class HashIndex implements Index {
    private int numBuckets;
    private int bucketSize;

    // 使用可扩展哈希或线性哈希
    public HashIndex(String tableName, String columnName,
                     int numBuckets, int bucketSize) {
        this.numBuckets = numBuckets;
        this.bucketSize = bucketSize;
        // 初始化桶
    }

    @Override
    public void addEntry(DataBox key, RecordId rid) {
        int hash = key.hashCode() % numBuckets;
        // 添加到对应桶
    }

    @Override
    public Iterator<RecordId> scan(DataBox key) {
        int hash = key.hashCode() % numBuckets;
        // 从桶中扫描
    }
}
```

#### 2.2 复合索引优化
**实现优先级**: 中

当前已支持复合索引，但可以优化:
- 前缀搜索 (Prefix Search)
- 索引覆盖 (Index-only Scan)

### 3. 多版本并发控制 (MVCC)

**实现优先级**: 高

这是 PostgreSQL 的核心特性，MySQL InnoDB 也支持。

```java
public class MVCCManager {
    private long currentTransactionId = 0;

    public long beginTransaction() {
        return ++currentTransactionId;
    }

    public boolean isVisible(long tupleXmin, long tupleXmax,
                            long currentXid, Snapshot snapshot) {
        // 检查元组对当前事务是否可见
        return tupleXmin < snapshot.xmax &&
               (tupleXmax == 0 || tupleXmax > snapshot.xmin);
    }

    public static class Snapshot {
        public long xmin; // 所有活跃事务中最小的 XID
        public long xmax; // 所有活跃事务中最大的 XID + 1
        public Set<Long> xidList; // 当前活跃事务列表
    }
}

// 在 Record 中添加版本信息
public class Record {
    private long xmin;  // 创建此记录的事务 ID
    private long xmax;  // 删除此记录的事务 ID (0 表示未删除)
    private long cid;   // 命令标识符
}
```

### 4. 事务隔离级别

**实现优先级**: 高

实现标准的 SQL 隔离级别:

```java
public enum IsolationLevel {
    READ_UNCOMMITTED,  // 读未提交
    READ_COMMITTED,    // 读已提交
    REPEATABLE_READ,   // 可重复读
    SERIALIZABLE       // 可串行化
}

public class Transaction {
    private IsolationLevel isolationLevel;
    private Snapshot snapshot;

    public void setIsolationLevel(IsolationLevel level) {
        this.isolationLevel = level;
        // 根据 MVCC 快照调整读取行为
    }
}
```

### 5. 查询优化器增强

**实现优先级**: 中

#### 5.1 基于成本的优化 (CBO)
```java
public class CostBasedOptimizer {
    public QueryPlan optimize(QueryPlan plan) {
        double minCost = Double.MAX_VALUE;
        QueryPlan bestPlan = null;

        for (QueryPlan candidate : generatePlans(plan)) {
            double cost = estimateCost(candidate);
            if (cost < minCost) {
                minCost = cost;
                bestPlan = candidate;
            }
        }
        return bestPlan;
    }

    private double estimateCost(QueryPlan plan) {
        // I/O 成本
        double ioCost = estimateIO(plan);
        // CPU 成本
        double cpuCost = estimateCPU(plan);
        // 内存成本
        double memCost = estimateMemory(plan);

        return ioCost + cpuCost * 0.01 + memCost * 0.001;
    }
}
```

#### 5.2 统计信息收集
```java
public class StatisticsCollector {
    public void collectTableStats(String tableName) {
        Table table = getTable(tableName);

        // 收集行数
        long numRows = table.getNumRecords();

        // 收集列统计
        for (int i = 0; i < table.getSchema().size(); i++) {
            ColumnStats stats = collectColumnStats(table, i);
            // 存储: NULL 比例、基数、直方图等
        }
    }
}
```

### 6. 存储过程 (Stored Procedures)

**实现优先级**: 低

```java
public class StoredProcedure {
    private String name;
    private List<String> parameters;
    private List<ExecutableStatement> body;

    public ResultSet execute(List<DataBox> args) {
        // 执行存储过程体
        TransactionContext context = getCurrentContext();
        for (ExecutableStatement stmt : body) {
            stmt.execute(context, args);
        }
        return context.getResult();
    }
}
```

### 7. 触发器 (Triggers)

**实现优先级**: 低

```java
public class Trigger {
    private String name;
    private String tableName;
    private TriggerEvent event;  // INSERT, UPDATE, DELETE
    private TriggerTiming timing; // BEFORE, AFTER, INSTEAD OF
    private String condition;
    private List<ExecutableStatement> actions;
}

public enum TriggerEvent {
    INSERT, UPDATE, DELETE
}

public enum TriggerTiming {
    BEFORE, AFTER, INSTEAD_OF
}
```

### 8. 视图 (Views)

**实现优先级**: 中

```java
public class View implements Relation {
    private String name;
    private QueryPlan definition;

    public BacktrackingIterator<Record> scan(TransactionContext transaction) {
        return definition.execute(transaction);
    }

    public Schema getSchema() {
        return definition.getSchema();
    }
}
```

### 9. JSON 数据类型

**实现优先级**: 中

```java
public class JsonDataBox extends DataBox {
    private String jsonValue;

    public JsonDataBox(String json) {
        this.jsonValue = json;
    }

    // JSON 操作
    public DataBox jsonGet(String path) {
        // 使用 JSON 解析库获取路径值
    }

    public JsonDataBox jsonSet(String path, DataBox value) {
        // 设置 JSON 路径值
    }
}
```

### 10. 全文搜索 (Full-Text Search)

**实现优先级**: 低

```java
public class FullTextIndex {
    private InvertedIndex invertedIndex;
    private Stemmer stemmer;
    private Tokenizer tokenizer;

    public void addDocument(RecordId rid, String text) {
        List<String> tokens = tokenizer.tokenize(text);
        for (String token : tokens) {
            String stemmed = stemmer.stem(token);
            invertedIndex.add(stemmed, rid);
        }
    }

    public Iterator<RecordId> search(String query) {
        // 支持布尔查询、短语搜索等
    }
}
```

### 11. 分区表 (Partitioning)

**实现优先级**: 中

```java
public class PartitionedTable {
    private List<Table> partitions;
    private PartitionStrategy strategy;

    public enum PartitionStrategy {
        RANGE, LIST, HASH
    }

    public RecordId addRecord(Record record) {
        int partitionIndex = strategy.getPartition(record);
        return partitions.get(partitionIndex).addRecord(record);
    }
}
```

### 12. 两阶段提交 (2PC)

**实现优先级**: 低（分布式场景需要）

```java
public class TwoPhaseCommitCoordinator {
    public boolean commit(List<Participant> participants) {
        // 阶段 1: 准备
        for (Participant p : participants) {
            if (!p.prepare()) {
                // 阶段 1 失败，全局回滚
                for (Participant rollbackP : participants) {
                    rollbackP.rollback();
                }
                return false;
            }
        }

        // 阶段 2: 提交
        for (Participant p : participants) {
            p.commit();
        }
        return true;
    }
}
```

---

## 总结

RookieDB 是一个优秀的教学用数据库系统，涵盖了关系型数据库的核心组件:

**已实现的核心功能**:
1. ✓ 存储引擎 (页式存储、缓冲管理)
2. ✓ 表管理 (CRUD 操作)
3. ✓ B+ 树索引
4. ✓ 基础查询处理 (多种连接算法)
5. ✓ 多粒度锁并发控制
6. ✓ ARIES 恢复机制
7. ✓ 基础 SQL 解析和执行

**建议的增强方向 (按优先级)**:

| 优先级 | 功能 | 难度 | 价值 |
|--------|------|------|------|
| 高 | MVCC 并发控制 | 高 | 极高 |
| 高 | 主键/外键约束 | 中 | 极高 |
| 高 | 事务隔离级别 | 中 | 高 |
| 中 | 哈希索引 | 中 | 高 |
| 中 | 视图支持 | 中 | 高 |
| 中 | 查询优化器增强 | 高 | 高 |
| 中 | JSON 数据类型 | 低 | 中 |
| 中 | 分区表 | 高 | 中 |
| 低 | 存储过程 | 低 | 中 |
| 低 | 触发器 | 中 | 中 |
| 低 | 全文搜索 | 高 | 中 |
| 低 | 两阶段提交 | 高 | 低 |

这些增强将使 RookieDB 更接近生产级数据库如 MySQL 和 PostgreSQL 的功能集，同时保持其教学价值。