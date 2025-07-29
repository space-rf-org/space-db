#pragma once

#include <memory>
#include <utility>
#include <vector>
#include <string>
#include <unordered_map>
#include <algorithm>
#include <chrono>
#include <queue>

// Include the logical planner header
#include "planner/logical_planner.h"

namespace physical_planner {
    // Forward declarations
    class PhysicalOperator;
    class ExecutionContext;
    class BufferPool;
    class IndexManager;

    // Physical operator types
    enum class PhysicalOperatorType {
        SEQ_SCAN,
        INDEX_SCAN,
        NESTED_LOOP_JOIN,
        HASH_JOIN,
        MERGE_JOIN,
        FILTER,
        PROJECT,
        HASH_AGGREGATE,
        SORT_AGGREGATE,
        SORT,
        LIMIT,
        UNION,
        INTERSECT,
        EXCEPT,
        MATERIALIZE,
        EXCHANGE  // For parallel execution
    };

    // Join algorithms
    enum class JoinAlgorithm {
        NESTED_LOOP,
        BLOCK_NESTED_LOOP,
        HASH_JOIN,
        SORT_MERGE_JOIN,
        INDEX_NESTED_LOOP
    };

    // Access path information
    struct AccessPath {
        std::string tableName;
        std::string indexName;
        bool isIndexPath = false;
        std::vector<std::string> keyColumns;
        double selectivity = 1.0;
        size_t estimatedRows = 0;
        double estimatedCost = 0.0;

        explicit AccessPath(std::string  table) : tableName(std::move(table)) {}
        AccessPath(std::string  table, std::string  index,
                   const std::vector<std::string>& keys)
            : tableName(std::move(table)), indexName(std::move(index)), isIndexPath(true), keyColumns(keys) {}
    };

    // Physical execution statistics
    struct PhysicalStats {
        size_t pagesRead = 0;
        size_t pagesWritten = 0;
        size_t tuplesProcessed = 0;
        size_t tuplesReturned = 0;
        std::chrono::microseconds executionTime{0};
        size_t memoryUsed = 0;
        size_t diskIO = 0;

        PhysicalStats& operator+=(const PhysicalStats& other) {
            pagesRead += other.pagesRead;
            pagesWritten += other.pagesWritten;
            tuplesProcessed += other.tuplesProcessed;
            tuplesReturned += other.tuplesReturned;
            executionTime += other.executionTime;
            memoryUsed = std::max(memoryUsed, other.memoryUsed);
            diskIO += other.diskIO;
            return *this;
        }
    };

    // Base class for all physical operators
    class PhysicalOperator {
    public:
        PhysicalOperatorType type;
        std::vector<std::unique_ptr<PhysicalOperator>> children;
        std::vector<logical_planner::ColumnInfo> outputSchema;

        // Cost estimates
        double startupCost = 0.0;    // Cost to produce first tuple
        double totalCost = 0.0;      // Total cost to produce all tuples
        size_t estimatedRows = 0;
        size_t estimatedWidth = 0;   // Average tuple width in bytes

        // Execution statistics
        PhysicalStats actualStats;

        // Memory and resource requirements
        size_t memoryRequirement = 0;
        int parallelism = 1;

        explicit PhysicalOperator(const PhysicalOperatorType t) : type(t) {}
        virtual ~PhysicalOperator() = default;

        [[nodiscard]] virtual std::string toString() const = 0;
        [[nodiscard]] virtual std::unique_ptr<PhysicalOperator> clone() const = 0;

        // Cost calculation methods
        virtual void calculateCost(const ExecutionContext& context) = 0;
        [[nodiscard]] virtual double getStartupCost() const { return startupCost; }
        [[nodiscard]] virtual double getTotalCost() const { return totalCost; }

        // Execution interface (would be implemented for actual execution)
        virtual bool open(ExecutionContext& context) = 0;
        virtual bool getNext(ExecutionContext& context, std::vector<std::string>& tuple) = 0;
        virtual void close(ExecutionContext& context) = 0;

        void addChild(std::unique_ptr<PhysicalOperator> child) {
            children.push_back(std::move(child));
        }

        [[nodiscard]] PhysicalOperator* getChild(size_t index) const {
            return index < children.size() ? children[index].get() : nullptr;
        }

        [[nodiscard]] size_t getChildCount() const {
            return children.size();
        }

        // Resource estimation
        [[nodiscard]] virtual size_t estimateMemoryUsage() const {
            return memoryRequirement;
        }

        [[nodiscard]] virtual bool supportsParallelism() const {
            return false;
        }
    };

    // Sequential scan operator
    class SeqScanOperator : public PhysicalOperator {
    public:
        std::string tableName;
        std::string alias;
        std::vector<std::string> projectedColumns;
        std::string filter; // Pushed-down filter

        explicit SeqScanOperator(std::string  table, std::string  tableAlias = "");

        [[nodiscard]] std::string toString() const override;

        [[nodiscard]] std::unique_ptr<PhysicalOperator> clone() const override;

        void calculateCost(const ExecutionContext& context) override;
        bool open(ExecutionContext& context) override;
        bool getNext(ExecutionContext& context, std::vector<std::string>& tuple) override;
        void close(ExecutionContext& context) override;

        [[nodiscard]] bool supportsParallelism() const override;
    };

    // Index scan operator
    class IndexScanOperator : public PhysicalOperator {

    public:
        std::string tableName;
        std::string indexName;
        std::vector<std::string> keyColumns;
        std::vector<std::string> keyValues;
        std::string scanCondition;
        bool isUnique = false;

        IndexScanOperator(std::string  table, std::string  index);

        [[nodiscard]] std::string toString() const override;

        [[nodiscard]] std::unique_ptr<PhysicalOperator> clone() const override;

        void calculateCost(const ExecutionContext& context) override;
        bool open(ExecutionContext& context) override;
        bool getNext(ExecutionContext& context, std::vector<std::string>& tuple) override;
        void close(ExecutionContext& context) override;
    };

    // Nested loop join operator
    class NestedLoopJoinOperator final : public PhysicalOperator {
    public:
        logical_planner::JoinType joinType;
        std::string joinCondition;
        bool isBlockJoin = false; // Block nested loop vs tuple nested loop
        size_t blockSize = 8192;  // Block size in bytes

        NestedLoopJoinOperator(logical_planner::JoinType type, std::string  condition);

        [[nodiscard]] std::string toString() const override;

        [[nodiscard]] std::unique_ptr<PhysicalOperator> clone() const override;
        void calculateCost(const ExecutionContext& context) override;
        bool open(ExecutionContext& context) override;
        bool getNext(ExecutionContext& context, std::vector<std::string>& tuple) override;
        void close(ExecutionContext& context) override;

    private:
        static std::string joinTypeToString(logical_planner::JoinType type) {
            switch (type) {
                case logical_planner::JoinType::INNER: return "INNER";
                case logical_planner::JoinType::LEFT: return "LEFT";
                case logical_planner::JoinType::RIGHT: return "RIGHT";
                case logical_planner::JoinType::FULL: return "FULL";
                case logical_planner::JoinType::CROSS: return "CROSS";
                default: return "INNER";
            }
        }
    };

    // Hash join operator
    class HashJoinOperator : public PhysicalOperator {
    public:
        logical_planner::JoinType joinType;
        std::string joinCondition;
        std::vector<std::string> leftKeys;
        std::vector<std::string> rightKeys;
        size_t hashTableSize = 0;
        bool isGraceHashJoin = false; // Grace hash join for large datasets

        HashJoinOperator(logical_planner::JoinType type, std::string  condition);

        [[nodiscard]] std::string toString() const override;

        [[nodiscard]] std::unique_ptr<PhysicalOperator> clone() const override;

        void calculateCost(const ExecutionContext& context) override;
        bool open(ExecutionContext& context) override;
        bool getNext(ExecutionContext& context, std::vector<std::string>& tuple) override;
        void close(ExecutionContext& context) override;

        [[nodiscard]] size_t estimateMemoryUsage() const override {
            return hashTableSize;
        }

    private:
        static std::string joinTypeToString(logical_planner::JoinType type) {
            switch (type) {
                case logical_planner::JoinType::INNER: return "INNER";
                case logical_planner::JoinType::LEFT: return "LEFT";
                case logical_planner::JoinType::RIGHT: return "RIGHT";
                case logical_planner::JoinType::FULL: return "FULL";
                default: return "INNER";
            }
        }
    };

    // Sort-merge join operator
    class SortMergeJoinOperator : public PhysicalOperator {
    public:
        logical_planner::JoinType joinType;
        std::string joinCondition;
        std::vector<std::string> leftKeys;
        std::vector<std::string> rightKeys;
        bool leftSorted = false;
        bool rightSorted = false;

        SortMergeJoinOperator(logical_planner::JoinType type, std::string  condition)
            : PhysicalOperator(PhysicalOperatorType::MERGE_JOIN), joinType(type), joinCondition(std::move(condition)) {}

        [[nodiscard]] std::string toString() const override {
            std::string joinTypeStr = joinTypeToString(joinType);
            return "SortMergeJoin(" + joinTypeStr + ", " + joinCondition + ")";
        }

        [[nodiscard]] std::unique_ptr<PhysicalOperator> clone() const override {
            auto cloned = std::make_unique<SortMergeJoinOperator>(joinType, joinCondition);
            cloned->leftKeys = leftKeys;
            cloned->rightKeys = rightKeys;
            cloned->leftSorted = leftSorted;
            cloned->rightSorted = rightSorted;
            cloned->outputSchema = outputSchema;
            return cloned;
        }

        void calculateCost(const ExecutionContext& context) override;
        bool open(ExecutionContext& context) override;
        bool getNext(ExecutionContext& context, std::vector<std::string>& tuple) override;
        void close(ExecutionContext& context) override;

    private:
        static std::string joinTypeToString(logical_planner::JoinType type) {
            switch (type) {
                case logical_planner::JoinType::INNER: return "INNER";
                case logical_planner::JoinType::LEFT: return "LEFT";
                case logical_planner::JoinType::RIGHT: return "RIGHT";
                case logical_planner::JoinType::FULL: return "FULL";
                default: return "INNER";
            }
        }
    };

    // Filter operator
    class FilterOperator : public PhysicalOperator {
    public:
        std::string condition;
        double selectivity = 0.1;

        explicit FilterOperator(std::string  cond)
            : PhysicalOperator(PhysicalOperatorType::FILTER), condition(std::move(cond)) {}

        [[nodiscard]] std::string toString() const override {
            return "Filter(" + condition + ")";
        }

        [[nodiscard]] std::unique_ptr<PhysicalOperator> clone() const override {
            auto cloned = std::make_unique<FilterOperator>(condition);
            cloned->selectivity = selectivity;
            cloned->outputSchema = outputSchema;
            return cloned;
        }

        void calculateCost(const ExecutionContext& context) override;
        bool open(ExecutionContext& context) override;
        bool getNext(ExecutionContext& context, std::vector<std::string>& tuple) override;
        void close(ExecutionContext& context) override;
    };

    // Projection operator
    class ProjectionOperator final : public PhysicalOperator {
    public:
        std::vector<std::string> projectedColumns;
        std::vector<std::string> expressions;

        explicit ProjectionOperator(const std::vector<std::string>& columns)
            : PhysicalOperator(PhysicalOperatorType::PROJECT), projectedColumns(columns) {}

        [[nodiscard]] std::string toString() const override {
            std::string result = "Project([";
            for (size_t i = 0; i < projectedColumns.size(); ++i) {
                if (i > 0) result += ", ";
                result += projectedColumns[i];
            }
            result += "])";
            return result;
        }

        [[nodiscard]] std::unique_ptr<PhysicalOperator> clone() const override {
            auto cloned = std::make_unique<ProjectionOperator>(projectedColumns);
            cloned->expressions = expressions;
            cloned->outputSchema = outputSchema;
            return cloned;
        }

        void calculateCost(const ExecutionContext& context) override;
        bool open(ExecutionContext& context) override;
        bool getNext(ExecutionContext& context, std::vector<std::string>& tuple) override;
        void close(ExecutionContext& context) override;
    };

    // Hash aggregate operator
    class HashAggregateOperator final : public PhysicalOperator {
    public:
        std::vector<std::string> groupByColumns;
        std::vector<std::pair<logical_planner::AggregateType, std::string>> aggregates;
        std::string havingCondition;
        size_t hashTableSize = 0;

        explicit HashAggregateOperator(const std::vector<std::string>& groupBy)
            : PhysicalOperator(PhysicalOperatorType::HASH_AGGREGATE), groupByColumns(groupBy) {}

        [[nodiscard]] std::string toString() const override {
            std::string result = "HashAggregate(";
            if (!groupByColumns.empty()) {
                result += "GROUP BY [";
                for (size_t i = 0; i < groupByColumns.size(); ++i) {
                    if (i > 0) result += ", ";
                    result += groupByColumns[i];
                }
                result += "]";
            }
            result += ")";
            return result;
        }

        [[nodiscard]] std::unique_ptr<PhysicalOperator> clone() const override {
            auto cloned = std::make_unique<HashAggregateOperator>(groupByColumns);
            cloned->aggregates = aggregates;
            cloned->havingCondition = havingCondition;
            cloned->hashTableSize = hashTableSize;
            cloned->outputSchema = outputSchema;
            return cloned;
        }

        void calculateCost(const ExecutionContext& context) override;
        bool open(ExecutionContext& context) override;
        bool getNext(ExecutionContext& context, std::vector<std::string>& tuple) override;
        void close(ExecutionContext& context) override;

        [[nodiscard]] size_t estimateMemoryUsage() const override {
            return hashTableSize;
        }
    };

    // Sort operator
    class SortOperator final : public PhysicalOperator {
    public:
        std::vector<std::pair<std::string, bool>> sortColumns; // column, ascending
        bool isExternal = false; // External sort for large datasets
        size_t memoryLimit = 0;

        explicit SortOperator(const std::vector<std::pair<std::string, bool>>& columns)
            : PhysicalOperator(PhysicalOperatorType::SORT), sortColumns(columns) {}

        [[nodiscard]] std::string toString() const override {
            std::string result = isExternal ? "ExternalSort([" : "Sort([";
            for (size_t i = 0; i < sortColumns.size(); ++i) {
                if (i > 0) result += ", ";
                result += sortColumns[i].first + (sortColumns[i].second ? " ASC" : " DESC");
            }
            result += "])";
            return result;
        }

        [[nodiscard]] std::unique_ptr<PhysicalOperator> clone() const override {
            auto cloned = std::make_unique<SortOperator>(sortColumns);
            cloned->isExternal = isExternal;
            cloned->memoryLimit = memoryLimit;
            cloned->outputSchema = outputSchema;
            return cloned;
        }

        void calculateCost(const ExecutionContext& context) override;
        bool open(ExecutionContext& context) override;
        bool getNext(ExecutionContext& context, std::vector<std::string>& tuple) override;
        void close(ExecutionContext& context) override;

        [[nodiscard]] size_t estimateMemoryUsage() const override {
            return memoryLimit;
        }
    };

    // Limit operator
    class LimitOperator final : public PhysicalOperator {
    public:
        size_t limit;
        size_t offset;

        explicit LimitOperator(const size_t lim, const size_t off = 0)
            : PhysicalOperator(PhysicalOperatorType::LIMIT), limit(lim), offset(off) {}

        [[nodiscard]] std::string toString() const override {
            std::string result = "Limit(" + std::to_string(limit);
            if (offset > 0) {
                result += ", OFFSET " + std::to_string(offset);
            }
            result += ")";
            return result;
        }

        [[nodiscard]] std::unique_ptr<PhysicalOperator> clone() const override {
            auto cloned = std::make_unique<LimitOperator>(limit, offset);
            cloned->outputSchema = outputSchema;
            return cloned;
        }

        void calculateCost(const ExecutionContext& context) override;
        bool open(ExecutionContext& context) override;
        bool getNext(ExecutionContext& context, std::vector<std::string>& tuple) override;
        void close(ExecutionContext& context) override;
    };

    // Execution context containing system resources and statistics
    class ExecutionContext {
    public:
        // System configuration
        size_t availableMemory = 128 * 1024 * 1024; // 128MB default
        size_t bufferPoolSize = 64 * 1024 * 1024;   // 64MB default
        size_t pageSize = 8192;                      // 8KB pages
        size_t maxParallelism = 4;                   // Max parallel workers

        // Cost model parameters
        double seqPageCost = 1.0;      // Cost of sequential page read
        double randomPageCost = 4.0;   // Cost of random page read
        double cpuTupleCost = 0.01;    // Cost per tuple processed
        double cpuIndexTupleCost = 0.005; // Cost per index tuple
        double cpuOperatorCost = 0.0025;  // Cost per operator call

        // Table and index statistics
        std::unordered_map<std::string, logical_planner::TableStats> tableStats;
        std::unordered_map<std::string, std::vector<std::string>> tableIndexes;
        std::unordered_map<std::string, std::vector<logical_planner::ColumnInfo>> tableSchemas;

        // Runtime statistics
        std::unordered_map<std::string, PhysicalStats> operatorStats;

        ExecutionContext() = default;

        void addTableStats(const std::string& tableName, const logical_planner::TableStats& stats) {
            tableStats[tableName] = stats;
        }

        void addTableSchema(const std::string& tableName, const std::vector<logical_planner::ColumnInfo>& schema) {
            tableSchemas[tableName] = schema;
        }

        void addIndex(const std::string& tableName, const std::string& indexName) {
            tableIndexes[tableName].push_back(indexName);
        }

        [[nodiscard]] const logical_planner::TableStats* getTableStats(const std::string& tableName) const {
            auto it = tableStats.find(tableName);
            return it != tableStats.end() ? &it->second : nullptr;
        }

        [[nodiscard]] const std::vector<logical_planner::ColumnInfo>* getTableSchema(const std::string& tableName) const {
            auto it = tableSchemas.find(tableName);
            return it != tableSchemas.end() ? &it->second : nullptr;
        }

        [[nodiscard]] std::vector<std::string> getTableIndexes(const std::string& tableName) const {
            const auto it = tableIndexes.find(tableName);
            return it != tableIndexes.end() ? it->second : std::vector<std::string>();
        }
    };

    // Physical planner class
    class PhysicalPlanner {
    private:
        ExecutionContext context;

    public:
        explicit PhysicalPlanner(ExecutionContext  ctx = ExecutionContext{}) : context(std::move(ctx)) {}

        // Main method to convert logical plan to physical plan // NOLINTNEXTLINE(misc-no-recursion)
        std::unique_ptr<PhysicalOperator> createPhysicalPlan(const logical_planner::LogicalOperator* logicalPlan) {
            if (!logicalPlan) {
                return nullptr;
            }

            std::unique_ptr<PhysicalOperator> physicalPlan;

            switch (logicalPlan->type) {
                case logical_planner::LogicalOperatorType::TABLE_SCAN:
                    physicalPlan = planTableScan(dynamic_cast<const logical_planner::TableScanOperator*>(logicalPlan));
                    break;

                case logical_planner::LogicalOperatorType::FILTER:
                    physicalPlan = planFilter(dynamic_cast<const logical_planner::FilterOperator*>(logicalPlan));
                    break;

                case logical_planner::LogicalOperatorType::PROJECT:
                    physicalPlan = planProjection(dynamic_cast<const logical_planner::ProjectionOperator*>(logicalPlan));
                    break;

                case logical_planner::LogicalOperatorType::NESTED_LOOP_JOIN:
                case logical_planner::LogicalOperatorType::HASH_JOIN:
                case logical_planner::LogicalOperatorType::MERGE_JOIN:
                    physicalPlan = planJoin(dynamic_cast<const logical_planner::JoinOperator*>(logicalPlan));
                    break;

                case logical_planner::LogicalOperatorType::AGGREGATE:
                    physicalPlan = planAggregate(dynamic_cast<const logical_planner::AggregateOperator*>(logicalPlan));
                    break;

                case logical_planner::LogicalOperatorType::SORT:
                    physicalPlan = planSort(dynamic_cast<const logical_planner::SortOperator*>(logicalPlan));
                    break;

                case logical_planner::LogicalOperatorType::LIMIT:
                    physicalPlan = planLimit(dynamic_cast<const logical_planner::LimitOperator*>(logicalPlan));
                    break;

                default:
                    // For unsupported operators, create a basic physical equivalent
                    physicalPlan = createDefaultPhysicalOperator(logicalPlan);
                    break;
            }

            // Recursively convert children
            if (physicalPlan) {
                for (const auto& logicalChild : logicalPlan->children) {
                    if (auto physicalChild = createPhysicalPlan(logicalChild.get())) {
                        physicalPlan->addChild(std::move(physicalChild));
                    }
                }

                // Calculate costs after children are processed
                physicalPlan->calculateCost(context);
            }

            return physicalPlan;
        }

        // Optimize physical plan
        std::unique_ptr<PhysicalOperator> optimizePhysicalPlan(std::unique_ptr<PhysicalOperator> plan) {
            if (!plan) return nullptr;

            // Apply physical optimizations
            plan = chooseJoinAlgorithms(std::move(plan));
            plan = optimizeMemoryUsage(std::move(plan));
            plan = addMaterializationNodes(std::move(plan));
            plan = optimizeForParallelism(std::move(plan));

            return plan;
        }

        // Print physical plan //NOLINTNEXTLINE(misc-no-recursion)
        static std::string printPhysicalPlan(const PhysicalOperator* op, int indent = 0) {
            if (!op) return "";

            std::string result;
            std::string indentStr(indent * 2, ' ');

            result += indentStr + op->toString() +
                     " [rows=" + std::to_string(op->estimatedRows) +
                     ", cost=" + std::to_string(op->totalCost) +
                     ", mem=" + std::to_string(op->estimateMemoryUsage()) + "]\n";

            for (const auto& child : op->children) {
                result += printPhysicalPlan(child.get(), indent + 1);
            }

            return result;
        }

        // Get execution statistics //NOLINTNEXTLINE(misc-no-recursion)
        static PhysicalStats getExecutionStats(const PhysicalOperator* op) {
            PhysicalStats totalStats;

            if (op) {
                totalStats += op->actualStats;

                for (const auto& child : op->children) {
                    totalStats += getExecutionStats(child.get());
                }
            }

            return totalStats;
        }

    private:
        // Plan table scan with access path selection
        std::unique_ptr<PhysicalOperator> planTableScan(const logical_planner::TableScanOperator* logicalScan) const {
            std::vector<AccessPath> accessPaths = generateAccessPaths(logicalScan->tableName);
            AccessPath bestPath = selectBestAccessPath(accessPaths, "");

            std::unique_ptr<PhysicalOperator> scanOp;

            if (bestPath.isIndexPath) {
                auto indexScan = std::make_unique<IndexScanOperator>(bestPath.tableName, bestPath.indexName);
                indexScan->keyColumns = bestPath.keyColumns;
                scanOp = std::move(indexScan);
            } else {
                auto seqScan = std::make_unique<SeqScanOperator>(bestPath.tableName, logicalScan->alias);
                seqScan->projectedColumns = logicalScan->projectedColumns;
                scanOp = std::move(seqScan);
            }

            scanOp->outputSchema = logicalScan->outputSchema;
            scanOp->estimatedRows = bestPath.estimatedRows;

            return scanOp;
        }

        // Plan filter operation
        static std::unique_ptr<PhysicalOperator> planFilter(const logical_planner::FilterOperator* logicalFilter) {
            auto filterOp = std::make_unique<FilterOperator>(logicalFilter->condition);
            filterOp->selectivity = logicalFilter->selectivity;
            filterOp->outputSchema = logicalFilter->outputSchema;

            return filterOp;
        }

        // Plan projection operation
        static std::unique_ptr<PhysicalOperator> planProjection(const logical_planner::ProjectionOperator* logicalProject) {
            auto projectOp = std::make_unique<ProjectionOperator>(logicalProject->projectedColumns);
            projectOp->expressions = logicalProject->expressions;
            projectOp->outputSchema = logicalProject->outputSchema;

            return projectOp;
        }

        // Plan join operation with algorithm selection
        std::unique_ptr<PhysicalOperator> planJoin(const logical_planner::JoinOperator* logicalJoin) const {
            JoinAlgorithm bestAlgorithm = selectBestJoinAlgorithm(logicalJoin);

            std::unique_ptr<PhysicalOperator> joinOp;

            switch (bestAlgorithm) {
                case JoinAlgorithm::HASH_JOIN: {
                    auto hashJoin = std::make_unique<HashJoinOperator>(logicalJoin->joinType, logicalJoin->joinCondition);

                    // Extract join keys from condition
                    extractJoinKeys(logicalJoin->joinCondition, hashJoin->leftKeys, hashJoin->rightKeys);

                    // Estimate hash table size based on smaller relation
                    const size_t leftRows = logicalJoin->children.empty() ? 1000 : logicalJoin->children[0]->estimatedRowCount;
                    const size_t rightRows = logicalJoin->children.size() < 2 ? 1000 : logicalJoin->children[1]->estimatedRowCount;

                    const size_t buildSideRows = std::min(leftRows, rightRows);
                    hashJoin->hashTableSize = buildSideRows * 100; // Rough estimate: 100 bytes per tuple

                    // Use Grace hash join for large datasets
                    if (hashJoin->hashTableSize > context.availableMemory / 2) {
                        hashJoin->isGraceHashJoin = true;
                    }

                    joinOp = std::move(hashJoin);
                    break;
                }

                case JoinAlgorithm::SORT_MERGE_JOIN: {
                    auto mergeJoin = std::make_unique<SortMergeJoinOperator>(logicalJoin->joinType, logicalJoin->joinCondition);
                    extractJoinKeys(logicalJoin->joinCondition, mergeJoin->leftKeys, mergeJoin->rightKeys);
                    joinOp = std::move(mergeJoin);
                    break;
                }

                case JoinAlgorithm::BLOCK_NESTED_LOOP: {
                    auto nlJoin = std::make_unique<NestedLoopJoinOperator>(logicalJoin->joinType, logicalJoin->joinCondition);
                    nlJoin->isBlockJoin = true;
                    nlJoin->blockSize = std::min(context.availableMemory / 4, static_cast<size_t>(64 * 1024)); // 64KB blocks
                    joinOp = std::move(nlJoin);
                    break;
                }

                case JoinAlgorithm::INDEX_NESTED_LOOP: {
                    // Try to use index on inner relation
                    auto nlJoin = std::make_unique<NestedLoopJoinOperator>(logicalJoin->joinType, logicalJoin->joinCondition);
                    nlJoin->isBlockJoin = false;
                    joinOp = std::move(nlJoin);
                    break;
                }

                default: {
                    auto nlJoin = std::make_unique<NestedLoopJoinOperator>(logicalJoin->joinType, logicalJoin->joinCondition);
                    joinOp = std::move(nlJoin);
                    break;
                }
            }

            joinOp->outputSchema = logicalJoin->outputSchema;
            return joinOp;
        }

        // Plan aggregate operation
        static std::unique_ptr<PhysicalOperator> planAggregate(const logical_planner::AggregateOperator* logicalAgg) {
            auto hashAgg = std::make_unique<HashAggregateOperator>(logicalAgg->groupByColumns);
            hashAgg->aggregates = logicalAgg->aggregates;
            hashAgg->havingCondition = logicalAgg->havingCondition;
            hashAgg->outputSchema = logicalAgg->outputSchema;

            // Estimate hash table size for grouping
            size_t inputRows = logicalAgg->children.empty() ? 1000 : logicalAgg->children[0]->estimatedRowCount;
            size_t estimatedGroups = logicalAgg->groupByColumns.empty() ? 1 : std::min(inputRows, inputRows / 10);
            hashAgg->hashTableSize = estimatedGroups * 200; // Rough estimate: 200 bytes per group

            return hashAgg;
        }

        // Plan sort operation
        std::unique_ptr<PhysicalOperator> planSort(const logical_planner::SortOperator* logicalSort) const {
            auto sortOp = std::make_unique<SortOperator>(logicalSort->sortColumns);
            sortOp->outputSchema = logicalSort->outputSchema;

            // Determine if external sort is needed
            size_t inputRows = logicalSort->children.empty() ? 1000 : logicalSort->children[0]->estimatedRowCount;
            size_t estimatedMemoryNeeded = inputRows * 100; // 100 bytes per tuple estimate

            if (estimatedMemoryNeeded > context.availableMemory / 2) {
                sortOp->isExternal = true;
                sortOp->memoryLimit = context.availableMemory / 2;
            } else {
                sortOp->memoryLimit = estimatedMemoryNeeded;
            }

            return sortOp;
        }

        // Plan limit operation
        static std::unique_ptr<PhysicalOperator> planLimit(const logical_planner::LimitOperator* logicalLimit) {
            auto limitOp = std::make_unique<LimitOperator>(logicalLimit->limit, logicalLimit->offset);
            limitOp->outputSchema = logicalLimit->outputSchema;
            return limitOp;
        }

        // Generate possible access paths for a table
        [[nodiscard]] std::vector<AccessPath> generateAccessPaths(const std::string& tableName) const {
            std::vector<AccessPath> paths;

            // Always include sequential scan
            AccessPath seqScan(tableName);
            if (auto stats = context.getTableStats(tableName)) {
                seqScan.estimatedRows = stats->rowCount;
                seqScan.estimatedCost = static_cast<double>(stats->rowCount) * context.seqPageCost;
            }
            paths.push_back(seqScan);

            // Add index scans for available indexes
            auto indexes = context.getTableIndexes(tableName);
            for (const auto& indexName : indexes) {
                std::vector<std::string> keyColumns = getIndexKeyColumns(tableName, indexName);
                AccessPath indexPath(tableName, indexName, keyColumns);

                if (auto stats = context.getTableStats(tableName)) {
                    // Estimate index scan cost (simplified)
                    indexPath.estimatedRows = stats->rowCount / 10; // Assume 10% selectivity
                    indexPath.estimatedCost = static_cast<double>(indexPath.estimatedRows) * context.randomPageCost;
                }

                paths.push_back(indexPath);
            }

            return paths;
        }

        // Select best access path based on cost
        static AccessPath selectBestAccessPath(const std::vector<AccessPath>& paths, const std::string& condition) {
            if (paths.empty()) {
                return AccessPath{("unknown")};
            }

            AccessPath bestPath = paths[0];
            double bestCost = bestPath.estimatedCost;

            for (const auto& path : paths) {
                double adjustedCost = path.estimatedCost;

                // Adjust cost based on condition selectivity
                if (!condition.empty() && path.isIndexPath) {
                    // If condition can use this index, reduce cost
                    if (canUseIndexForCondition(path, condition)) {
                        adjustedCost *= 0.1; // Much cheaper with index
                    }
                }

                if (adjustedCost < bestCost) {
                    bestCost = adjustedCost;
                    bestPath = path;
                }
            }

            return bestPath;
        }

        // Select best join algorithm
        JoinAlgorithm selectBestJoinAlgorithm(const logical_planner::JoinOperator* joinOp) const {
            if (joinOp->children.size() < 2) {
                return JoinAlgorithm::NESTED_LOOP;
            }

            size_t leftRows = joinOp->children[0]->estimatedRowCount;
            size_t rightRows = joinOp->children[1]->estimatedRowCount;

            // Hash join is generally good for equi-joins with moderate cardinalities
            if (hasEquiJoinCondition(joinOp->joinCondition)) {
                size_t smallerTable = std::min(leftRows, rightRows);
                size_t largerTable = std::max(leftRows, rightRows);

                // Hash join if smaller table fits in memory
                if (smallerTable * 100 < context.availableMemory / 2) {
                    return JoinAlgorithm::HASH_JOIN;
                }

                // Sort-merge join for large equi-joins
                if (largerTable > 10000) {
                    return JoinAlgorithm::SORT_MERGE_JOIN;
                }
            }

            // Block nested loop for small tables or non-equi joins
            if (leftRows < 1000 || rightRows < 1000) {
                return JoinAlgorithm::BLOCK_NESTED_LOOP;
            }

            // Default to nested loop
            return JoinAlgorithm::NESTED_LOOP;
        }

        // Physical plan optimizations //NOLINTNEXTLINE(misc-no-recursion)
        std::unique_ptr<PhysicalOperator> chooseJoinAlgorithms(std::unique_ptr<PhysicalOperator> plan) {
            if (!plan) return nullptr;

            // Recursively optimize children first
            for (auto& child : plan->children) {
                child = chooseJoinAlgorithms(std::move(child));
            }

            // Re-evaluate join algorithms with updated child statistics
            if (plan->type == PhysicalOperatorType::HASH_JOIN ||
                plan->type == PhysicalOperatorType::NESTED_LOOP_JOIN ||
                plan->type == PhysicalOperatorType::MERGE_JOIN) {

                // Re-calculate costs and potentially change algorithm
                plan->calculateCost(context);
                }

            return plan;
        }

        std::unique_ptr<PhysicalOperator> optimizeMemoryUsage(std::unique_ptr<PhysicalOperator> plan) {
            if (!plan) return nullptr;

            // Calculate total memory requirements
            size_t totalMemory = calculateTotalMemoryUsage(plan.get());

            if (totalMemory > context.availableMemory) {
                // Apply memory optimization strategies
                plan = spillToDisk(std::move(plan));
                plan = reduceHashTableSizes(std::move(plan));
            }

            return plan;
        }

        static std::unique_ptr<PhysicalOperator> addMaterializationNodes(std::unique_ptr<PhysicalOperator> plan) {
            // Add materialization nodes where beneficial
            // This is a simplified implementation
            return plan;
        }
        //NOLINTNEXTLINE(misc-no-recursion)
        std::unique_ptr<PhysicalOperator> optimizeForParallelism(std::unique_ptr<PhysicalOperator> plan) {
            if (!plan) return nullptr;

            // Mark operators that can benefit from parallelism
            if (plan->supportsParallelism()) {
                plan->parallelism = std::min(context.maxParallelism, static_cast<size_t>(4));
            }

            // Recursively optimize children
            for (auto& child : plan->children) {
                child = optimizeForParallelism(std::move(child));
            }

            return plan;
        }

        // Helper methods
        static std::vector<std::string> getIndexKeyColumns(const std::string& tableName, const std::string& indexName) {
            // In a real implementation, this would query the catalog
            // For now, return a placeholder
            return {"id"}; // Assume primary key index
        }

        static bool canUseIndexForCondition(const AccessPath& path, const std::string& condition) {
            // Simplified check - in practice, this would analyze the condition AST
            return std::any_of(path.keyColumns.begin(), path.keyColumns.end(),
                   [&](const auto& keyCol) {
                       return condition.find(keyCol) != std::string::npos;
                   });
        }

        static bool hasEquiJoinCondition(const std::string& condition) {
            return condition.find('=') != std::string::npos &&
                   condition.find("!=") == std::string::npos &&
                   condition.find("<>") == std::string::npos;
        }

        static void extractJoinKeys(const std::string& condition,
                            std::vector<std::string>& leftKeys,
                            std::vector<std::string>& rightKeys) {
            // Simplified extraction - in practice, this would parse the AST
            if (const size_t eqPos = condition.find('='); eqPos != std::string::npos) {
                std::string leftSide = condition.substr(0, eqPos);
                std::string rightSide = condition.substr(eqPos + 1);

                // Trim whitespace
                leftSide.erase(0, leftSide.find_first_not_of(" \t"));
                leftSide.erase(leftSide.find_last_not_of(" \t") + 1);
                rightSide.erase(0, rightSide.find_first_not_of(" \t"));
                rightSide.erase(rightSide.find_last_not_of(" \t") + 1);

                leftKeys.push_back(leftSide);
                rightKeys.push_back(rightSide);
            }
        }

        // NOLINTNEXTLINE(misc-no-recursion)
        static size_t calculateTotalMemoryUsage(const PhysicalOperator* op) {
            if (!op) return 0;

            size_t totalMemory = op->estimateMemoryUsage();

            for (const auto& child : op->children) {
                totalMemory += calculateTotalMemoryUsage(child.get());
            }

            return totalMemory;
        }

        // NOLINTNEXTLINE(misc-no-recursion)
        std::unique_ptr<PhysicalOperator> spillToDisk(std::unique_ptr<PhysicalOperator> plan) {
            if (!plan) return nullptr;

            // Enable external algorithms for memory-intensive operations
            if (plan->type == PhysicalOperatorType::HASH_JOIN) {
                if (const auto hashJoin = dynamic_cast<HashJoinOperator*>(plan.get()); hashJoin->hashTableSize > context.availableMemory / 2) {
                    hashJoin->isGraceHashJoin = true;
                }
            } else if (plan->type == PhysicalOperatorType::SORT) {
                if (const auto sortOp = dynamic_cast<SortOperator*>(plan.get()); sortOp->memoryLimit > context.availableMemory / 2) {
                    sortOp->isExternal = true;
                    sortOp->memoryLimit = context.availableMemory / 2;
                }
            }

            // Recursively process children
            for (auto& child : plan->children) {
                child = spillToDisk(std::move(child));
            }

            return plan;
        }

        // NOLINTNEXTLINE(misc-no-recursion)
        std::unique_ptr<PhysicalOperator> reduceHashTableSizes(std::unique_ptr<PhysicalOperator> plan) {
            if (!plan) return nullptr;

            // Reduce hash table sizes for hash-based operations
            if (plan->type == PhysicalOperatorType::HASH_JOIN) {
                auto hashJoin = dynamic_cast<HashJoinOperator*>(plan.get());
                hashJoin->hashTableSize = std::min(hashJoin->hashTableSize, context.availableMemory / 4);
            } else if (plan->type == PhysicalOperatorType::HASH_AGGREGATE) {
                auto hashAgg = dynamic_cast<HashAggregateOperator*>(plan.get());
                hashAgg->hashTableSize = std::min(hashAgg->hashTableSize, context.availableMemory / 4);
            }

            // Recursively process children
            for (auto& child : plan->children) {
                child = reduceHashTableSizes(std::move(child));
            }

            return plan;
        }

        static std::unique_ptr<PhysicalOperator> createDefaultPhysicalOperator(const logical_planner::LogicalOperator* logicalOp) {
            // Create a basic physical operator for unsupported logical operators
            // This is a fallback implementation
            auto seqScan = std::make_unique<SeqScanOperator>("unknown_table");
            seqScan->outputSchema = logicalOp->outputSchema;
            return seqScan;
        }
    };

    // Implementation of cost calculation methods for physical operators

    inline void SeqScanOperator::calculateCost(const ExecutionContext& context) {
        if (const auto stats = context.getTableStats(tableName)) {
            estimatedRows = static_cast<size_t>(stats->rowCount);
            estimatedWidth = stats->avgRowSize;

            // Apply filter selectivity if present
            if (!filter.empty()) {
                estimatedRows = static_cast<size_t>(static_cast<double>(estimatedRows) * 0.1); // Default 10% selectivity
            }

            // Cost = number of pages * sequential page cost
            size_t numPages = (stats->rowCount * stats->avgRowSize + context.pageSize - 1) / context.pageSize;
            startupCost = 0.0;
            totalCost = static_cast<double>(numPages) * context.seqPageCost;

            // Add CPU cost for processing tuples
            totalCost += static_cast<double>(stats->rowCount) * context.cpuTupleCost;

            // Add filter evaluation cost
            if (!filter.empty()) {
                totalCost += static_cast<double>(stats->rowCount) * context.cpuOperatorCost;
            }
        } else {
            // Default estimates
            estimatedRows = 1000;
            estimatedWidth = 100;
            startupCost = 0.0;
            totalCost = 100.0;
        }
    }

    inline void IndexScanOperator::calculateCost(const ExecutionContext& context) {
        if (const auto stats = context.getTableStats(tableName)) {
            // Estimate selectivity based on index usage
            double selectivity;// = 0.01; // Default 1% for index scans

            if (isUnique) {
                selectivity = 1.0 / static_cast<double>(stats->rowCount); // Single row for unique index
            } else {
                // Estimate based on key values if available
                selectivity = std::min(0.1, 1.0 / static_cast<double>(keyValues.size()));
            }

            estimatedRows = static_cast<size_t>(static_cast<double>(stats->rowCount) * selectivity);
            estimatedWidth = stats->avgRowSize;

            // Index access cost: tree traversal + leaf page reads
            startupCost = 4.0 * context.randomPageCost; // Assume 4-level B-tree
            totalCost = startupCost + static_cast<double>(estimatedRows) * context.randomPageCost;

            // Add CPU cost for index tuple processing
            totalCost += static_cast<double>(estimatedRows) * context.cpuIndexTupleCost;
        } else {
            estimatedRows = 100;
            estimatedWidth = 100;
            startupCost = 4.0;
            totalCost = 50.0;
        }
    }

    inline void NestedLoopJoinOperator::calculateCost(const ExecutionContext& context) {
        if (children.size() >= 2) {
            const double leftCost = children[0]->getTotalCost();
            const double rightCost = children[1]->getTotalCost();
            const size_t leftRows = children[0]->estimatedRows;
            const size_t rightRows = children[1]->estimatedRows;

            startupCost = children[0]->getStartupCost();

            if (isBlockJoin) {
                // Block nested loop join
                const size_t tuplesPerBlock = blockSize / children[0]->estimatedWidth;
                const size_t leftBlocks = (leftRows + tuplesPerBlock - 1) / tuplesPerBlock;

                totalCost = leftCost + static_cast<double>(leftBlocks) * rightCost;
                totalCost += static_cast<double>(leftRows) * static_cast<double>(rightRows) * context.cpuOperatorCost * 0.1; // Reduced CPU cost for blocks
            } else {
                // Tuple nested loop join
                totalCost = leftCost + static_cast<double>(leftRows) * rightCost;
                totalCost += static_cast<double>(leftRows) * static_cast<double>(rightRows) * context.cpuOperatorCost;
            }

            // Estimate output cardinality
            double joinSelectivity = 0.1; // Default join selectivity
            estimatedRows = static_cast<size_t>(static_cast<double>(leftRows) * static_cast<double>(rightRows) * joinSelectivity);
            estimatedWidth = children[0]->estimatedWidth + children[1]->estimatedWidth;
        }
    }

    inline void HashJoinOperator::calculateCost(const ExecutionContext& context) {
        if (children.size() >= 2) {
            double leftCost = children[0]->getTotalCost();
            double rightCost = children[1]->getTotalCost();
            size_t leftRows = children[0]->estimatedRows;
            size_t rightRows = children[1]->estimatedRows;

            // Choose smaller relation as build side
            size_t buildRows = std::min(leftRows, rightRows);
            size_t probeRows = std::max(leftRows, rightRows);

            startupCost = leftCost + rightCost;

            if (isGraceHashJoin) {
                // Grace hash join with partitioning
                totalCost = startupCost + static_cast<double>(buildRows + probeRows) * 2 * context.seqPageCost; // Write/read partitions
                totalCost += static_cast<double>(buildRows + probeRows) * context.cpuOperatorCost * 2; // Hash and compare
            } else {
                // In-memory hash join
                totalCost = startupCost + static_cast<double>(buildRows) * context.cpuOperatorCost; // Build hash table
                totalCost += static_cast<double>(probeRows) * context.cpuOperatorCost; // Probe phase
            }

            // Estimate output cardinality
            double joinSelectivity = 0.1;
            estimatedRows = static_cast<size_t>(static_cast<double>(leftRows * rightRows) * joinSelectivity);
            estimatedWidth = children[0]->estimatedWidth + children[1]->estimatedWidth;

            // Update hash table size estimate
            hashTableSize = buildRows * estimatedWidth;
        }
    }

    inline void SortMergeJoinOperator::calculateCost(const ExecutionContext& context) {
        if (children.size() >= 2) {
            const double leftCost = children[0]->getTotalCost();
            const double rightCost = children[1]->getTotalCost();
            const size_t leftRows = children[0]->estimatedRows;
            const size_t rightRows = children[1]->estimatedRows;

            startupCost = leftCost + rightCost;

            // Add sorting cost if not already sorted
            if (!leftSorted) {
                startupCost += static_cast<double>(leftRows) * std::log2(leftRows) * context.cpuOperatorCost;
            }
            if (!rightSorted) {
                startupCost += static_cast<double>(rightRows) * std::log2(rightRows) * context.cpuOperatorCost;
            }

            // Merge cost
            totalCost = startupCost + static_cast<double>(leftRows + rightRows) * context.cpuOperatorCost;

            // Estimate output cardinality
            double joinSelectivity = 0.1;
            estimatedRows = static_cast<size_t>(static_cast<double>(leftRows * rightRows) * joinSelectivity);
            estimatedWidth = children[0]->estimatedWidth + children[1]->estimatedWidth;
        }
    }

    inline void FilterOperator::calculateCost(const ExecutionContext& context) {
        if (!children.empty()) {
            const double childCost = children[0]->getTotalCost();
            const size_t inputRows = children[0]->estimatedRows;

            startupCost = children[0]->getStartupCost();
            totalCost = childCost + static_cast<double>(inputRows) * context.cpuOperatorCost;

            estimatedRows = static_cast<size_t>(static_cast<double>(inputRows) * selectivity);
            estimatedWidth = children[0]->estimatedWidth;
        }
    }

    inline void ProjectionOperator::calculateCost(const ExecutionContext& context) {
        if (!children.empty()) {
            double childCost = children[0]->getTotalCost();
            size_t inputRows = children[0]->estimatedRows;

            startupCost = children[0]->getStartupCost();
            totalCost = childCost + static_cast<double>(inputRows) * context.cpuOperatorCost * 0.5; // Projection is cheap

            estimatedRows = inputRows;

            // Estimate output width based on projected columns
            size_t projectedWidth = children[0]->estimatedWidth;
            if (!projectedColumns.empty() && projectedColumns[0] != "*") {
                projectedWidth = projectedColumns.size() * 20; // Rough estimate
            }
            estimatedWidth = projectedWidth;
        }
    }

    inline void HashAggregateOperator::calculateCost(const ExecutionContext& context) {
        if (!children.empty()) {
            double childCost = children[0]->getTotalCost();
            size_t inputRows = children[0]->estimatedRows;

            startupCost = children[0]->getStartupCost();

            // Cost includes building hash table and computing aggregates
            totalCost = childCost + static_cast<double>(inputRows) * context.cpuOperatorCost * 2;

            // Estimate number of groups
            if (groupByColumns.empty()) {
                estimatedRows = 1; // Single aggregate
            } else {
                // Estimate based on group by column cardinality
                estimatedRows = std::min(inputRows, static_cast<size_t>(static_cast<double>(inputRows) * 0.1));
            }

            estimatedWidth = groupByColumns.size() * 20 + aggregates.size() * 8; // Rough estimate
            hashTableSize = estimatedRows * estimatedWidth * 2; // Hash table overhead
        }
    }

    inline void SortOperator::calculateCost(const ExecutionContext& context) {
        if (!children.empty()) {
            const size_t inputRows = children[0]->estimatedRows;

            startupCost = children[0]->getTotalCost(); // Must consume all input first

            if (isExternal) {
                // External sort with multiple passes
                size_t numPasses = 2; // Simplified: assume 2-pass external sort
                totalCost = startupCost + static_cast<double>(inputRows) * std::log2(inputRows) * context.cpuOperatorCost;
                totalCost += static_cast<double>(numPasses * inputRows) * context.seqPageCost; // I/O cost
            } else {
                // In-memory sort
                totalCost = startupCost + static_cast<double>(inputRows) * std::log2(inputRows) * context.cpuOperatorCost;
            }

            estimatedRows = inputRows;
            estimatedWidth = children[0]->estimatedWidth;
        }
    }

    inline void LimitOperator::calculateCost(const ExecutionContext& context) {
        if (!children.empty()) {
            startupCost = children[0]->getStartupCost();

            // Limit can stop early, so cost is proportional to limit + offset
            const size_t tuplesNeeded = std::min(children[0]->estimatedRows, limit + offset);
            const double fractionNeeded = static_cast<double>(tuplesNeeded) / static_cast<double>(children[0]->estimatedRows);

            totalCost = startupCost + children[0]->getTotalCost() * fractionNeeded;

            estimatedRows = std::min(children[0]->estimatedRows, limit);
            estimatedWidth = children[0]->estimatedWidth;
        }
    }

    // Implementation of execution interface methods (stubs for demonstration)
    inline bool SeqScanOperator::open(ExecutionContext& context) {
        // Initialize table scan
        return true;
    }

    inline bool SeqScanOperator::getNext(ExecutionContext& context, std::vector<std::string>& tuple) {
        // Read next tuple from table
        return false; // End of scan
    }

    inline void SeqScanOperator::close(ExecutionContext& context) {
        // Cleanup resources
    }

    inline bool IndexScanOperator::open(ExecutionContext& context) { return true; }
    inline bool IndexScanOperator::getNext(ExecutionContext& context, std::vector<std::string>& tuple) { return false; }
    inline void IndexScanOperator::close(ExecutionContext& context) {}

    inline bool NestedLoopJoinOperator::open(ExecutionContext& context) { return true; }
    inline bool NestedLoopJoinOperator::getNext(ExecutionContext& context, std::vector<std::string>& tuple) { return false; }
    inline void NestedLoopJoinOperator::close(ExecutionContext& context) {}

    inline bool HashJoinOperator::open(ExecutionContext& context) { return true; }
    inline bool HashJoinOperator::getNext(ExecutionContext& context, std::vector<std::string>& tuple) { return false; }
    inline void HashJoinOperator::close(ExecutionContext& context) {}

    inline bool SortMergeJoinOperator::open(ExecutionContext& context) { return true; }
    inline bool SortMergeJoinOperator::getNext(ExecutionContext& context, std::vector<std::string>& tuple) { return false; }
    inline void SortMergeJoinOperator::close(ExecutionContext& context) {}

    inline bool FilterOperator::open(ExecutionContext& context) { return true; }
    inline bool FilterOperator::getNext(ExecutionContext& context, std::vector<std::string>& tuple) { return false; }
    inline void FilterOperator::close(ExecutionContext& context) {}

    inline bool ProjectionOperator::open(ExecutionContext& context) { return true; }
    inline bool ProjectionOperator::getNext(ExecutionContext& context, std::vector<std::string>& tuple) { return false; }
    inline void ProjectionOperator::close(ExecutionContext& context) {}

    inline bool HashAggregateOperator::open(ExecutionContext& context) { return true; }
    inline bool HashAggregateOperator::getNext(ExecutionContext& context, std::vector<std::string>& tuple) { return false; }
    inline void HashAggregateOperator::close(ExecutionContext& context) {}

    inline bool SortOperator::open(ExecutionContext& context) { return true; }
    inline bool SortOperator::getNext(ExecutionContext& context, std::vector<std::string>& tuple) { return false; }
    inline void SortOperator::close(ExecutionContext& context) {}

    inline bool LimitOperator::open(ExecutionContext& context) { return true; }
    inline bool LimitOperator::getNext(ExecutionContext& context, std::vector<std::string>& tuple) { return false; }
    inline void LimitOperator::close(ExecutionContext& context) {}
}

//