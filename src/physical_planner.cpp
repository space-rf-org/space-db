#include "planner/physical_planner.h"

namespace physical_planner {
    SeqScanOperator::SeqScanOperator(std::string  table, std::string  tableAlias)
                : PhysicalOperator(PhysicalOperatorType::SEQ_SCAN), tableName(std::move(table)), alias(std::move(tableAlias)) {}
    std::string SeqScanOperator::toString() const  {
        std::string result = "SeqScan(" + tableName;
        if (!alias.empty()) {
            result += " AS " + alias;
        }
        if (!filter.empty()) {
            result += ", filter=" + filter;
        }
        result += ")";
        return result;
    }

    std::unique_ptr<PhysicalOperator> SeqScanOperator::clone() const {
        auto cloned = std::make_unique<SeqScanOperator>(tableName, alias);
        cloned->projectedColumns = projectedColumns;
        cloned->filter = filter;
        cloned->outputSchema = outputSchema;
        return cloned;
    }

    bool SeqScanOperator::supportsParallelism() const {
        return true; // Sequential scans can be parallelized
    }
    // Index Scan Operator
    IndexScanOperator::IndexScanOperator(std::string  table, std::string  index)
            : PhysicalOperator(PhysicalOperatorType::INDEX_SCAN), tableName(std::move(table)), indexName(std::move(index)) {}

    std::string IndexScanOperator::toString() const {
        std::string result = "IndexScan(" + tableName + "." + indexName;
        if (!scanCondition.empty()) {
            result += ", condition=" + scanCondition;
        }
        result += ")";
        return result;
    }

    std::unique_ptr<PhysicalOperator> IndexScanOperator::clone() const  {
        auto cloned = std::make_unique<IndexScanOperator>(tableName, indexName);
        cloned->keyColumns = keyColumns;
        cloned->keyValues = keyValues;
        cloned->scanCondition = scanCondition;
        cloned->isUnique = isUnique;
        cloned->outputSchema = outputSchema;
        return cloned;
    }

    // Nested Loop Join Operator
    NestedLoopJoinOperator::NestedLoopJoinOperator(logical_planner::JoinType type, std::string  condition)
            : PhysicalOperator(PhysicalOperatorType::NESTED_LOOP_JOIN), joinType(type), joinCondition(std::move(condition)) {}

    std::string NestedLoopJoinOperator::toString() const  {
        const std::string joinTypeStr = joinTypeToString(joinType);
        const std::string algorithm = isBlockJoin ? "BlockNestedLoopJoin" : "NestedLoopJoin";
        return algorithm + "(" + joinTypeStr + ", " + joinCondition + ")";
    }
    std::unique_ptr<PhysicalOperator> NestedLoopJoinOperator::clone() const {
        auto cloned = std::make_unique<NestedLoopJoinOperator>(joinType, joinCondition);
        cloned->isBlockJoin = isBlockJoin;
        cloned->blockSize = blockSize;
        cloned->outputSchema = outputSchema;
        return cloned;
    }

    // Hash Join Operator
    HashJoinOperator::HashJoinOperator(logical_planner::JoinType type, std::string  condition)
            : PhysicalOperator(PhysicalOperatorType::HASH_JOIN), joinType(type), joinCondition(std::move(condition)) {}

    std::string HashJoinOperator::toString() const override {
        std::string joinTypeStr = joinTypeToString(joinType);
        std::string algorithm = isGraceHashJoin ? "GraceHashJoin" : "HashJoin";
        return algorithm + "(" + joinTypeStr + ", " + joinCondition + ")";
    }

    std::unique_ptr<PhysicalOperator> HashJoinOperator::clone() const {
        auto cloned = std::make_unique<HashJoinOperator>(joinType, joinCondition);
        cloned->leftKeys = leftKeys;
        cloned->rightKeys = rightKeys;
        cloned->hashTableSize = hashTableSize;
        cloned->isGraceHashJoin = isGraceHashJoin;
        cloned->outputSchema = outputSchema;
        return cloned;
    }
} // namespace physical_planner