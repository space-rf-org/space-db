#include <memory>
#include <utility>
#include <vector>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <optional>

#include <sstream>

#include "planner/logical_planner.h"

namespace logical_planner {
    // Table scan operator
    TableScanOperator::TableScanOperator(std::string  table, std::string  tableAlias)
                : LogicalOperator(LogicalOperatorType::TABLE_SCAN), tableName(std::move(table)), alias(std::move(tableAlias)) {}

    std::string TableScanOperator::toString() const {
        std::string result = "TableScan(" + tableName;
        if (!alias.empty()) {
            result += " AS " + alias;
        }
        if (!projectedColumns.empty()) {
            result += ", columns=[";
            for (size_t i = 0; i < projectedColumns.size(); ++i) {
                if (i > 0) result += ", ";
                result += projectedColumns[i];
            }
            result += "]";
        }
        result += ")";
        return result;
    }

    std::unique_ptr<LogicalOperator> TableScanOperator::clone() const {
        auto cloned = std::make_unique<TableScanOperator>(tableName, alias);
        cloned->projectedColumns = projectedColumns;
        cloned->outputSchema = outputSchema;
        return cloned;
    }

    double TableScanOperator::calculateCost(const std::unordered_map<std::string, TableStats>& stats)  {
        if (const auto it = stats.find(tableName); it != stats.end()) {
            estimatedRowCount = it->second.rowCount;
            estimatedCost = static_cast<double>(estimatedRowCount); // Sequential scan cost
        } else {
            estimatedRowCount = 1000; // Default estimate
            estimatedCost = 1000.0;
        }
        return estimatedCost;
    }

    FilterOperator::FilterOperator(std::string  cond)
            : LogicalOperator(LogicalOperatorType::FILTER), condition(std::move(cond)) {}

    FilterOperator::FilterOperator(std::unique_ptr<sql_parser::ASTNode> condAST)
        : LogicalOperator(LogicalOperatorType::FILTER), conditionAST(std::move(condAST)) {
        if (conditionAST) {
            condition = conditionAST->toString();
        }
    }

    std::string FilterOperator::toString() const  {
        return "Filter(" + condition + ")";
    }

    std::unique_ptr<LogicalOperator> FilterOperator::clone() const  {
        auto cloned = std::make_unique<FilterOperator>(condition);
        cloned->selectivity = selectivity;
        cloned->outputSchema = outputSchema;
        return cloned;
    }

    double FilterOperator::calculateCost(const std::unordered_map<std::string, TableStats>& stats)  {
        if (!children.empty()) {
            double childCost = children[0]->calculateCost(stats);
            estimatedRowCount = static_cast<size_t>(static_cast<double>(children[0]->estimatedRowCount) * selectivity);
            estimatedCost = childCost + static_cast<double>(children[0]->estimatedRowCount) * 0.01; // CPU cost per row
        }
        return estimatedCost;
    }

    void FilterOperator::setSelectivity(const double sel) {
        selectivity = std::max(0.001, std::min(1.0, sel)); // Clamp between 0.1% and 100%
    }

    // Projection operator (SELECT columns)

    ProjectionOperator::ProjectionOperator(const std::vector<std::string>& columns)
        : LogicalOperator(LogicalOperatorType::PROJECT), projectedColumns(columns) {}

    std::string ProjectionOperator::toString() const {
        std::string result = "Project([";
        for (size_t i = 0; i < projectedColumns.size(); ++i) {
            if (i > 0) result += ", ";
            result += projectedColumns[i];
        }
        result += "])";
        return result;
    }

    std::unique_ptr<LogicalOperator> ProjectionOperator::clone() const {
        auto cloned = std::make_unique<ProjectionOperator>(projectedColumns);
        cloned->expressions = expressions;
        cloned->outputSchema = outputSchema;
        return cloned;
    }

    double ProjectionOperator::calculateCost(const std::unordered_map<std::string, TableStats>& stats)  {
        if (!children.empty()) {
            double childCost = children[0]->calculateCost(stats);
            estimatedRowCount = children[0]->estimatedRowCount;
            estimatedCost = childCost + static_cast<double>(estimatedRowCount) * 0.005; // Small CPU cost for projection
        }
        return estimatedCost;
    }


    // Join operator
    JoinOperator::JoinOperator(const JoinType type, std::string  condition)
        : LogicalOperator(LogicalOperatorType::NESTED_LOOP_JOIN), joinType(type), joinCondition(std::move(condition)) {}

    std::string JoinOperator::toString() const {
        std::string joinTypeStr;
        switch (joinType) {
            case JoinType::INNER: joinTypeStr = "INNER"; break;
            case JoinType::LEFT: joinTypeStr = "LEFT"; break;
            case JoinType::RIGHT: joinTypeStr = "RIGHT"; break;
            case JoinType::FULL: joinTypeStr = "FULL"; break;
            case JoinType::CROSS: joinTypeStr = "CROSS"; break;
            case JoinType::SEMI: joinTypeStr = "SEMI"; break;
            case JoinType::ANTI: joinTypeStr = "ANTI"; break;
        }

        std::string algorithmStr;
        switch (type) {
            case LogicalOperatorType::NESTED_LOOP_JOIN: algorithmStr = "NestedLoopJoin"; break;
            case LogicalOperatorType::HASH_JOIN: algorithmStr = "HashJoin"; break;
            case LogicalOperatorType::MERGE_JOIN: algorithmStr = "MergeJoin"; break;
            default: algorithmStr = "Join"; break;
        }

        return algorithmStr + "(" + joinTypeStr + ", " + joinCondition + ")";
    }

    std::unique_ptr<LogicalOperator> JoinOperator::clone() const {
        auto cloned = std::make_unique<JoinOperator>(joinType, joinCondition);
        cloned->type = type; // Preserve the join algorithm type
        cloned->joinColumns = joinColumns;
        cloned->outputSchema = outputSchema;
        return cloned;
    }

    double JoinOperator::calculateCost(const std::unordered_map<std::string, TableStats>& stats) {
        if (children.size() >= 2) {
            const double leftCost = children[0]->calculateCost(stats);
            const double rightCost = children[1]->calculateCost(stats);

            const size_t leftRows = children[0]->estimatedRowCount;
            const size_t rightRows = children[1]->estimatedRowCount;

            switch (type) {
                case LogicalOperatorType::NESTED_LOOP_JOIN:
                    estimatedCost = leftCost + rightCost + static_cast<double>(leftRows) * static_cast<double>(rightRows) * 0.001;
                    break;
                case LogicalOperatorType::HASH_JOIN:
                    estimatedCost = leftCost + rightCost + (static_cast<double>(leftRows) + static_cast<double>(rightRows)) * 0.01;
                    break;
                case LogicalOperatorType::MERGE_JOIN:
                    estimatedCost = leftCost + rightCost + (static_cast<double>(leftRows) + static_cast<double>(rightRows)) * 0.005;
                    break;
                default:
                    estimatedCost = leftCost + rightCost + static_cast<double>(leftRows) * static_cast<double>(rightRows) * 0.001;
            }

            // Estimate output cardinality
            double selectivity = 0.1; // Default join selectivity
            estimatedRowCount = static_cast<size_t>(static_cast<double>(leftRows) * static_cast<double>(rightRows) * selectivity);
        }
        return estimatedCost;
    }

    void JoinOperator::optimizeJoinAlgorithm() {
        if (children.size() >= 2) {
            const size_t leftRows = children[0]->estimatedRowCount;

            // Choose join algorithm based on cardinalities
            if (const size_t rightRows = children[1]->estimatedRowCount; leftRows < 1000 || rightRows < 1000) {
                type = LogicalOperatorType::NESTED_LOOP_JOIN;
            } else if (!joinColumns.empty()) {
                type = LogicalOperatorType::HASH_JOIN; // Good for equi-joins
            } else {
                type = LogicalOperatorType::NESTED_LOOP_JOIN; // Fallback
            }
        }
    }

    // Aggregate operator (GROUP BY, HAVING)
    AggregateOperator::AggregateOperator(const std::vector<std::string>& groupBy)
        : LogicalOperator(LogicalOperatorType::AGGREGATE), groupByColumns(groupBy) {}

    std::string AggregateOperator::toString() const {
        std::string result = "Aggregate(";

        if (!groupByColumns.empty()) {
            result += "GROUP BY [";
            for (size_t i = 0; i < groupByColumns.size(); ++i) {
                if (i > 0) result += ", ";
                result += groupByColumns[i];
            }
            result += "]";
        }

        if (!aggregates.empty()) {
            if (!groupByColumns.empty()) result += ", ";
            result += "AGG [";
            for (size_t i = 0; i < aggregates.size(); ++i) {
                if (i > 0) result += ", ";
                result += aggregateTypeToString(aggregates[i].first) + "(" + aggregates[i].second + ")";
            }
            result += "]";
        }

        if (!havingCondition.empty()) {
            result += ", HAVING " + havingCondition;
        }

        result += ")";
        return result;
    }

    std::unique_ptr<LogicalOperator> AggregateOperator::clone() const {
        auto cloned = std::make_unique<AggregateOperator>(groupByColumns);
        cloned->aggregates = aggregates;
        cloned->havingCondition = havingCondition;
        cloned->outputSchema = outputSchema;
        return cloned;
    }

    double AggregateOperator::calculateCost(const std::unordered_map<std::string, TableStats>& stats)  {
        if (!children.empty()) {
            const double childCost = children[0]->calculateCost(stats);
            const size_t inputRows = children[0]->estimatedRowCount;

            // Estimate output cardinality (depends on GROUP BY selectivity)
            if (groupByColumns.empty()) {
                estimatedRowCount = 1; // Single aggregate row
            } else {
                // Estimate based on group by column cardinality
                estimatedRowCount = std::min(inputRows, static_cast<size_t>(static_cast<double>(inputRows) * 0.1)); // Default 10% groups
            }

            // Cost includes input cost + sorting/hashing cost for grouping
            estimatedCost = childCost + static_cast<double>(inputRows) * std::log2(inputRows) * 0.001;
        }
        return estimatedCost;
    }

    std::string AggregateOperator::aggregateTypeToString(const AggregateType type) {
        switch (type) {
            case AggregateType::COUNT: return "COUNT";
            case AggregateType::SUM: return "SUM";
            case AggregateType::AVG: return "AVG";
            case AggregateType::MIN: return "MIN";
            case AggregateType::MAX: return "MAX";
            case AggregateType::STDDEV: return "STDDEV";
            case AggregateType::VARIANCE: return "VARIANCE";
            case AggregateType::COUNT_DISTINCT: return "COUNT_DISTINCT";
            default: return "UNKNOWN_AGG";
        }
    }
    ;

    // Sort operator (ORDER BY)

    SortOperator::SortOperator(const std::vector<std::pair<std::string, bool>>& columns)
       : LogicalOperator(LogicalOperatorType::SORT), sortColumns(columns) {}

    std::string SortOperator::toString() const  {
        std::string result = "Sort([";
        for (size_t i = 0; i < sortColumns.size(); ++i) {
            if (i > 0) result += ", ";
            result += sortColumns[i].first + (sortColumns[i].second ? " ASC" : " DESC");
        }
        result += "])";
        return result;
    }

    std::unique_ptr<LogicalOperator> SortOperator::clone() const  {
        auto cloned = std::make_unique<SortOperator>(sortColumns);
        cloned->outputSchema = outputSchema;
        return cloned;
    }

    double SortOperator::calculateCost(const std::unordered_map<std::string, TableStats>& stats) {
        if (!children.empty()) {
            double childCost = children[0]->calculateCost(stats);
            estimatedRowCount = children[0]->estimatedRowCount;
            // Sorting cost: O(n log n)
            estimatedCost = childCost + static_cast<double>(estimatedRowCount) * std::log2(estimatedRowCount) * 0.01;
        }
        return estimatedCost;
    };

    // Limit operator
    LimitOperator::LimitOperator(const size_t lim, const size_t off)
        : LogicalOperator(LogicalOperatorType::LIMIT), limit(lim), offset(off) {}

    std::string LimitOperator::toString() const {
        std::string result = "Limit(" + std::to_string(limit);
        if (offset > 0) {
            result += ", OFFSET " + std::to_string(offset);
        }
        result += ")";
        return result;
    }

    std::unique_ptr<LogicalOperator> LimitOperator::clone() const {
        auto cloned = std::make_unique<LimitOperator>(limit, offset);
        cloned->outputSchema = outputSchema;
        return cloned;
    }

    double LimitOperator::calculateCost(const std::unordered_map<std::string, TableStats>& stats) {
        if (!children.empty()) {
            double childCost = children[0]->calculateCost(stats);
            estimatedRowCount = std::min(children[0]->estimatedRowCount, limit);
            estimatedCost = childCost; // Limit doesn't add significant cost
        }
        return estimatedCost;
    }
// Main logical planner class
class LogicalPlanner {
private:
    std::unordered_map<std::string, TableStats> tableStats;
    std::unordered_map<std::string, std::vector<ColumnInfo>> tableSchemas;
    
public:
    LogicalPlanner() = default;
    
    // Add table statistics for cost estimation
    void addTableStats(const std::string& tableName, const TableStats& stats) {
        tableStats[tableName] = stats;
    }
    
    // Add table schema information
    void addTableSchema(const std::string& tableName, const std::vector<ColumnInfo>& schema) {
        tableSchemas[tableName] = schema;
    }
    
    // Main planning method
    std::unique_ptr<LogicalOperator> createPlan(const sql_parser::SelectStatement& selectStmt) {
        // 1. Start with FROM clause (table scans and joins)
        std::unique_ptr<LogicalOperator> plan = planFromClause(selectStmt);
        
        // 2. Add WHERE clause (filters)
        if (selectStmt.whereClause) {
            plan = planWhereClause(std::move(plan), selectStmt.whereClause.get());
        }
        
        // 3. Add GROUP BY and aggregates
        if (!selectStmt.groupByClause.empty() || hasAggregates(selectStmt.selectList)) {
            plan = planAggregation(std::move(plan), selectStmt);
        }
        
        // 4. Add HAVING clause
        if (selectStmt.havingClause) {
            plan = planHavingClause(std::move(plan), selectStmt.havingClause.get());
        }
        
        // 5. Add projection (SELECT list)
        plan = planProjection(std::move(plan), selectStmt.selectList);
        
        // 6. Add ORDER BY
        if (!selectStmt.orderByClause.empty()) {
            plan = planOrderBy(std::move(plan), selectStmt.orderByClause);
        }
        
        // 7. Add LIMIT/OFFSET
        if (selectStmt.limitClause.has_value() || selectStmt.offsetClause.has_value()) {
            plan = planLimit(std::move(plan), selectStmt.limitClause, selectStmt.offsetClause);
        }
        
        // 8. Optimize the plan
        optimizePlan(plan.get());
        
        return plan;
    }
    
    // Print the logical plan
    //NOLINTNEXTLINE
    std::string printPlan(const LogicalOperator* op, int indent = 0) const {
        if (!op) return "";
        
        std::string result;
        std::string indentStr(indent * 2, ' ');
        
        result += indentStr + op->toString() + 
                 " [rows=" + std::to_string(op->estimatedRowCount) + 
                 ", cost=" + std::to_string(op->estimatedCost) + "]\n";
        
        for (const auto& child : op->children) {
            result += printPlan(child.get(), indent + 1);
        }
        
        return result;
    }
    
private:
    // Plan FROM clause (tables and joins)
    static std::unique_ptr<LogicalOperator> planFromClause(const sql_parser::SelectStatement& selectStmt) {
        if (selectStmt.fromClause.empty()) {
            // No FROM clause - create a dummy table scan
            return std::make_unique<TableScanOperator>("dual");
        }
        
        std::unique_ptr<LogicalOperator> result;
        
        // Process first table
        if (const auto identifier = dynamic_cast<sql_parser::Identifier*>(selectStmt.fromClause[0].get())) {
            //result = std::make_unique<TableScanOperator>(identifier->name, identifier->alias);
            result = std::make_unique<TableScanOperator>(identifier->name, identifier->alias.value_or(""));

        }
        
        // Process additional tables and joins
        for (size_t i = 1; i < selectStmt.fromClause.size(); ++i) {
            if (auto joinClause = dynamic_cast<sql_parser::JoinClause*>(selectStmt.fromClause[i].get())) {
                result = planJoin(std::move(result), joinClause);
            }
        }
        
        return result;
    }
    
    // Plan JOIN operations
    static std::unique_ptr<LogicalOperator> planJoin(std::unique_ptr<LogicalOperator> left,
                                             const sql_parser::JoinClause* joinClause) {
        JoinType joinType = convertJoinType(joinClause->joinType);
        std::string condition = joinClause->condition ? joinClause->condition->toString() : "";
        
        auto joinOp = std::make_unique<JoinOperator>(joinType, condition);
        joinOp->addChild(std::move(left));
        
        // Add right side of join
        if (auto identifier = dynamic_cast<sql_parser::Identifier*>(joinClause->table.get())) {
            auto rightScan = std::make_unique<TableScanOperator>(identifier->name,
                identifier->alias.value_or(""));

            joinOp->addChild(std::move(rightScan));
        }
        
        return joinOp;
    }
    
    // Plan WHERE clause
    static std::unique_ptr<LogicalOperator> planWhereClause(std::unique_ptr<LogicalOperator> input,
                                                    const sql_parser::ASTNode* whereCondition) {
        auto filterOp = std::make_unique<FilterOperator>(whereCondition->toString());
        filterOp->addChild(std::move(input));
        
        // Estimate selectivity based on condition type
        estimateFilterSelectivity(filterOp.get(), whereCondition);
        
        return filterOp;
    }
    
    // Plan aggregation (GROUP BY)
    static std::unique_ptr<LogicalOperator> planAggregation(std::unique_ptr<LogicalOperator> input,
                                                    const sql_parser::SelectStatement& selectStmt) {
        std::vector<std::string> groupByColumns;
        groupByColumns.reserve(selectStmt.groupByClause.size());
for (const auto& groupCol : selectStmt.groupByClause) {
            groupByColumns.push_back(groupCol->toString());
        }
        
        auto aggOp = std::make_unique<AggregateOperator>(groupByColumns);
        
        // Extract aggregates from SELECT list
        extractAggregates(selectStmt.selectList, aggOp.get());
        
        aggOp->addChild(std::move(input));
        return aggOp;
    }
    
    // Plan HAVING clause
    static std::unique_ptr<LogicalOperator> planHavingClause(std::unique_ptr<LogicalOperator> input,
                                                     const sql_parser::ASTNode* havingCondition) {
        auto filterOp = std::make_unique<FilterOperator>(havingCondition->toString());
        filterOp->addChild(std::move(input));
        return filterOp;
    }
    
    // Plan projection (SELECT list)
    static std::unique_ptr<LogicalOperator> planProjection(std::unique_ptr<LogicalOperator> input,
                                                   const std::vector<std::unique_ptr<sql_parser::ASTNode>>& selectList) {
        std::vector<std::string> projectedColumns;
        projectedColumns.reserve(selectList.size());
for (const auto& selectItem : selectList) {
            projectedColumns.push_back(selectItem->toString());
        }
        
        auto projOp = std::make_unique<ProjectionOperator>(projectedColumns);
        projOp->addChild(std::move(input));
        return projOp;
    }
    
    // Plan ORDER BY
    static std::unique_ptr<LogicalOperator> planOrderBy(std::unique_ptr<LogicalOperator> input,
                                                const std::vector<std::pair<std::unique_ptr<sql_parser::ASTNode>, bool>>& orderBy) {
        std::vector<std::pair<std::string, bool>> sortColumns;
        sortColumns.reserve(orderBy.size());
for (const auto& orderItem : orderBy) {
            sortColumns.emplace_back(orderItem.first->toString(), orderItem.second);
        }
        
        auto sortOp = std::make_unique<SortOperator>(sortColumns);
        sortOp->addChild(std::move(input));
        return sortOp;
    }
    
    // Plan LIMIT/OFFSET
    static std::unique_ptr<LogicalOperator> planLimit(std::unique_ptr<LogicalOperator> input,
                                              const std::optional<int>& limit,
                                              const std::optional<int>& offset) {
        size_t limitVal = limit.has_value() ? static_cast<size_t>(limit.value()) : SIZE_MAX;
        size_t offsetVal = offset.has_value() ? static_cast<size_t>(offset.value()) : 0;
        
        auto limitOp = std::make_unique<LimitOperator>(limitVal, offsetVal);
        limitOp->addChild(std::move(input));
        return limitOp;
    }
    
    // Helper methods
    static JoinType convertJoinType(sql_parser::JoinClause::JoinType sqlJoinType) {
        switch (sqlJoinType) {
            case sql_parser::JoinClause::JoinType::INNER: return JoinType::INNER;
            case sql_parser::JoinClause::JoinType::LEFT: return JoinType::LEFT;
            case sql_parser::JoinClause::JoinType::RIGHT: return JoinType::RIGHT;
            case sql_parser::JoinClause::JoinType::FULL_OUTER: return JoinType::FULL;
            default: return JoinType::INNER;
        }
    }
    
    static bool hasAggregates(const std::vector<std::unique_ptr<sql_parser::ASTNode>>& selectList) {
        return std::any_of(selectList.begin(), selectList.end(),
        [](const std::unique_ptr<sql_parser::ASTNode>& item) {
            if (const auto funcCall = dynamic_cast<sql_parser::FunctionCall*>(item.get())) {
                return isAggregateFunction(funcCall->name);
            }
            return false;
        });
    }
    
    static bool isAggregateFunction(const std::string& funcName) {
        static const std::unordered_set<std::string> aggFunctions = {
            "COUNT", "SUM", "AVG", "MIN", "MAX", "STDDEV", "VARIANCE"
        };
        return aggFunctions.count(funcName) > 0;
    }
    
    void static extractAggregates(const std::vector<std::unique_ptr<sql_parser::ASTNode>>& selectList,
                          AggregateOperator* aggOp) {
        for (const auto& item : selectList) {
            if (auto funcCall = dynamic_cast<sql_parser::FunctionCall*>(item.get())) {
                if (isAggregateFunction(funcCall->name)) {
                    AggregateType aggType = stringToAggregateType(funcCall->name);
                    std::string column = funcCall->arguments.empty() ? "*" : funcCall->arguments[0]->toString();
                    aggOp->aggregates.emplace_back(aggType, column);
                }
            }
        }
    }
    
    static AggregateType stringToAggregateType(const std::string& funcName) {
        if (funcName == "COUNT") return AggregateType::COUNT;
        if (funcName == "SUM") return AggregateType::SUM;
        if (funcName == "AVG") return AggregateType::AVG;
        if (funcName == "MIN") return AggregateType::MIN;
        if (funcName == "MAX") return AggregateType::MAX;
        if (funcName == "STDDEV") return AggregateType::STDDEV;
        if (funcName == "VARIANCE") return AggregateType::VARIANCE;
        return AggregateType::COUNT; // Default
    }
    
    static void estimateFilterSelectivity(FilterOperator* filterOp, const sql_parser::ASTNode* condition) {
        // Basic selectivity estimation based on condition type
        if (auto binaryExpr = dynamic_cast<const sql_parser::BinaryExpression*>(condition)) {
            switch (binaryExpr->operator_) {
                case sql_parser::TokenType::EQUALS:
                    filterOp->setSelectivity(0.1); // 10% for equality
                    break;
                case sql_parser::TokenType::LESS_THAN:
                case sql_parser::TokenType::GREATER_THAN:
                    filterOp->setSelectivity(0.33); // 33% for range
                    break;
                case sql_parser::TokenType::LIKE:
                    filterOp->setSelectivity(0.15); // 15% for LIKE
                    break;
                case sql_parser::TokenType::AND:
                    filterOp->setSelectivity(0.05); // 5% for AND (more selective)
                    break;
                case sql_parser::TokenType::OR:
                    filterOp->setSelectivity(0.25); // 25% for OR (less selective)
                    break;
                default:
                    filterOp->setSelectivity(0.1); // Default 10%
            }
        } else if (auto unaryExpr = dynamic_cast<const sql_parser::UnaryExpression*>(condition)) {
            if (unaryExpr->operator_ == sql_parser::TokenType::NOT) {
                filterOp->setSelectivity(0.9); // 90% for NOT (inverts selectivity)
            }
        } else {
            filterOp->setSelectivity(0.1); // Default 10%
        }
    }
    
    // Plan optimization methods
    // NOLINTNEXTLINE
    void optimizePlan(LogicalOperator* root) {
        if (!root) return;
        
        // Apply various optimization rules
        pushDownFilters(root);
        optimizeJoins(root);
        eliminateRedundantOperators(root);
        
        // Recursively optimize children
        for (auto& child : root->children) {
            optimizePlan(child.get());
        }
        
        // Calculate final costs
        root->calculateCost(tableStats);
    }
    
    // Push filters down the tree (closer to table scans)
    static void pushDownFilters(LogicalOperator* op) {
        if (op->type != LogicalOperatorType::FILTER) return;
        
        auto* filter = dynamic_cast<FilterOperator*>(op);
        
        // Try to push filter below joins
        if (!op->children.empty() && op->children[0]->type == LogicalOperatorType::NESTED_LOOP_JOIN) {
            auto* join = dynamic_cast<JoinOperator*>(op->children[0].get());
            
            // Analyze filter condition to see if it can be pushed to left or right side
            std::vector<std::string> referencedTables = extractReferencedTables(filter->condition);
            
            if (referencedTables.size() == 1) {
                // Filter references only one table - can potentially push down
                if (canPushFilterToChild(filter, join, 0, referencedTables[0])) {
                    pushFilterToChild(filter, join, 0);
                } else if (canPushFilterToChild(filter, join, 1, referencedTables[0])) {
                    pushFilterToChild(filter, join, 1);
                }
            }
        }
    }
    
    // Optimize join order and algorithms
    static void optimizeJoins(LogicalOperator* op) {
        if (!op) return; // No children - nothing to optimize -// TODO: what is the best check here?
        
        if (op->type == LogicalOperatorType::NESTED_LOOP_JOIN ||
            op->type == LogicalOperatorType::HASH_JOIN ||
            op->type == LogicalOperatorType::MERGE_JOIN) {
            
            auto* join = dynamic_cast<JoinOperator*>(op);
            
            // Optimize join algorithm based on estimated cardinalities
            join->optimizeJoinAlgorithm();
            
            // Consider join reordering for multi-way joins
            if (op->children.size() >= 2) {
                optimizeJoinOrder(join);
            }
        }
    }
    
    // Eliminate redundant operators
    static void eliminateRedundantOperators(LogicalOperator* op) {
        if (!op) return;
        
        // Remove redundant projections
        if (op->type == LogicalOperatorType::PROJECT && !op->children.empty()) {
            // If projection doesn't change schema, it might be redundant
            if (auto* proj = dynamic_cast<ProjectionOperator*>(op); isProjectionRedundant(proj)) {
                // Replace this projection with its child
                // (This is a simplified check - full implementation would be more complex)
            }
        }
        
        // Combine adjacent filters
        if (op->type == LogicalOperatorType::FILTER && 
            !op->children.empty() && 
            op->children[0]->type == LogicalOperatorType::FILTER) {
            
            auto* upperFilter = dynamic_cast<FilterOperator*>(op);
            auto* lowerFilter = dynamic_cast<FilterOperator*>(op->children[0].get());
            
            // Combine conditions with AND
            std::string combinedCondition = "(" + upperFilter->condition + ") AND (" + lowerFilter->condition + ")";
            upperFilter->condition = combinedCondition;
            
            // Replace lower filter's child as upper filter's child
            if (!lowerFilter->children.empty()) {
                upperFilter->children[0] = std::move(lowerFilter->children[0]);
            }
        }
    }
    
    // Helper methods for optimization
    static std::vector<std::string> extractReferencedTables(const std::string& condition) {
        std::vector<std::string> tables;
        
        // Simple implementation - look for table.column patterns
        // In practice, this would use proper AST analysis
        size_t pos = 0;
        while ((pos = condition.find('.', pos)) != std::string::npos) {
            // Find the table name before the dot
            size_t start = pos;
            while (start > 0 && (std::isalnum(condition[start-1]) || condition[start-1] == '_')) {
                start--;
            }
            
            if (start < pos) {
                std::string tableName = condition.substr(start, pos - start);
                if (std::find(tables.begin(), tables.end(), tableName) == tables.end()) {
                    tables.push_back(tableName);
                }
            }
            pos++;
        }
        
        return tables;
    }
    
    static bool canPushFilterToChild(FilterOperator* filter, const JoinOperator* join,
                             const int childIndex, const std::string& tableName) {
        // Check if the filter can be pushed to the specified child
        // This would involve analyzing the join type and condition
        
        if (childIndex >= static_cast<int>(join->children.size())) {
            return false;
        }
        
        // For inner joins, most filters can be pushed down
        if (join->joinType == JoinType::INNER) {
            return true;
        }
        
        // For outer joins, be more careful about which filters can be pushed
        if (join->joinType == JoinType::LEFT && childIndex == 0) {
            return true; // Can push to left side of LEFT JOIN
        }
        
        if (join->joinType == JoinType::RIGHT && childIndex == 1) {
            return true; // Can push to right side of RIGHT JOIN
        }
        
        return false;
    }
    
    static void pushFilterToChild(FilterOperator* filter, JoinOperator* join, int childIndex) {
        if (childIndex >= static_cast<int>(join->children.size())) {
            return;
        }
        
        // Create a new filter operator
        auto newFilter = std::make_unique<FilterOperator>(filter->condition);
        newFilter->setSelectivity(filter->selectivity);
        
        // Insert the filter between the join and its child
        newFilter->addChild(std::move(join->children[childIndex]));
        join->children[childIndex] = std::move(newFilter);
    }
    
    static void optimizeJoinOrder(JoinOperator* join) {
        // Simple join reordering based on estimated cardinalities
        if (join->children.size() >= 2) {
            size_t leftRows = join->children[0]->estimatedRowCount;
            size_t rightRows = join->children[1]->estimatedRowCount;
            
            // For hash joins, put smaller relation on the right (build side)
            if (join->type == LogicalOperatorType::HASH_JOIN && leftRows < rightRows) {
                std::swap(join->children[0], join->children[1]);
                
                // Adjust join type for swapped operands
                if (join->joinType == JoinType::LEFT) {
                    join->joinType = JoinType::RIGHT;
                } else if (join->joinType == JoinType::RIGHT) {
                    join->joinType = JoinType::LEFT;
                }
            }
        }
    }
    
    static bool isProjectionRedundant(const ProjectionOperator* proj) {
        // Check if projection is selecting all columns in the same order
        // This is a simplified check - full implementation would compare schemas
        
        if (proj->children.empty()) {
            return false;
        }
        
        // If projecting everything (*), might be redundant
        if (proj->projectedColumns.size() == 1 && proj->projectedColumns[0] == "*") {
            return true;
        }
        
        return false;
    }
};

// Logical plan execution statistics
struct ExecutionStats {
    double totalCost = 0.0;
    size_t totalRows = 0;
    std::unordered_map<LogicalOperatorType, size_t> operatorCounts;
    std::chrono::milliseconds planningTime{0};
    
    void addOperator(const LogicalOperatorType type, const double cost, const size_t rows) {
        totalCost += cost;
        totalRows += rows;
        operatorCounts[type]++;
    }
    
    [[nodiscard]] std::string toString() const {
        std::ostringstream oss;
        oss << "Execution Statistics:\n";
        oss << "  Total Cost: " << totalCost << "\n";
        oss << "  Total Rows: " << totalRows << "\n";
        oss << "  Planning Time: " << planningTime.count() << "ms\n";
        oss << "  Operator Counts:\n";
        
        for (const auto& [type, count] : operatorCounts) {
            oss << "    " << operatorTypeToString(type) << ": " << count << "\n";
        }
        
        return oss.str();
    }
    
private:
    static std::string operatorTypeToString(LogicalOperatorType type) {
        switch (type) {
            case LogicalOperatorType::TABLE_SCAN: return "TableScan";
            case LogicalOperatorType::INDEX_SCAN: return "IndexScan";
            case LogicalOperatorType::NESTED_LOOP_JOIN: return "NestedLoopJoin";
            case LogicalOperatorType::HASH_JOIN: return "HashJoin";
            case LogicalOperatorType::MERGE_JOIN: return "MergeJoin";
            case LogicalOperatorType::FILTER: return "Filter";
            case LogicalOperatorType::PROJECT: return "Project";
            case LogicalOperatorType::AGGREGATE: return "Aggregate";
            case LogicalOperatorType::SORT: return "Sort";
            case LogicalOperatorType::LIMIT: return "Limit";
            case LogicalOperatorType::UNION: return "Union";
            case LogicalOperatorType::INTERSECT: return "Intersect";
            case LogicalOperatorType::EXCEPT: return "Except";
            case LogicalOperatorType::SUBQUERY: return "Subquery";
            default: return "Unknown";
        }
    }
};

// Utility function to create a sample planner with test data
std::unique_ptr<LogicalPlanner> createSamplePlanner() {
    auto planner = std::make_unique<LogicalPlanner>();
    
    // Add sample table statistics
    TableStats usersStats;
    usersStats.tableName = "users";
    usersStats.rowCount = 10000;
    usersStats.avgRowSize = 100;
    usersStats.columnCardinality["id"] = 10000;
    usersStats.columnCardinality["name"] = 9500;
    usersStats.columnCardinality["status"] = 3;
    usersStats.indexedColumns.insert("id");
    planner->addTableStats("users", usersStats);
    
    TableStats ordersStats;
    ordersStats.tableName = "orders";
    ordersStats.rowCount = 50000;
    ordersStats.avgRowSize = 80;
    ordersStats.columnCardinality["id"] = 50000;
    ordersStats.columnCardinality["user_id"] = 8000;
    ordersStats.columnCardinality["status"] = 5;
    ordersStats.indexedColumns.insert("id");
    ordersStats.indexedColumns.insert("user_id");
    planner->addTableStats("orders", ordersStats);
    
    TableStats profilesStats;
    profilesStats.tableName = "profiles";
    profilesStats.rowCount = 8000;
    profilesStats.avgRowSize = 150;
    profilesStats.columnCardinality["user_id"] = 8000;
    profilesStats.columnCardinality["phone"] = 7500;
    profilesStats.indexedColumns.insert("user_id");
    planner->addTableStats("profiles", profilesStats);
    
    // Add sample schemas
    std::vector<ColumnInfo> usersSchema = {
        {"users", "id", "", "INTEGER", false, true, true},
        {"users", "name", "", "VARCHAR(100)", true, false, false},
        {"users", "age", "", "INTEGER", true, false, false},
        {"users", "status", "", "VARCHAR(20)", true, false, false},
        {"users", "created_at", "", "TIMESTAMP", true, false, false}
    };
    planner->addTableSchema("users", usersSchema);
    
    std::vector<ColumnInfo> ordersSchema = {
        {"orders", "id", "", "INTEGER", false, true, true},
        {"orders", "user_id", "", "INTEGER", false, false, true},
        {"orders", "amount", "", "DECIMAL(10,2)", true, false, false},
        {"orders", "status", "", "VARCHAR(20)", true, false, false},
        {"orders", "created_at", "", "TIMESTAMP", true, false, false}
    };
    planner->addTableSchema("orders", ordersSchema);
    
    std::vector<ColumnInfo> profilesSchema = {
        {"profiles", "user_id", "", "INTEGER", false, true, true},
        {"profiles", "phone", "", "VARCHAR(20)", true, false, false},
        {"profiles", "address", "", "TEXT", true, false, false}
    };
    planner->addTableSchema("profiles", profilesSchema);
    
    return planner;
}

} // namespace logical_planner