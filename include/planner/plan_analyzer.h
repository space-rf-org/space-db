#pragma once

#include <memory>
#include <vector>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <optional>
#include <algorithm>
#include <chrono>
#include <queue>
#include <functional>
#include <sstream>
#include <iomanip>

// Include your existing physical planner header
// #include "physical_planner.h"

namespace physical_planner {

    // Analysis results for plan optimization
    struct PlanAnalysisResult {
        double totalCost = 0.0;
        double totalMemoryUsage = 0.0;
        double estimatedExecutionTime = 0.0;
        size_t totalEstimatedRows = 0;
        
        // Performance bottlenecks
        std::vector<std::string> bottlenecks;
        std::vector<std::string> recommendations;
        
        // Resource usage breakdown
        std::unordered_map<std::string, double> costBreakdown;
        std::unordered_map<std::string, double> memoryBreakdown;
        
        // Plan characteristics
        bool hasExpensiveOperations = false;
        bool hasMemoryIntensiveOperations = false;
        bool hasParallelizableOperations = false;
        int maxParallelism = 1;
        
        // Operator statistics
        struct OperatorAnalysis {
            std::string operatorType;
            std::string operatorName;
            double cost = 0.0;
            double memoryUsage = 0.0;
            size_t estimatedRows = 0;
            double selectivity = 1.0;
            std::vector<std::string> issues;
            std::vector<std::string> suggestions;
        };
        
        std::vector<OperatorAnalysis> operatorAnalysis;
    };

    // Plan Analyzer class for analyzing and optimizing physical plans
    class PlanAnalyzer {
    private:
        const ExecutionContext& context;
        
        // Analysis thresholds
        static constexpr double HIGH_COST_THRESHOLD = 10000.0;
        static constexpr double HIGH_MEMORY_THRESHOLD = 0.8; // 80% of available memory
        static constexpr size_t LARGE_TABLE_THRESHOLD = 100000;
        static constexpr double LOW_SELECTIVITY_THRESHOLD = 0.01;

    public:
        explicit PlanAnalyzer(const ExecutionContext& ctx) : context(ctx) {}

        // Main analysis method
        PlanAnalysisResult analyzePlan(const PhysicalOperator* plan) {
            PlanAnalysisResult result;
            
            if (!plan) {
                result.recommendations.push_back("Plan is null - cannot analyze");
                return result;
            }

            // Perform comprehensive analysis
            analyzeOperatorTree(plan, result);
            analyzeResourceUsage(plan, result);
            analyzePerformanceCharacteristics(plan, result);
            detectBottlenecks(plan, result);
            generateRecommendations(plan, result);
            
            return result;
        }

        // Analyze specific aspects of the plan
        std::vector<std::string> analyzeJoinOrder(const PhysicalOperator* plan) {
            std::vector<std::string> analysis;
            analyzeJoinOrderRecursive(plan, analysis, 0);
            return analysis;
        }

        std::vector<std::string> analyzeIndexUsage(const PhysicalOperator* plan) {
            std::vector<std::string> analysis;
            analyzeIndexUsageRecursive(plan, analysis);
            return analysis;
        }

        std::vector<std::string> analyzeMemoryPressure(const PhysicalOperator* plan) {
            std::vector<std::string> analysis;
            double totalMemory = calculateTotalMemoryUsage(plan);
            
            if (totalMemory > context.availableMemory * HIGH_MEMORY_THRESHOLD) {
                analysis.push_back("HIGH MEMORY PRESSURE DETECTED:");
                analysis.push_back("  Total estimated memory: " + formatBytes(totalMemory));
                analysis.push_back("  Available memory: " + formatBytes(context.availableMemory));
                analysis.push_back("  Pressure ratio: " + std::to_string(totalMemory / context.availableMemory));
                
                analyzeMemoryPressureRecursive(plan, analysis);
            } else {
                analysis.push_back("Memory usage within acceptable limits");
            }
            
            return analysis;
        }

        // Plan comparison utilities
        double comparePlans(const PhysicalOperator* plan1, const PhysicalOperator* plan2) {
            if (!plan1 || !plan2) return 0.0;
            
            double cost1 = plan1->getTotalCost();
            double cost2 = plan2->getTotalCost();
            
            return (cost1 - cost2) / std::max(cost1, cost2);
        }

        std::string generatePlanReport(const PlanAnalysisResult& result) {
            std::stringstream report;
            
            report << "=== PHYSICAL PLAN ANALYSIS REPORT ===\n\n";
            
            // Summary
            report << "SUMMARY:\n";
            report << "  Total Cost: " << std::fixed << std::setprecision(2) << result.totalCost << "\n";
            report << "  Memory Usage: " << formatBytes(result.totalMemoryUsage) << "\n";
            report << "  Estimated Execution Time: " << result.estimatedExecutionTime << " seconds\n";
            report << "  Total Estimated Rows: " << result.totalEstimatedRows << "\n";
            report << "  Max Parallelism: " << result.maxParallelism << "\n\n";
            
            // Characteristics
            report << "PLAN CHARACTERISTICS:\n";
            report << "  Has Expensive Operations: " << (result.hasExpensiveOperations ? "YES" : "NO") << "\n";
            report << "  Has Memory Intensive Operations: " << (result.hasMemoryIntensiveOperations ? "YES" : "NO") << "\n";
            report << "  Has Parallelizable Operations: " << (result.hasParallelizableOperations ? "YES" : "NO") << "\n\n";
            
            // Cost breakdown
            if (!result.costBreakdown.empty()) {
                report << "COST BREAKDOWN:\n";
                for (const auto& [component, cost] : result.costBreakdown) {
                    double percentage = (cost / result.totalCost) * 100.0;
                    report << "  " << component << ": " << cost << " (" << std::fixed << std::setprecision(1) << percentage << "%)\n";
                }
                report << "\n";
            }
            
            // Bottlenecks
            if (!result.bottlenecks.empty()) {
                report << "PERFORMANCE BOTTLENECKS:\n";
                for (const auto& bottleneck : result.bottlenecks) {
                    report << "  - " << bottleneck << "\n";
                }
                report << "\n";
            }
            
            // Recommendations
            if (!result.recommendations.empty()) {
                report << "RECOMMENDATIONS:\n";
                for (const auto& recommendation : result.recommendations) {
                    report << "  - " << recommendation << "\n";
                }
                report << "\n";
            }
            
            // Detailed operator analysis
            if (!result.operatorAnalysis.empty()) {
                report << "DETAILED OPERATOR ANALYSIS:\n";
                for (size_t i = 0; i < result.operatorAnalysis.size(); ++i) {
                    const auto& op = result.operatorAnalysis[i];
                    report << "  " << (i + 1) << ". " << op.operatorName << " (" << op.operatorType << ")\n";
                    report << "     Cost: " << op.cost << ", Memory: " << formatBytes(op.memoryUsage) << ", Rows: " << op.estimatedRows << "\n";
                    
                    if (!op.issues.empty()) {
                        report << "     Issues: ";
                        for (size_t j = 0; j < op.issues.size(); ++j) {
                            if (j > 0) report << ", ";
                            report << op.issues[j];
                        }
                        report << "\n";
                    }
                    
                    if (!op.suggestions.empty()) {
                        report << "     Suggestions: ";
                        for (size_t j = 0; j < op.suggestions.size(); ++j) {
                            if (j > 0) report << ", ";
                            report << op.suggestions[j];
                        }
                        report << "\n";
                    }
                }
            }
            
            return report.str();
        }

    private:
        void analyzeOperatorTree(const PhysicalOperator* op, PlanAnalysisResult& result) {
            if (!op) return;
            
            // Analyze current operator
            PlanAnalysisResult::OperatorAnalysis opAnalysis;
            opAnalysis.operatorType = getOperatorTypeName(op->type);
            opAnalysis.operatorName = op->toString();
            opAnalysis.cost = op->getTotalCost();
            opAnalysis.memoryUsage = op->estimateMemoryUsage();
            opAnalysis.estimatedRows = op->estimatedRows;
            
            analyzeIndividualOperator(op, opAnalysis);
            result.operatorAnalysis.push_back(opAnalysis);
            
            // Update totals
            result.totalCost += op->getTotalCost();
            result.totalMemoryUsage += op->estimateMemoryUsage();
            result.totalEstimatedRows += op->estimatedRows;
            
            // Update characteristics
            if (op->getTotalCost() > HIGH_COST_THRESHOLD) {
                result.hasExpensiveOperations = true;
            }
            
            if (op->estimateMemoryUsage() > context.availableMemory * 0.2) {
                result.hasMemoryIntensiveOperations = true;
            }
            
            if (op->supportsParallelism()) {
                result.hasParallelizableOperations = true;
                result.maxParallelism = std::max(result.maxParallelism, op->parallelism);
            }
            
            // Recursively analyze children
            for (const auto& child : op->children) {
                analyzeOperatorTree(child.get(), result);
            }
        }

        void analyzeResourceUsage(const PhysicalOperator* op, PlanAnalysisResult& result) {
            if (!op) return;
            
            std::string opType = getOperatorTypeName(op->type);
            result.costBreakdown[opType] += op->getTotalCost();
            result.memoryBreakdown[opType] += op->estimateMemoryUsage();
            
            for (const auto& child : op->children) {
                analyzeResourceUsage(child.get(), result);
            }
        }

        void analyzePerformanceCharacteristics(const PhysicalOperator* op, PlanAnalysisResult& result) {
            // Estimate execution time based on cost and system characteristics
            double cpuTime = result.totalCost * 0.001; // Rough estimate: 1ms per 1000 cost units
            double ioTime = (result.totalMemoryUsage / (1024 * 1024)) * 0.01; // 10ms per MB of I/O
            
            result.estimatedExecutionTime = cpuTime + ioTime;
            
            // Apply parallelism factor
            if (result.hasParallelizableOperations && result.maxParallelism > 1) {
                result.estimatedExecutionTime /= std::min(result.maxParallelism, static_cast<int>(context.maxParallelism));
            }
        }

        void detectBottlenecks(const PhysicalOperator* op, PlanAnalysisResult& result) {
            if (!op) return;
            
            // Check for specific bottleneck patterns
            if (op->type == PhysicalOperatorType::NESTED_LOOP_JOIN && op->estimatedRows > LARGE_TABLE_THRESHOLD) {
                result.bottlenecks.push_back("Large nested loop join detected - consider hash join or sort-merge join");
            }
            
            if (op->type == PhysicalOperatorType::SEQ_SCAN) {
                const auto* seqScan = static_cast<const SeqScanOperator*>(op);
                if (seqScan->estimatedRows > LARGE_TABLE_THRESHOLD && seqScan->filter.empty()) {
                    result.bottlenecks.push_back("Large sequential scan without filter - consider adding indexes");
                }
            }
            
            if (op->estimateMemoryUsage() > context.availableMemory * HIGH_MEMORY_THRESHOLD) {
                result.bottlenecks.push_back("Operator " + op->toString() + " may cause memory pressure");
            }
            
            // Check join order
            if (op->type == PhysicalOperatorType::HASH_JOIN || 
                op->type == PhysicalOperatorType::NESTED_LOOP_JOIN) {
                if (op->children.size() >= 2) {
                    size_t leftRows = op->children[0]->estimatedRows;
                    size_t rightRows = op->children[1]->estimatedRows;
                    
                    if (leftRows > rightRows * 10) {
                        result.bottlenecks.push_back("Potentially suboptimal join order - larger table on outer side");
                    }
                }
            }
            
            for (const auto& child : op->children) {
                detectBottlenecks(child.get(), result);
            }
        }

        void generateRecommendations(const PhysicalOperator* op, PlanAnalysisResult& result) {
            // General recommendations based on analysis
            if (result.totalMemoryUsage > context.availableMemory * HIGH_MEMORY_THRESHOLD) {
                result.recommendations.push_back("Consider increasing available memory or using external algorithms");
            }
            
            if (result.hasExpensiveOperations && !result.hasParallelizableOperations) {
                result.recommendations.push_back("Look for opportunities to parallelize expensive operations");
            }
            
            if (result.totalCost > HIGH_COST_THRESHOLD) {
                result.recommendations.push_back("High total cost detected - review join algorithms and access paths");
            }
            
            // Add specific operator recommendations from detailed analysis
            for (const auto& opAnalysis : result.operatorAnalysis) {
                for (const auto& suggestion : opAnalysis.suggestions) {
                    result.recommendations.push_back(suggestion);
                }
            }
        }

        void analyzeIndividualOperator(const PhysicalOperator* op, PlanAnalysisResult::OperatorAnalysis& analysis) {
            switch (op->type) {
                case PhysicalOperatorType::SEQ_SCAN: {
                    const auto* seqScan = static_cast<const SeqScanOperator*>(op);
                    if (seqScan->estimatedRows > LARGE_TABLE_THRESHOLD) {
                        analysis.issues.push_back("Large table scan");
                        analysis.suggestions.push_back("Consider adding indexes or partitioning");
                    }
                    if (seqScan->filter.empty()) {
                        analysis.suggestions.push_back("Consider pushing down filters");
                    }
                    break;
                }
                
                case PhysicalOperatorType::NESTED_LOOP_JOIN: {
                    if (op->estimatedRows > 10000) {
                        analysis.issues.push_back("Large nested loop join");
                        analysis.suggestions.push_back("Consider hash join or sort-merge join");
                    }
                    break;
                }
                
                case PhysicalOperatorType::HASH_JOIN: {
                    const auto* hashJoin = static_cast<const HashJoinOperator*>(op);
                    if (hashJoin->hashTableSize > context.availableMemory * 0.5) {
                        analysis.issues.push_back("Large hash table");
                        analysis.suggestions.push_back("Consider Grace hash join or sort-merge join");
                    }
                    break;
                }
                
                case PhysicalOperatorType::SORT: {
                    const auto* sortOp = static_cast<const SortOperator*>(op);
                    if (!sortOp->isExternal && sortOp->memoryLimit > context.availableMemory * 0.5) {
                        analysis.issues.push_back("Large in-memory sort");
                        analysis.suggestions.push_back("Consider external sort");
                    }
                    break;
                }
                
                default:
                    break;
            }
        }

        void analyzeJoinOrderRecursive(const PhysicalOperator* op, std::vector<std::string>& analysis, int depth) {
            if (!op) return;
            
            std::string indent(depth * 2, ' ');
            
            if (op->type == PhysicalOperatorType::HASH_JOIN || 
                op->type == PhysicalOperatorType::NESTED_LOOP_JOIN ||
                op->type == PhysicalOperatorType::MERGE_JOIN) {
                
                analysis.push_back(indent + "Join: " + op->toString());
                
                if (op->children.size() >= 2) {
                    size_t leftRows = op->children[0]->estimatedRows;
                    size_t rightRows = op->children[1]->estimatedRows;
                    
                    analysis.push_back(indent + "  Left side: " + std::to_string(leftRows) + " rows");
                    analysis.push_back(indent + "  Right side: " + std::to_string(rightRows) + " rows");
                    
                    if (leftRows > rightRows * 5) {
                        analysis.push_back(indent + "  WARNING: Left side much larger than right side");
                    }
                }
            }
            
            for (const auto& child : op->children) {
                analyzeJoinOrderRecursive(child.get(), analysis, depth + 1);
            }
        }

        void analyzeIndexUsageRecursive(const PhysicalOperator* op, std::vector<std::string>& analysis) {
            if (!op) return;
            
            if (op->type == PhysicalOperatorType::INDEX_SCAN) {
                const auto* indexScan = static_cast<const IndexScanOperator*>(op);
                analysis.push_back("Index scan on " + indexScan->tableName + "." + indexScan->indexName);
                analysis.push_back("  Estimated rows: " + std::to_string(indexScan->estimatedRows));
                analysis.push_back("  Condition: " + indexScan->scanCondition);
            } else if (op->type == PhysicalOperatorType::SEQ_SCAN) {
                const auto* seqScan = static_cast<const SeqScanOperator*>(op);
                analysis.push_back("Sequential scan on " + seqScan->tableName);
                if (seqScan->estimatedRows > LARGE_TABLE_THRESHOLD) {
                    analysis.push_back("  WARNING: Large table scan - consider indexes");
                }
            }
            
            for (const auto& child : op->children) {
                analyzeIndexUsageRecursive(child.get(), analysis);
            }
        }

        void analyzeMemoryPressureRecursive(const PhysicalOperator* op, std::vector<std::string>& analysis) {
            if (!op) return;
            
            double memUsage = op->estimateMemoryUsage();
            if (memUsage > context.availableMemory * 0.1) { // More than 10% of available memory
                double percentage = (memUsage / context.availableMemory) * 100.0;
                analysis.push_back("  " + op->toString() + ": " + formatBytes(memUsage) + 
                                 " (" + std::to_string(percentage) + "%)");
            }
            
            for (const auto& child : op->children) {
                analyzeMemoryPressureRecursive(child.get(), analysis);
            }
        }

        double calculateTotalMemoryUsage(const PhysicalOperator* op) {
            if (!op) return 0.0;
            
            double total = op->estimateMemoryUsage();
            for (const auto& child : op->children) {
                total += calculateTotalMemoryUsage(child.get());
            }
            return total;
        }

        std::string getOperatorTypeName(PhysicalOperatorType type) {
            switch (type) {
                case PhysicalOperatorType::SEQ_SCAN: return "SeqScan";
                case PhysicalOperatorType::INDEX_SCAN: return "IndexScan";
                case PhysicalOperatorType::NESTED_LOOP_JOIN: return "NestedLoopJoin";
                case PhysicalOperatorType::HASH_JOIN: return "HashJoin";
                case PhysicalOperatorType::MERGE_JOIN: return "MergeJoin";
                case PhysicalOperatorType::FILTER: return "Filter";
                case PhysicalOperatorType::PROJECT: return "Project";
                case PhysicalOperatorType::HASH_AGGREGATE: return "HashAggregate";
                case PhysicalOperatorType::SORT_AGGREGATE: return "SortAggregate";
                case PhysicalOperatorType::SORT: return "Sort";
                case PhysicalOperatorType::LIMIT: return "Limit";
                case PhysicalOperatorType::UNION: return "Union";
                case PhysicalOperatorType::INTERSECT: return "Intersect";
                case PhysicalOperatorType::EXCEPT: return "Except";
                case PhysicalOperatorType::MATERIALIZE: return "Materialize";
                case PhysicalOperatorType::EXCHANGE: return "Exchange";
                default: return "Unknown";
            }
        }

        std::string formatBytes(double bytes) {
            const char* units[] = {"B", "KB", "MB", "GB"};
            int unit = 0;
            while (bytes >= 1024 && unit < 3) {
                bytes /= 1024;
                unit++;
            }
            return std::to_string(static_cast<int>(bytes)) + " " + units[unit];
        }
    };

    // Execution simulation results
    struct ExecutionSimulationResult {
        bool success = false;
        std::chrono::microseconds totalExecutionTime{0};
        PhysicalStats aggregatedStats;
        
        // Per-operator execution results
        struct OperatorExecutionResult {
            std::string operatorName;
            std::chrono::microseconds executionTime{0};
            PhysicalStats stats;
            size_t actualRowsProcessed = 0;
            size_t actualRowsReturned = 0;
            bool completed = false;
            std::string errorMessage;
        };
        
        std::vector<OperatorExecutionResult> operatorResults;
        
        // Resource usage tracking
        size_t peakMemoryUsage = 0;
        size_t totalDiskIO = 0;
        size_t totalNetworkIO = 0;
        
        // Performance metrics
        double tuplesPerSecond = 0.0;
        double avgCpuUtilization = 0.0;
        double avgIOUtilization = 0.0;
        
        // Warnings and errors
        std::vector<std::string> warnings;
        std::vector<std::string> errors;
    };

    // Execution Simulator class for simulating plan execution
    class ExecutionSimulator {
    private:
        ExecutionContext& context;
        bool enableDetailedTracking = true;
        bool simulateResourceContention = true;
        
        // Simulation parameters
        double cpuSlowdownFactor = 1.0;
        double ioSlowdownFactor = 1.0;
        double memoryPressureFactor = 1.0;

    public:
        explicit ExecutionSimulator(ExecutionContext& ctx) : context(ctx) {}

        // Configuration methods
        void setDetailedTracking(bool enabled) { enableDetailedTracking = enabled; }
        void setResourceContentionSimulation(bool enabled) { simulateResourceContention = enabled; }
        void setCpuSlowdownFactor(double factor) { cpuSlowdownFactor = factor; }
        void setIoSlowdownFactor(double factor) { ioSlowdownFactor = factor; }
        void setMemoryPressureFactor(double factor) { memoryPressureFactor = factor; }

        // Main simulation method
        ExecutionSimulationResult simulateExecution(PhysicalOperator* plan) {
            ExecutionSimulationResult result;
            
            if (!plan) {
                result.errors.push_back("Cannot simulate null plan");
                return result;
            }

            auto startTime = std::chrono::high_resolution_clock::now();
            
            try {
                // Initialize simulation state
                initializeSimulation(plan, result);
                
                // Simulate plan execution
                result.success = simulateOperatorExecution(plan, result);
                
                // Finalize results
                auto endTime = std::chrono::high_resolution_clock::now();
                result.totalExecutionTime = std::chrono::duration_cast<std::chrono::microseconds>(endTime - startTime);
                
                calculatePerformanceMetrics(result);
                generateSimulationWarnings(result);
                
            } catch (const std::exception& e) {
                result.success = false;
                result.errors.push_back("Simulation error: " + std::string(e.what()));
            }
            
            return result;
        }

        // Simulate specific execution scenarios
        ExecutionSimulationResult simulateWithMemoryPressure(PhysicalOperator* plan, double memoryReductionFactor = 0.5) {
            size_t originalMemory = context.availableMemory;
            context.availableMemory = static_cast<size_t>(originalMemory * memoryReductionFactor);
            
            auto result = simulateExecution(plan);
            result.warnings.push_back("Simulation run with reduced memory: " + 
                                    std::to_string(context.availableMemory) + " bytes");
            
            context.availableMemory = originalMemory;
            return result;
        }

        ExecutionSimulationResult simulateWithHighCpuLoad(PhysicalOperator* plan, double cpuLoadFactor = 2.0) {
            setCpuSlowdownFactor(cpuLoadFactor);
            
            auto result = simulateExecution(plan);
            result.warnings.push_back("Simulation run with high CPU load factor: " + std::to_string(cpuLoadFactor));
            
            setCpuSlowdownFactor(1.0);
            return result;
        }

        ExecutionSimulationResult simulateWithSlowIO(PhysicalOperator* plan, double ioSlowFactor = 3.0) {
            setIoSlowdownFactor(ioSlowFactor);
            
            auto result = simulateExecution(plan);
            result.warnings.push_back("Simulation run with slow I/O factor: " + std::to_string(ioSlowFactor));
            
            setIoSlowdownFactor(1.0);
            return result;
        }

        // Compare execution of different plans
        std::string comparePlansExecution(PhysicalOperator* plan1, PhysicalOperator* plan2) {
            auto result1 = simulateExecution(plan1);
            auto result2 = simulateExecution(plan2);
            
            std::stringstream comparison;
            comparison << "=== PLAN EXECUTION COMPARISON ===\n\n";
            
            comparison << "Plan 1 Results:\n";
            comparison << "  Execution Time: " << result1.totalExecutionTime.count() << " microseconds\n";
            comparison << "  Peak Memory: " << formatBytes(result1.peakMemoryUsage) << "\n";
            comparison << "  Total I/O: " << formatBytes(result1.totalDiskIO) << "\n";
            comparison << "  Success: " << (result1.success ? "YES" : "NO") << "\n\n";
            
            comparison << "Plan 2 Results:\n";
            comparison << "  Execution Time: " << result2.totalExecutionTime.count() << " microseconds\n";
            comparison << "  Peak Memory: " << formatBytes(result2.peakMemoryUsage) << "\n";
            comparison << "  Total I/O: " << formatBytes(result2.totalDiskIO) << "\n";
            comparison << "  Success: " << (result2.success ? "YES" : "NO") << "\n\n";
            
            if (result1.success && result2.success) {
                double timeRatio = static_cast<double>(result1.totalExecutionTime.count()) / result2.totalExecutionTime.count();
                comparison << "Performance Comparison:\n";
                comparison << "  Time Ratio (Plan1/Plan2): " << std::fixed << std::setprecision(2) << timeRatio << "\n";
                
                if (timeRatio < 0.9) {
                    comparison << "  Plan 1 is significantly faster\n";
                } else if (timeRatio > 1.1) {
                    comparison << "  Plan 2 is significantly faster\n";
                } else {
                    comparison << "  Plans have similar performance\n";
                }
            }
            
            return comparison.str();
        }

        // Generate detailed simulation report
        std::string generateSimulationReport(const ExecutionSimulationResult& result) {
            std::stringstream report;
            
            report << "=== EXECUTION SIMULATION REPORT ===\n\n";
            
            // Overall results
            report << "OVERALL RESULTS:\n";
            report << "  Success: " << (result.success ? "YES" : "NO") << "\n";
            report << "  Total Execution Time: " << result.totalExecutionTime.count() << " microseconds\n";
            report << "  Peak Memory Usage: " << formatBytes(result.peakMemoryUsage) << "\n";
            report << "  Total Disk I/O: " << formatBytes(result.totalDiskIO) << "\n";
            report << "  Tuples Per Second: " << std::fixed << std::setprecision(2) << result.tuplesPerSecond << "\n\n";
            
            // Aggregated statistics
            report << "AGGREGATED STATISTICS:\n";
            report << "  Pages Read: " << result.aggregatedStats.pagesRead << "\n";
            report << "  Pages Written: " << result.aggregatedStats.pagesWritten << "\n";
            report << "  Tuples Processed: " << result.aggregatedStats.tuplesProcessed << "\n";
            report << "  Tuples Returned: " << result.aggregatedStats.tuplesReturned << "\n";
            report << "  Memory Used: " << formatBytes(result.aggregatedStats.memoryUsed) << "\n\n";
            
            // Per-operator results
            if (!result.operatorResults.empty()) {
                report << "PER-OPERATOR RESULTS:\n";
                for (size_t i = 0; i < result.operatorResults.size(); ++i) {
                    const auto& opResult = result.operatorResults[i];
                    report << "  " << (i + 1) << ". " << opResult.operatorName << "\n";
                    report << "     Execution Time: " << opResult.executionTime.count() << " microseconds\n";
                    report << "     Rows Processed: " << opResult.actualRowsProcessed << "\n";
                    report << "     Rows Returned: " << opResult.actualRowsReturned << "\n";
                    report << "     Memory Used: " << formatBytes(opResult.stats.memoryUsed) << "\n";
                    report << "     Completed: " << (opResult.completed ? "YES" : "NO") << "\n";
                    
                    if (!opResult.errorMessage.empty()) {
                        report << "     Error: " << opResult.errorMessage << "\n";
                    }
                }
                report << "\n";
            }
            
            // Warnings and errors
            if (!result.warnings.empty()) {
                report << "WARNINGS:\n";
                for (const auto& warning : result.warnings) {
                    report << "  - " << warning << "\n";
                }
                report << "\n";
            }
            
            if (!result.errors.empty()) {
                report << "ERRORS:\n";
                for (const auto& error : result.errors) {
                    report << "  - " << error << "\n";
                }
            }
            
            return report.str();
        }

    private:
        void initializeSimulation(PhysicalOperator* plan, ExecutionSimulationResult& result) {
            // Initialize simulation state
            result.peakMemoryUsage = 0;
            result.totalDiskIO = 0;
            result.totalNetworkIO = 0;
            
            // Pre-allocate operator results
            countOperators(plan, result);
        }

        bool simulateOperatorExecution(PhysicalOperator* op, ExecutionSimulationResult& result) {
            if (!op) return true;
            
            auto opStartTime = std::chrono::high_resolution_clock::now();
            
            ExecutionSimulationResult::OperatorExecutionResult opResult;
            opResult.operatorName = op->toString();
            
            try {
                // Simulate opening the operator
                if (!simulateOperatorOpen(op, opResult)) {
                    opResult.completed = false;
                    result.operatorResults.push_back(opResult);
                    return false;
                }
                
                // Simulate execution of children first
                for (auto& child : op->children) {
                    if (!simulateOperatorExecution(child.get(), result)) {
                        opResult.errorMessage = "Child operator failed";
                        opResult.completed = false;
                        result.operatorResults.push_back(opResult);
                        return false;
                    }
                }
                
                // Simulate main execution
                if (!simulateOperatorGetNext(op, opResult)) {
                    opResult.completed = false;
                    result.operatorResults.push_back(opResult);
                    return false;
                }
                
                // Simulate closing the operator
                simulateOperatorClose(op, opResult);
                
                opResult.completed = true;
                
            } catch (const std::exception& e) {
                opResult.errorMessage = e.what();
                opResult.completed = false;
            }
            
            auto opEndTime = std::chrono::high_resolution_clock::now();
            opResult.executionTime = std::chrono::duration_cast<std::chrono::microseconds>(opEndTime - opStartTime);
            
            // Update aggregated statistics
            result.aggregatedStats += opResult.stats;
            result.peakMemoryUsage = std::max(result.peakMemoryUsage, opResult.stats.memoryUsed);
            result.totalDiskIO += opResult.stats.diskIO;
            
            result.operatorResults.push_back(opResult);
            
            return opResult.completed;
        }

        bool simulateOperatorOpen(PhysicalOperator* op, ExecutionSimulationResult::OperatorExecutionResult& opResult) {
            // Simulate resource allocation and initialization
            size_t memoryNeeded = op->estimateMemoryUsage();
            
            // Check if we have enough memory
            if (memoryNeeded > context.availableMemory) {
                if (op->type == PhysicalOperatorType::HASH_JOIN ||
                    op->type == PhysicalOperatorType::SORT ||
                    op->type == PhysicalOperatorType::HASH_AGGREGATE) {
                    
                    // These operators can spill to disk
                    opResult.stats.diskIO += memoryNeeded; // Simplified: assume full spill
                    memoryNeeded = context.availableMemory / 4; // Use smaller memory footprint
                } else {
                    opResult.errorMessage = "Insufficient memory for operator";
                    return false;
                }
            }
            
            opResult.stats.memoryUsed = memoryNeeded;
            
            // Simulate initialization I/O
            switch (op->type) {
                case PhysicalOperatorType::SEQ_SCAN: {
                    const auto* seqScan = static_cast<const SeqScanOperator*>(op);
                    auto stats = context.getTableStats(seqScan->tableName);
                    if (stats) {
                        size_t pages = (stats->rowCount * stats->avgRowSize + context.pageSize - 1) / context.pageSize;
                        opResult.stats.pagesRead = pages;
                        opResult.stats.diskIO = pages * context.pageSize;
                    }
                    break;
                }
                
                case PhysicalOperatorType::INDEX_SCAN: {
                    // Simulate index tree traversal
                    opResult.stats.pagesRead = 4; // Assume 4-level B-tree
                    opResult.stats.diskIO = 4 * context.pageSize;
                    break;
                }
                
                default:
                    break;
            }
            
            return true;
        }

        bool simulateOperatorGetNext(PhysicalOperator* op, ExecutionSimulationResult::OperatorExecutionResult& opResult) {
            // Simulate tuple processing
            size_t estimatedTuples = op->estimatedRows;
            
            // Apply simulation factors
            auto processingTime = std::chrono::microseconds(
                static_cast<long long>(estimatedTuples * context.cpuTupleCost * 1000 * cpuSlowdownFactor)
            );
            
            // Simulate different operator behaviors
            switch (op->type) {
                case PhysicalOperatorType::SEQ_SCAN: {
                    opResult.actualRowsProcessed = estimatedTuples;
                    opResult.actualRowsReturned = estimatedTuples;
                    
                    // Apply filter selectivity
                    const auto* seqScan = static_cast<const SeqScanOperator*>(op);
                    if (!seqScan->filter.empty()) {
                        opResult.actualRowsReturned = static_cast<size_t>(estimatedTuples * 0.1); // Default 10% selectivity
                    }
                    break;
                }
                
                case PhysicalOperatorType::INDEX_SCAN: {
                    opResult.actualRowsProcessed = estimatedTuples;
                    opResult.actualRowsReturned = estimatedTuples;
                    break;
                }
                
                case PhysicalOperatorType::NESTED_LOOP_JOIN: {
                    if (op->children.size() >= 2) {
                        size_t leftRows = op->children[0]->estimatedRows;
                        size_t rightRows = op->children[1]->estimatedRows;
                        
                        opResult.actualRowsProcessed = leftRows * rightRows;
                        opResult.actualRowsReturned = static_cast<size_t>(leftRows * rightRows * 0.1); // Join selectivity
                        
                        // Nested loop is CPU intensive
                        processingTime *= 2;
                    }
                    break;
                }
                
                case PhysicalOperatorType::HASH_JOIN: {
                    if (op->children.size() >= 2) {
                        size_t leftRows = op->children[0]->estimatedRows;
                        size_t rightRows = op->children[1]->estimatedRows;
                        
                        opResult.actualRowsProcessed = leftRows + rightRows;
                        opResult.actualRowsReturned = static_cast<size_t>(std::min(leftRows, rightRows) * 0.1);
                        
                        // Hash join has build and probe phases
                        const auto* hashJoin = static_cast<const HashJoinOperator*>(op);
                        if (hashJoin->isGraceHashJoin) {
                            // Additional I/O for partitioning
                            opResult.stats.diskIO += (leftRows + rightRows) * 100; // Rough estimate
                            processingTime *= ioSlowdownFactor;
                        }
                    }
                    break;
                }
                
                case PhysicalOperatorType::SORT: {
                    opResult.actualRowsProcessed = estimatedTuples;
                    opResult.actualRowsReturned = estimatedTuples;
                    
                    const auto* sortOp = static_cast<const SortOperator*>(op);
                    if (sortOp->isExternal) {
                        // External sort requires multiple I/O passes
                        opResult.stats.diskIO += estimatedTuples * 200; // Write and read temp files
                        processingTime *= ioSlowdownFactor * 2;
                    }
                    
                    // Sorting is O(n log n)
                    if (estimatedTuples > 0) {
                        processingTime *= static_cast<long long>(std::log2(estimatedTuples));
                    }
                    break;
                }
                
                case PhysicalOperatorType::HASH_AGGREGATE: {
                    opResult.actualRowsProcessed = estimatedTuples;
                    
                    const auto* hashAgg = static_cast<const HashAggregateOperator*>(op);
                    if (hashAgg->groupByColumns.empty()) {
                        opResult.actualRowsReturned = 1; // Single aggregate
                    } else {
                        opResult.actualRowsReturned = std::min(estimatedTuples, estimatedTuples / 10); // Estimated groups
                    }
                    break;
                }
                
                case PhysicalOperatorType::FILTER: {
                    opResult.actualRowsProcessed = estimatedTuples;
                    
                    const auto* filterOp = static_cast<const FilterOperator*>(op);
                    opResult.actualRowsReturned = static_cast<size_t>(estimatedTuples * filterOp->selectivity);
                    break;
                }
                
                case PhysicalOperatorType::LIMIT: {
                    const auto* limitOp = static_cast<const LimitOperator*>(op);
                    opResult.actualRowsProcessed = std::min(estimatedTuples, limitOp->limit + limitOp->offset);
                    opResult.actualRowsReturned = std::min(estimatedTuples, limitOp->limit);
                    
                    // Limit can stop early
                    processingTime = std::chrono::microseconds(
                        static_cast<long long>(opResult.actualRowsProcessed * context.cpuTupleCost * 1000)
                    );
                    break;
                }
                
                default: {
                    opResult.actualRowsProcessed = estimatedTuples;
                    opResult.actualRowsReturned = estimatedTuples;
                    break;
                }
            }
            
            // Update statistics
            opResult.stats.tuplesProcessed = opResult.actualRowsProcessed;
            opResult.stats.tuplesReturned = opResult.actualRowsReturned;
            opResult.stats.executionTime = processingTime;
            
            // Simulate memory pressure effects
            if (simulateResourceContention && opResult.stats.memoryUsed > context.availableMemory * 0.8) {
                processingTime = std::chrono::microseconds(
                    static_cast<long long>(processingTime.count() * memoryPressureFactor)
                );
                opResult.stats.executionTime = processingTime;
            }
            
            return true;
        }

        void simulateOperatorClose(PhysicalOperator* op, ExecutionSimulationResult::OperatorExecutionResult& opResult) {
            // Simulate cleanup operations
            if (op->type == PhysicalOperatorType::SORT && 
                static_cast<const SortOperator*>(op)->isExternal) {
                // Cleanup temporary files
                opResult.stats.diskIO += 1024; // Cleanup overhead
            }
            
            if (op->type == PhysicalOperatorType::HASH_JOIN &&
                static_cast<const HashJoinOperator*>(op)->isGraceHashJoin) {
                // Cleanup partition files
                opResult.stats.diskIO += 2048; // Cleanup overhead
            }
        }

        void countOperators(PhysicalOperator* op, ExecutionSimulationResult& result) {
            if (!op) return;
            
            // Reserve space for this operator
            result.operatorResults.reserve(result.operatorResults.size() + 1);
            
            for (auto& child : op->children) {
                countOperators(child.get(), result);
            }
        }

        void calculatePerformanceMetrics(ExecutionSimulationResult& result) {
            if (result.totalExecutionTime.count() > 0) {
                result.tuplesPerSecond = static_cast<double>(result.aggregatedStats.tuplesReturned) /
                                       (static_cast<double>(result.totalExecutionTime.count()) / 1000000.0);
            }
            
            // Calculate average utilization (simplified)
            result.avgCpuUtilization = std::min(1.0, cpuSlowdownFactor * 0.7);
            result.avgIOUtilization = std::min(1.0, ioSlowdownFactor * 0.5);
        }

        void generateSimulationWarnings(ExecutionSimulationResult& result) {
            // Check for performance issues
            if (result.peakMemoryUsage > context.availableMemory * 0.9) {
                result.warnings.push_back("Peak memory usage exceeded 90% of available memory");
            }
            
            if (result.totalDiskIO > 100 * 1024 * 1024) { // 100MB
                result.warnings.push_back("High disk I/O detected: " + formatBytes(result.totalDiskIO));
            }
            
            // Check for operators that didn't complete
            size_t incompleteOps = 0;
            for (const auto& opResult : result.operatorResults) {
                if (!opResult.completed) {
                    incompleteOps++;
                }
            }
            
            if (incompleteOps > 0) {
                result.warnings.push_back(std::to_string(incompleteOps) + " operators did not complete successfully");
            }
            
            // Check for very slow operations
            for (const auto& opResult : result.operatorResults) {
                if (opResult.executionTime.count() > 1000000) { // More than 1 second
                    result.warnings.push_back("Slow operator detected: " + opResult.operatorName + 
                                            " took " + std::to_string(opResult.executionTime.count()) + " microseconds");
                }
            }
        }

        std::string formatBytes(size_t bytes) {
            const char* units[] = {"B", "KB", "MB", "GB"};
            int unit = 0;
            double size = static_cast<double>(bytes);
            
            while (size >= 1024 && unit < 3) {
                size /= 1024;
                unit++;
            }
            
            return std::to_string(static_cast<int>(size)) + " " + units[unit];
        }
    };

} // namespace physical_planner