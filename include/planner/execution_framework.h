#pragma once

#include <memory>
#include <vector>
#include <string>
#include <unordered_map>
#include <fstream>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <atomic>
#include <queue>
#include <unordered_set>

// Include your existing headers
#include "physical_planner.h"

namespace physical_planner {

    // Forward declarations for real execution components
    class StorageManager;
    class BufferPool;
    class TransactionManager;
    class LockManager;
    class LogManager;

    // ==== 1. STORAGE LAYER ====

    // Page structure for disk storage
    struct Page {
        static constexpr size_t PAGE_SIZE = 8192; // 8KB pages
        static constexpr size_t HEADER_SIZE = 64;
        static constexpr size_t DATA_SIZE = PAGE_SIZE - HEADER_SIZE;
        
        struct Header {
            uint32_t pageId;
            uint32_t pageType;
            uint32_t freeSpace;
            uint32_t tupleCount;
            uint32_t nextPage;
            uint32_t prevPage;
            uint64_t lsn; // Log sequence number
            uint32_t checksum;
            char reserved[24];
        } header;
        
        char data[DATA_SIZE];
        
        Page() : header{}, data{} {
        }
    };

    // Tuple structure
    struct Tuple {
        struct Header {
            uint16_t size;
            uint16_t nullBitmap;
            uint32_t xmin; // Transaction that inserted
            uint32_t xmax; // Transaction that deleted
        } header;
        
        std::vector<char> data;
        
        Tuple() : header{} {}
        explicit Tuple(const std::vector<std::string>& values);
        [[nodiscard]] std::vector<std::string> getValues() const;
    };

    // Buffer pool for page management
    class BufferPool {
    private:
        struct BufferFrame {
            Page page;
            uint32_t pageId;
            bool isDirty;
            bool isPinned;
            uint32_t pinCount;
            std::chrono::time_point<std::chrono::steady_clock> lastAccess;
            std::mutex frameMutex;
        };
        
        std::vector<std::unique_ptr<BufferFrame>> frames;
        std::unordered_map<uint32_t, size_t> pageTable; // pageId -> frame index
        std::queue<size_t> freeFrames;
        std::mutex poolMutex;
        
        size_t poolSize;
        StorageManager* storageManager;
        
    public:
        explicit BufferPool(size_t size, StorageManager* storage);
        ~BufferPool();
        
        // Page management
        Page* getPage(uint32_t pageId);
        void unpinPage(uint32_t pageId, bool isDirty = false);
        void flushPage(uint32_t pageId);
        void flushAll();
        
        // Statistics
        [[nodiscard]] size_t getHitRate() const;
        [[nodiscard]] size_t getUsedFrames() const;
        
    private:
        size_t evictPage();
        void loadPage(uint32_t pageId, size_t frameIndex);
    };

    // Storage manager for file I/O
    class StorageManager {
    private:
        std::string dataDirectory;
        std::unordered_map<std::string, std::unique_ptr<std::fstream>> tableFiles;
        std::mutex fileMutex;
        
    public:
        explicit StorageManager(const std::string& dataDir);
        ~StorageManager();
        
        // File operations
        bool createTable(const std::string& tableName, const std::vector<logical_planner::ColumnInfo>& schema);
        bool dropTable(const std::string& tableName);
        
        // Page I/O
        bool readPage(const std::string& tableName, uint32_t pageId, Page& page);
        bool writePage(const std::string& tableName, uint32_t pageId, const Page& page);
        uint32_t allocatePage(const std::string& tableName);
        
        // Table metadata
        std::vector<logical_planner::ColumnInfo> getTableSchema(const std::string& tableName);
        logical_planner::TableStats getTableStats(const std::string& tableName);
        
    private:
        std::string getTableFilePath(const std::string& tableName);
        bool ensureFileExists(const std::string& tableName);
    };

    // ==== 2. EXECUTION ENGINE ====

    // Execution context with real resources
    class RealExecutionContext : public ExecutionContext {
    private:
        std::unique_ptr<BufferPool> bufferPool;
        std::unique_ptr<StorageManager> storageManager;
        std::unique_ptr<TransactionManager> transactionManager;
        
    public:
        RealExecutionContext(const std::string& dataDirectory, size_t bufferPoolSize);
        ~RealExecutionContext();
        
        // Resource access
        [[nodiscard]] BufferPool* getBufferPool() const { return bufferPool.get(); }
        [[nodiscard]] StorageManager* getStorageManager() const { return storageManager.get(); }
        [[nodiscard]] TransactionManager* getTransactionManager() const { return transactionManager.get(); }
        
        // Transaction support
        uint32_t beginTransaction();
        bool commitTransaction(uint32_t txnId);
        bool abortTransaction(uint32_t txnId);
        
        // Statistics collection
        void updateOperatorStats(const std::string& operatorName, const PhysicalStats& stats);
        [[nodiscard]] PhysicalStats getOperatorStats(const std::string& operatorName) const;
    };

    // Real iterator interface for operators
    class TupleIterator {
    public:
        virtual ~TupleIterator() = default;
        virtual bool hasNext() = 0;
        virtual std::unique_ptr<Tuple> next() = 0;
        virtual void reset() = 0;
        [[nodiscard]] virtual size_t getEstimatedCount() const = 0;
    };

    // ==== 3. REAL OPERATOR IMPLEMENTATIONS ====

    // Real sequential scan implementation
    class RealSeqScanOperator : public SeqScanOperator {
    private:
        RealExecutionContext* realContext;
        uint32_t currentPageId;
        size_t currentTupleIndex;
        std::unique_ptr<Page> currentPage;
        bool isOpen;
        std::vector<logical_planner::ColumnInfo> tableSchema;
        
    public:
        RealSeqScanOperator(const std::string& table, RealExecutionContext* ctx);
        
        // Override execution methods
        bool open(ExecutionContext& context) override;
        bool getNext(ExecutionContext& context, std::vector<std::string>& tuple) override;
        void close(ExecutionContext& context) override;
        
        // Iterator interface
        std::unique_ptr<TupleIterator> getIterator();
        
    private:
        bool loadNextPage();
        std::unique_ptr<Tuple> getTupleFromPage(size_t tupleIndex);
        bool matchesFilter(const Tuple& tuple);
    };

    // Real index scan implementation
    class RealIndexScanOperator : public IndexScanOperator {
    private:
        RealExecutionContext* realContext;
        // B+ tree traversal state
        std::vector<uint32_t> pathToLeaf;
        uint32_t currentLeafPage;
        size_t currentKeyIndex;
        bool isOpen;
        
    public:
        RealIndexScanOperator(const std::string& table, const std::string& index, RealExecutionContext* ctx);
        
        bool open(ExecutionContext& context) override;
        bool getNext(ExecutionContext& context, std::vector<std::string>& tuple) override;
        void close(ExecutionContext& context) override;
        
    private:
        bool traverseToLeaf();
        bool findNextMatchingKey();
    };

    // Real hash join implementation
    class RealHashJoinOperator final : public HashJoinOperator {
    private:
        struct HashEntry {
            std::vector<std::string> tuple;
            std::unique_ptr<HashEntry> next; // For collision chaining
        };
        
        RealExecutionContext* realContext;
        std::vector<std::unique_ptr<HashEntry>> hashTable;
        std::unique_ptr<TupleIterator> leftIterator;
        std::unique_ptr<TupleIterator> rightIterator;
        bool buildPhaseComplete;
        bool isOpen;
        
        // For grace hash join
        std::vector<std::unique_ptr<std::fstream>> partitionFiles;
        bool isGraceJoin;
        
    public:
        RealHashJoinOperator(logical_planner::JoinType type, const std::string& condition, RealExecutionContext* ctx);
        
        bool open(ExecutionContext& context) override;
        bool getNext(ExecutionContext& context, std::vector<std::string>& tuple) override;
        void close(ExecutionContext& context) override;
        
    private:
        bool buildHashTable();
        bool probeHashTable(std::vector<std::string>& result);
        size_t hashFunction(const std::string& key);
        bool performGraceHashJoin();
        void partitionRelations();
        bool joinPartitions();
    };

    // ==== 4. EXECUTION ENGINE ====

    class ExecutionEngine {
    private:
        std::unique_ptr<RealExecutionContext> context;
        std::atomic<bool> isRunning;
        std::mutex executionMutex;
        
        // Parallel execution support
        struct WorkerThread {
            std::unique_ptr<std::thread> thread;
            std::queue<PhysicalOperator*> workQueue;
            std::mutex queueMutex;
            std::condition_variable workAvailable;
            std::atomic<bool> isActive;
        };
        
        std::vector<std::unique_ptr<WorkerThread>> workers;
        
    public:
        explicit ExecutionEngine(const std::string& dataDirectory, size_t bufferPoolSize = 64 * 1024 * 1024);
        ~ExecutionEngine();
        
        // Main execution interface
        class ResultSet {
        public:
            virtual ~ResultSet() = default;
            virtual bool hasNext() = 0;
            virtual std::vector<std::string> next() = 0;
            [[nodiscard]] virtual std::vector<logical_planner::ColumnInfo> getSchema() const = 0;
            [[nodiscard]] virtual size_t getRowCount() const = 0;
        };
        
        std::unique_ptr<ResultSet> execute(std::unique_ptr<PhysicalOperator> plan);
        
        // Parallel execution
        std::unique_ptr<ResultSet> executeParallel(std::unique_ptr<PhysicalOperator> plan, int parallelism = 4);
        
        // Utility methods
        void loadTableData(const std::string& tableName, const std::vector<std::vector<std::string>>& data);
        void createTable(const std::string& tableName, const std::vector<logical_planner::ColumnInfo>& schema);
        void dropTable(const std::string& tableName);
        
        // Performance monitoring
        PhysicalStats getExecutionStats() const;
        std::string getExecutionReport() const;
        
    private:
        std::unique_ptr<PhysicalOperator> convertToRealOperators(std::unique_ptr<PhysicalOperator> plan);
        void initializeWorkerThreads(int numThreads);
        void shutdownWorkerThreads();
        void workerThreadMain(WorkerThread* worker);
    };

    // Result set implementation
    class OperatorResultSet final : public ExecutionEngine::ResultSet {
    private:
        std::unique_ptr<PhysicalOperator> rootOperator;
        RealExecutionContext* context;
        bool isOpen;
        size_t rowCount;
        
    public:
        OperatorResultSet(std::unique_ptr<PhysicalOperator> op, RealExecutionContext* ctx);
        ~OperatorResultSet() override;
        
        bool hasNext() override;
        std::vector<std::string> next() override;
        [[nodiscard]] std::vector<logical_planner::ColumnInfo> getSchema() const override;
        [[nodiscard]] size_t getRowCount() const override;
    };

    // ==== 5. TRANSACTION SUPPORT ====

    class TransactionManager {
    private:
        struct Transaction {
            uint32_t txnId;
            std::chrono::time_point<std::chrono::steady_clock> startTime;
            std::unordered_set<uint32_t> readPages;
            std::unordered_set<uint32_t> writtenPages;
            bool isActive;
        };
        
        std::unordered_map<uint32_t, std::unique_ptr<Transaction>> transactions;
        std::atomic<uint32_t> nextTxnId;
        std::mutex txnMutex;
        
        std::unique_ptr<LockManager> lockManager;
        std::unique_ptr<LogManager> logManager;
        
    public:
        TransactionManager();
        ~TransactionManager();
        
        uint32_t beginTransaction();
        bool commitTransaction(uint32_t txnId);
        bool abortTransaction(uint32_t txnId);
        
        // Concurrency control
        bool acquireReadLock(uint32_t txnId, uint32_t pageId);
        bool acquireWriteLock(uint32_t txnId, uint32_t pageId);
        void releaseLocks(uint32_t txnId);
        
        // Recovery support
        void writeLogRecord(uint32_t txnId, const std::string& operation, const std::string& data);
        bool recover();
    };

    // ==== 6. IMPLEMENTATION EXAMPLES ====

    // Example: Real sequential scan implementation
    bool RealSeqScanOperator::open(ExecutionContext& context) {
        realContext = static_cast<RealExecutionContext*>(&context);
        
        // Get table schema
        tableSchema = realContext->getStorageManager()->getTableSchema(tableName);
        if (tableSchema.empty()) {
            return false;
        }
        
        // Initialize scan state
        currentPageId = 0; // Start from first page
        currentTupleIndex = 0;
        isOpen = true;
        
        // Load first page
        return loadNextPage();
    }

    bool RealSeqScanOperator::getNext(ExecutionContext& context, std::vector<std::string>& tuple) {
        if (!isOpen || !currentPage) {
            return false;
        }
        
        while (true) {
            // Try to get tuple from current page
            auto tuplePtr = getTupleFromPage(currentTupleIndex);
            if (tuplePtr) {
                // Check if tuple matches filter
                if (filter.empty() || matchesFilter(*tuplePtr)) {
                    tuple = tuplePtr->getValues();
                    
                    // Apply projection if specified
                    if (!projectedColumns.empty() && projectedColumns[0] != "*") {
                        std::vector<std::string> projectedTuple;
                        for (const auto& col : projectedColumns) {
                            // Find column index and extract value
                            // This is simplified - real implementation would use column metadata
                            for (size_t i = 0; i < tableSchema.size() && i < tuple.size(); ++i) {
                                if (tableSchema[i].columnName == col) {
                                    projectedTuple.push_back(tuple[i]);
                                    break;
                                }
                            }
                        }
                        tuple = projectedTuple;
                    }
                    
                    currentTupleIndex++;
                    actualStats.tuplesReturned++;
                    return true;
                }
                currentTupleIndex++;
            } else {
                // No more tuples in current page, try next page
                if (!loadNextPage()) {
                    return false; // No more pages
                }
            }
        }
    }

    void RealSeqScanOperator::close(ExecutionContext& context) {
        if (currentPage) {
            realContext->getBufferPool()->unpinPage(currentPageId);
            currentPage.reset();
        }
        isOpen = false;
    }

    bool RealSeqScanOperator::loadNextPage() {
        // Unpin current page if any
        if (currentPage) {
            realContext->getBufferPool()->unpinPage(currentPageId);
            currentPage.reset();
        }
        
        // Try to load next page
        currentPageId++;
        Page* page = realContext->getBufferPool()->getPage(currentPageId);
        if (!page || page->header.tupleCount == 0) {
            return false; // No more pages or empty page
        }
        
        currentPage = std::make_unique<Page>(*page);
        currentTupleIndex = 0;
        actualStats.pagesRead++;
        
        return true;
    }

    // ==== 7. USAGE EXAMPLE ====

    /*
    // Example usage:
    int main() {
        // Create execution engine
        ExecutionEngine engine("./data", 128 * 1024 * 1024); // 128MB buffer pool
        
        // Create table
        std::vector<logical_planner::ColumnInfo> schema = {
            {"id", logical_planner::DataType::INT, false},
            {"name", logical_planner::DataType::VARCHAR, false},
            {"age", logical_planner::DataType::INT, true}
        };
        engine.createTable("users", schema);
        
        // Load data
        std::vector<std::vector<std::string>> data = {
            {"1", "Alice", "25"},
            {"2", "Bob", "30"},
            {"3", "Charlie", "35"}
        };
        engine.loadTableData("users", data);
        
        // Create and execute plan
        auto seqScan = std::make_unique<SeqScanOperator>("users");
        auto filter = std::make_unique<FilterOperator>("age > 25");
        filter->addChild(std::move(seqScan));
        
        auto results = engine.execute(std::move(filter));
        
        // Process results
        while (results->hasNext()) {
            auto row = results->next();
            for (const auto& value : row) {
                std::cout << value << " ";
            }
            std::cout << std::endl;
        }
        
        return 0;
    }
    */

} // namespace physical_planner