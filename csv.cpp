#include <condition_variable>
#include <filesystem>
#include <fstream>
#include <memory>
#include <mutex>
#include <string_view>
#include <thread>
#include <vector>
#include <algorithm>

#include "csv.h"


typedef struct {
  int index;
  int amountOfLines;
  std::vector<std::string> order;
} Order;

typedef struct {
  Order storedOrder;
  std::vector<std::vector<std::string_view>> column;
} CompletedOrder;

typedef struct{
	int chunk;
	int localIndex;
} RowIndex;

class ParserThreadHandler {
private:
  int m_sizeOfOrder = 8 * 1024 * 1024;
  int m_columnAmount;
  std::vector<Order> m_orders;

  std::vector<std::thread> m_workers;
  std::condition_variable m_orderCV;
  std::mutex m_orderMutex;
  std::vector<int> m_headerPositions;
  bool m_workDone = false;

  std::mutex m_completedOrderMutex;
  std::vector<CompletedOrder> m_completedOrders;
  std::condition_variable m_completedCV;

  void workerThread();
  void mainThread(std::filesystem::path path);
	CompletedOrder work(Order &localOrder);
	void queueOrder(int &index, int &orderLines, std::vector<std::string> &order) {
    Order new_order;
    std::swap(new_order.order, order);
    new_order.index = index;
    new_order.amountOfLines = orderLines;
    index++;

    {
      std::lock_guard<std::mutex> lock(m_orderMutex);
      m_orders.emplace_back(std::move(new_order));
    }
    order.clear();
    m_orderCV.notify_one();
  }
  
	void endThreads() {
    {
      std::unique_lock<std::mutex> lock(m_orderMutex);
      m_completedCV.wait(lock, [this] { return m_orders.empty(); });
      m_workDone = true;
    }
    m_orderCV.notify_all();

    for (auto &t : m_workers)
      t.join();
  }
  
	void countColumns(const std::string &line) {
    m_columnAmount = 0;
    for (const char &c : line) {
      if (c == ',')
        m_columnAmount++;
    }
  }

  std::vector<std::vector<std::string_view>>
  prepareCompletedOrderVector(Order &localOrder) {
    std::vector<std::vector<std::string_view>> preparedVector;
    preparedVector.resize(m_columnAmount);
		for(auto &prep: preparedVector)
			prep.resize(localOrder.amountOfLines);
    return preparedVector;
  }

	void organizeCompletedOrders(){
		std::sort(m_completedOrders.begin(), m_completedOrders.end(), 
						[](const CompletedOrder &a, const CompletedOrder &b)
						{ return a.storedOrder.index < b.storedOrder.index; });

		int rows = 0;
		for(auto &completed: m_completedOrders)
			rows += completed.storedOrder.amountOfLines;

		m_rowIndexTable.resize(rows);
		int lineSum = 0;
		for(auto &completed: m_completedOrders){
			int lines = completed.storedOrder.amountOfLines;
			for(int i = 0; i < lines; i++){
				RowIndex rowIndex;
				rowIndex.chunk = completed.storedOrder.index;
				rowIndex.localIndex = i;

				m_rowIndexTable[i+lineSum] = std::move(rowIndex);
			}
			lineSum += lines;
		}
	}

	std::vector<RowIndex> m_rowIndexTable;
public:
  ParserThreadHandler(std::filesystem::path path);
	std::string get(int col, int row){
		auto &index = m_rowIndexTable[row];
		return std::string(m_completedOrders[index.chunk].column[col][index.localIndex]);
	}

	int rowLen(){
		return m_columnAmount;
	}
	int colLen(){
		return m_rowIndexTable.size();
	}
};

CompletedOrder ParserThreadHandler::work(Order &localOrder) {
  CompletedOrder completed;
  completed.storedOrder = std::move(localOrder);
  completed.column = prepareCompletedOrderVector(completed.storedOrder);
  int row = 0;

  for (std::string &line : completed.storedOrder.order) {
    const char *startingPosition = line.data();
    int column = 0;

    for (const char &c : line) {
      if (c != ',') continue;

      completed.column[column][row] = std::string_view(startingPosition, (&c - startingPosition));
      startingPosition = &c + 1;
      column++;
    }
    row++;
  }
  return completed;
}

ParserThreadHandler::ParserThreadHandler(std::filesystem::path path) {
  unsigned int cores = std::thread::hardware_concurrency();
  for (int i = 0; i < cores; i++)
    m_workers.emplace_back(
        std::thread(&ParserThreadHandler::workerThread, this));
  mainThread(path);
	organizeCompletedOrders();
}

void ParserThreadHandler::mainThread(std::filesystem::path path) {
  std::ifstream stream(path);
  std::string line;

  std::vector<std::string> order;

  int orderSize = 0;
  int orderLines = 0;
  int index = 0;

  bool firstTime = true;

  while (std::getline(stream, line)) {
    line.push_back(',');
    orderSize += line.size();
    orderLines++;

    if (firstTime) {
      firstTime = false;
      countColumns(line);
    }

    std::string newLine;
    std::swap(newLine, line);
    line.reserve(newLine.size());

    order.emplace_back(std::move(newLine));

    if (orderSize >= m_sizeOfOrder) {
      queueOrder(index, orderLines, order);
      orderSize = 0;
      orderLines = 0;
    }
  }
  queueOrder(index, orderLines, order);
  endThreads();
}

void ParserThreadHandler::workerThread() {
  Order localOrder;
  while (true) {
    {
      std::unique_lock<std::mutex> lock(m_orderMutex);
      m_orderCV.wait(lock, [this] { return !m_orders.empty() or m_workDone; });

      if (m_workDone)
        return;

      localOrder = std::move(m_orders.back());
      m_orders.pop_back();
    }
    CompletedOrder completed = work(localOrder);
    {
      std::lock_guard<std::mutex> lock(m_completedOrderMutex);
      m_completedOrders.emplace_back(std::move(completed));
    }
    m_completedCV.notify_one();
  }
}

// This is for the helper class that implements the CsvParser
CsvParser::CsvParser(const std::filesystem::path &path): impl(std::make_unique<Impl>(path)) {}
CsvParser::~CsvParser() = default;

struct CsvParser::Impl{
	ParserThreadHandler parser;
	Impl(const std::filesystem::path &path) : parser(path){}
};

std::string CsvParser::get(int col, int row){	return impl->parser.get(col, row); }
int CsvParser::rowLen(){ return impl->parser.rowLen(); }
int CsvParser::colLen(){ return impl->parser.colLen(); }
