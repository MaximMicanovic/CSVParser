#include <filesystem>
class CsvParser{
public:
	CsvParser(const std::filesystem::path &path);
	~CsvParser();
	std::string get(int col, int row);
	int colLen();
	int rowLen();
private:
	struct Impl;
	std::unique_ptr<Impl> impl;
};
