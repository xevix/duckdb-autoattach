#pragma once

#include "duckdb.hpp"
#include "efsw/efsw.hpp"
namespace duckdb {

class AutoattachExtension : public Extension {
public:
    void Load(DuckDB &db) override;
    std::string Name() override;
    std::string Version() const override;
	void addWatch(const std::string &path, const std::string &alias);
	void Unload();

private:
    efsw::FileWatcher* fileWatcher;
    DuckDB* db_instance;
}; 

} // namespace duckdb
