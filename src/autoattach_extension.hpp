#pragma once

#include "duckdb.hpp"
#include "efsw/efsw.hpp"
namespace duckdb {

class AutoattachExtension : public Extension {
public:
    void Load(DuckDB &db) override;
    std::string Name() override;
    std::string Version() const override;
}; 

} // namespace duckdb
