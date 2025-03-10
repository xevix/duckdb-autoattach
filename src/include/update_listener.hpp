#pragma once

#include "duckdb.hpp"
#include <efsw/efsw.hpp>

class UpdateListener : public efsw::FileWatchListener {
private:
	duckdb::ClientContext *context;
	// duckdb::shared_ptr<duckdb::DuckDB> db;
	std::string current_attached_file;
	std::string db_alias;

public:
	UpdateListener(duckdb::ClientContext *context, const std::string &alias) : context(context), db_alias(alias) {
	}

	void attach(const std::string &new_db_path);

	void handleFileAction(efsw::WatchID watchid, const std::string &dir, const std::string &filename,
	                      efsw::Action action, std::string oldFilename) override;
};
