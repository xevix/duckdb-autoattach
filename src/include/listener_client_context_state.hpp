#pragma once

#include "duckdb.hpp"
#include <efsw/efsw.hpp>

class ListenerClientContextState : public duckdb::ClientContextState {
public:
	ListenerClientContextState(duckdb::ClientContext *context)
	    : fileWatcher(new efsw::FileWatcher()), context(context) {
	}
	efsw::FileWatcher *getFileWatcher();

	std::string get_current_db_path(const std::string &db_alias);

	void attach_or_replace(const std::string &db_alias, const std::string &new_db_path);

	void attach(const std::string &db_alias, const std::string &new_db_path, bool lock);

	std::string getLatestFileAtPath(const std::string &path);

	std::string getLatestAtRemotePath(const std::string &path);

	void addLocalWatch(const std::string &path, const std::string &alias);

	// SELECT attach_auto('s3_db', 's3://test-bucket/presigned/attach*.db')
	void addRemoteWatch(const std::string &path, const std::string &alias);

	void addWatch(const std::string &path, const std::string &alias);

private:
	efsw::FileWatcher *fileWatcher = nullptr;
	duckdb::ClientContext *context = nullptr;
};
