#pragma once

#include "duckdb.hpp"
#include <efsw/efsw.hpp>
#include "s3_watcher.hpp"

class ListenerClientContextState : public duckdb::ClientContextState {
public:
	ListenerClientContextState(duckdb::ClientContext *context)
	    : fileWatcher(new efsw::FileWatcher()), context(context) {
	}
	efsw::FileWatcher *getFileWatcher();

	std::string get_current_db_path(const std::string &db_alias);

	void attach_or_replace(const std::string &db_alias, const std::string &new_db_path);

	void attach(const std::string &db_alias, const std::string &new_db_path, bool lock);

	void attach_latest_remote_file(const std::string &path, const std::string &alias, bool lock);

	void detach(const std::string &alias);

	std::string getLatestFileAtPath(const std::string &path);

	std::string getLatestAtRemotePath(const std::string &path);

	void addLocalWatch(const std::string &path, const std::string &alias);

	uint64_t S3PollInterval();

	void addRemoteWatch(const std::string &path, const std::string &alias);

	void addWatch(const std::string &path, const std::string &alias);

	void removeWatch(const std::string &alias);

private:
	efsw::FileWatcher *fileWatcher = nullptr;
	std::unordered_map<std::string, efsw::WatchID> fileWatchIds;
	duckdb::ClientContext *context = nullptr;
	std::unordered_map<std::string, std::unique_ptr<S3Watcher>> s3Watchers;
};
