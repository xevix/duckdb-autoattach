#include <boost/filesystem.hpp>
#include <iostream>

#include "listener_client_context_state.hpp"
#include "update_listener.hpp"


	efsw::FileWatcher *ListenerClientContextState::getFileWatcher() {
		return fileWatcher;
	}

    std::string ListenerClientContextState::get_current_db_path(const std::string &db_alias) {
	duckdb::Connection con(*context->db);
	auto watched_db = context->db->GetDatabaseManager().GetDatabase(*context, db_alias);
	if (watched_db) {
		return duckdb::Catalog::GetCatalog(*watched_db).GetDBPath();
	}
	return "";
}

void ListenerClientContextState::attach_or_replace(const std::string &db_alias, const std::string &new_db_path) {
	// Run a Query() to fetch the current attached file
	std::cerr << "getting path" << std::endl;
	auto current_db_path = get_current_db_path(db_alias);
	std::cerr << "path: " << current_db_path << std::endl;
	std::cerr << "new path: " << new_db_path << std::endl;
	// Check if the filename is lexicographically greater than the current attached file
	if (new_db_path > current_db_path) {
		duckdb::Connection con(*context->db);
		auto query = "ATTACH OR REPLACE '" + new_db_path + "' AS " + db_alias;
		std::cerr << "Attaching " << query << std::endl;
		auto result = con.Query(query);
		if (result->HasError()) {
			std::cerr << result->GetError() << std::endl;
		} else {
			std::cerr << result->ToString() << std::endl;
		}
	}
}

void ListenerClientContextState::attach(const std::string &db_alias, const std::string &new_db_path, bool lock) {
	std::cerr << "attach" << std::endl;
	if (lock) {
		context->RunFunctionInTransaction([&]() { attach_or_replace(db_alias, new_db_path); });
	} else {
		attach_or_replace(db_alias, new_db_path);
	}
}

	std::string ListenerClientContextState::getLatestFileAtPath(const std::string &path) {
		// Get all files in the directory using Boost filesystem
		std::string latest_file = "";
		boost::filesystem::path dir_path(path);

		if (boost::filesystem::exists(dir_path) && boost::filesystem::is_directory(dir_path)) {
			for (const auto &entry : boost::filesystem::directory_iterator(dir_path)) {
				if (boost::filesystem::is_regular_file(entry)) {
					auto latest_file_candidate = entry.path().filename().string();
					if (entry.path().extension() == ".duckdb" && latest_file_candidate > latest_file) {
						latest_file = latest_file_candidate;
					}
				}
			}
		}

		return latest_file;
	}

    void ListenerClientContextState::addLocalWatch(const std::string &path, const std::string &alias) {
		auto listener = new UpdateListener(context, alias, this);
		// TODO: bootstrap the listener with the first file to attach
		std::cerr << "Adding watch to: " << path << std::endl;
		attach(alias, path + "/" + getLatestFileAtPath(path), false);
		fileWatcher->addWatch(path, listener, false);
		// Start watching asynchronously the directories
		fileWatcher->watch();
		std::cerr << "Watching directories: " << fileWatcher->directories().size() << std::endl;
    }

    // SELECT attach_auto('s3_db', 's3://test-bucket/presigned/attach*.db')
    void ListenerClientContextState::addRemoteWatch(const std::string &path, const std::string &alias) {
        // TODO: implement
        duckdb::Connection con(*context->db);
		auto result = con.Query("SELECT filename FROM read_blob(CAST(? AS text)) ORDER BY filename ASC LIMIT 1", path);
		if (result->HasError()) {
			std::cerr << "Error: " << result->GetError() << std::endl;
		} else {
            for (const auto &row : *result) {
                std::cerr << "Row: " << row.GetValue<std::string>(0) << std::endl;
				attach(alias, row.GetValue<std::string>(0), false);
            }
        }
    }

	void ListenerClientContextState::addWatch(const std::string &path, const std::string &alias) {
        if (duckdb::FileSystem::IsRemoteFile(path)) {
            std::cerr << "Adding remote watch to: " << path << std::endl;
            addRemoteWatch(path, alias);
        } else {
            std::cerr << "Adding local watch to: " << path << std::endl;
            addLocalWatch(path, alias);
        }
	}
