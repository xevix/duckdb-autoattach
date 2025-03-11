#include <efsw/efsw.hpp>
#include <iostream>

#include "duckdb.hpp"
#include "duckdb/main/attached_database.hpp"
#include "update_listener.hpp"

std::string UpdateListener::get_current_db_path() {
	duckdb::Connection con(*context->db);
	auto watched_db = context->db->GetDatabaseManager().GetDatabase(*context, db_alias);
	if (watched_db) {
		return duckdb::Catalog::GetCatalog(*watched_db).GetDBPath();
	}
	return "";
}

void UpdateListener::attach_or_replace(const std::string &new_db_path) {
	// Run a Query() to fetch the current attached file
	std::cerr << "getting path" << std::endl;
	auto current_db_path = get_current_db_path();
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

void UpdateListener::attach(const std::string &new_db_path, bool lock) {
	std::cerr << "attach" << std::endl;
	if (lock) {
		context->RunFunctionInTransaction([&]() { attach_or_replace(new_db_path); });
	} else {
		attach_or_replace(new_db_path);
	}
}

void UpdateListener::handleFileAction(efsw::WatchID watchid, const std::string &dir, const std::string &filename,
                                      efsw::Action action, std::string oldFilename) {

	switch (action) {
	case efsw::Actions::Add:
		std::cerr << "DIR (" << dir << ") FILE (" << filename << ") has event Added" << std::endl;
		if (filename.find(".duckdb") != std::string::npos && filename.substr(filename.length() - 7) == ".duckdb") {
			attach(dir + filename, true);
		}
		break;
	case efsw::Actions::Delete:
		// TODO: detach if currently attached, and try to attach now-latest file with the pattern. If no files found,
		// throw an error.
		std::cerr << "DIR (" << dir << ") FILE (" << filename << ") has event Delete" << std::endl;
		break;
	default:
		std::cerr << "Unknown file action: " << action << std::endl;
	}
}
