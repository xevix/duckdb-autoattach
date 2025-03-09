#include <efsw/efsw.hpp>
#include <iostream>

#include "duckdb.hpp"
#include "duckdb/main/attached_database.hpp"
#include "update_listener.hpp"


void UpdateListener::attach(const std::string &new_db_path) {
	duckdb::Connection con(*context->db);
	context->RunFunctionInTransaction([&]() {
		auto watched_db = context->db->GetDatabaseManager().GetDatabase(*context, db_alias);
		std::string current_db_path;
		if (watched_db) {
			current_db_path = duckdb::Catalog::GetCatalog(*watched_db).GetDBPath();
		} else {
			current_db_path = "";
		}
		// Run a Query() to fetch the current attached file

		// Check if the filename is lexicographically greater than the current attached file
		if (new_db_path > current_db_path) {
			auto attach_info = duckdb::unique_ptr<duckdb::AttachInfo>(new duckdb::AttachInfo());
			attach_info->name = db_alias;
			attach_info->path = new_db_path;
			attach_info->on_conflict = duckdb::OnCreateConflict::REPLACE_ON_CONFLICT;
			std::cerr << "Attaching " << new_db_path << " as " << db_alias << std::endl;
			duckdb::AttachOptions options(attach_info, duckdb::AccessMode::READ_ONLY);
			// context->db->GetDatabaseManager().AttachDatabase(*context, *attach_info, options);
			// con.BeginTransaction();
			auto result = con.Query("ATTACH OR REPLACE '" + new_db_path + "' AS " + db_alias);
			if (result->HasError()) {
				std::cerr << result->GetError() << std::endl;
			} else {
				std::cerr << result->ToString() << std::endl;
			}
			// con.Commit();
		}
	});
}

void UpdateListener::handleFileAction(efsw::WatchID watchid, const std::string &dir, const std::string &filename,
                                      efsw::Action action, std::string oldFilename) {

	switch (action) {
	case efsw::Actions::Add:
		std::cerr << "DIR (" << dir << ") FILE (" << filename << ") has event Added" << std::endl;
		if (filename.find(".duckdb") != std::string::npos && filename.substr(filename.length() - 7) == ".duckdb") {
			attach(dir + "/" + filename);
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
