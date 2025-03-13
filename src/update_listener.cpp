#include <efsw/efsw.hpp>
#include <iostream>

#include "duckdb.hpp"
#include "duckdb/main/attached_database.hpp"
#include "update_listener.hpp"

void UpdateListener::handleFileAction(efsw::WatchID watchid, const std::string &dir, const std::string &filename,
                                      efsw::Action action, std::string oldFilename) {

	switch (action) {
	case efsw::Actions::Add:
		if (filename.find(".duckdb") != std::string::npos && filename.substr(filename.length() - 7) == ".duckdb") {
			listener_state->attach(db_alias, dir + filename, true);
		}
		break;
	case efsw::Actions::Delete:
		// TODO: detach if currently attached, and try to attach now-latest file with the pattern. If no files found,
		// throw an error.
		break;
	default:
		std::cerr << "Unknown file action: " << action << std::endl;
	}
}
