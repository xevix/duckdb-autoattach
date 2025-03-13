#pragma once

#include "duckdb.hpp"
#include <efsw/efsw.hpp>
#include "listener_client_context_state.hpp"
class UpdateListener : public efsw::FileWatchListener {
private:
	duckdb::ClientContext *context;
	std::string current_attached_file;
	std::string db_alias;
    ListenerClientContextState *listener_state;
public:
	UpdateListener(duckdb::ClientContext *context, const std::string &alias, ListenerClientContextState *listener_state) : context(context), db_alias(alias), listener_state(listener_state) {
	}

	void handleFileAction(efsw::WatchID watchid, const std::string &dir, const std::string &filename,
	                      efsw::Action action, std::string oldFilename) override;
};
