#define DUCKDB_EXTENSION_MAIN

#include "autoattach_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/extension_util.hpp"
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/main/attached_database.hpp"
#include <efsw/efsw.hpp>
#include <boost/filesystem.hpp>

#include "update_listener.hpp"

class ListenerClientContextState : public duckdb::ClientContextState {
public:
	ListenerClientContextState() : fileWatcher(new efsw::FileWatcher()) {
	}
	efsw::FileWatcher *getFileWatcher() {
		return fileWatcher;
	}

	std::string getLatestFileAtPath(const std::string &path) {
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

	void addWatch(const std::string &path, const std::string &alias, duckdb::ClientContext &context) {
		auto listener = new UpdateListener(&context, alias);
		// TODO: bootstrap the listener with the first file to attach
		std::cerr << "Adding watch to: " << path << std::endl;
		listener->attach(path + "/" + getLatestFileAtPath(path), false);
		fileWatcher->addWatch(path, listener, false);
		// Start watching asynchronously the directories
		fileWatcher->watch();
		std::cerr << "Watching directories: " << fileWatcher->directories().size() << std::endl;
	}

private:
	efsw::FileWatcher *fileWatcher = nullptr;
};

// SELECT attach_auto('watched_db', '/Users/xevix/dbtest');
namespace duckdb {
inline void AutoattachScalarFun(DataChunk &args, ExpressionState &state, Vector &result) {
	// TODO: sanitize input, check if path exists, etc.
	auto &name_vector = args.data[0];
	auto &pattern_vector = args.data[1];

	auto const &listener_state =
	    state.GetContext().registered_state->GetOrCreate<ListenerClientContextState>("listener_client_context_state");

	BinaryExecutor::Execute<string_t, string_t, string_t>(
	    name_vector, pattern_vector, result, args.size(), [&](string_t name, string_t pattern) {
		    listener_state->addWatch(pattern.GetString(), name.GetString(), state.GetContext());
		    return StringVector::AddString(result, "alias: " + name.GetString());
	    });
}

static void LoadInternal(DatabaseInstance &instance) {
	// Register scalar function with 2 arguments
	auto autoattach_scalar_function = ScalarFunction("attach_auto", {LogicalType::VARCHAR, LogicalType::VARCHAR},
	                                                 LogicalType::VARCHAR, AutoattachScalarFun);
	ExtensionUtil::RegisterFunction(instance, autoattach_scalar_function);
}

void AutoattachExtension::Load(DuckDB &db) {
	LoadInternal(*db.instance);
}

std::string AutoattachExtension::Name() {
	return "autoattach";
}

std::string AutoattachExtension::Version() const {
#ifdef EXT_VERSION_AUTOATTACH
	return EXT_VERSION_AUTOATTACH;
#else
	return "";
#endif
}

} // namespace duckdb

extern "C" {

DUCKDB_EXTENSION_API void autoattach_init(duckdb::DatabaseInstance &db) {
	duckdb::DuckDB db_wrapper(db);
	db_wrapper.LoadExtension<duckdb::AutoattachExtension>();
}

DUCKDB_EXTENSION_API const char *autoattach_version() {
	return duckdb::DuckDB::LibraryVersion();
}
}

#ifndef DUCKDB_EXTENSION_MAIN
#error DUCKDB_EXTENSION_MAIN not defined
#endif
