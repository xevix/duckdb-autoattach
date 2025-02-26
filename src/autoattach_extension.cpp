#define DUCKDB_EXTENSION_MAIN

#include "autoattach_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/extension_util.hpp"
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>

#include <efsw/efsw.hpp>

class UpdateListener : public efsw::FileWatchListener {
  public:
    void handleFileAction( efsw::WatchID watchid, const std::string& dir,
                           const std::string& filename, efsw::Action action,
                           std::string oldFilename ) override {
        switch ( action ) {
            case efsw::Actions::Add:
                std::cerr << "DIR (" << dir << ") FILE (" << filename << ") has event Added"
                          << std::endl;
                break;
            case efsw::Actions::Delete:
            // TODO: detach if currently attached, and try to attach now-latest file with the pattern. If no files found, throw an error.
                std::cerr << "DIR (" << dir << ") FILE (" << filename << ") has event Delete"
                          << std::endl;
                break;
            default:
                std::cerr << "Unknown file action: " << action << std::endl;
        }
    }
};

namespace duckdb {
inline void AutoattachScalarFun(DataChunk &args, ExpressionState &state, Vector &result) {
    auto &name_vector = args.data[0];
    UnaryExecutor::Execute<string_t, string_t>(
	    name_vector, result, args.size(),
	    [&](string_t name) {
			return StringVector::AddString(result, "Autoattach "+name.GetString()+" 🚀");
        });
}

static void LoadInternal(DatabaseInstance &instance) {
    // Register a scalar function
    auto autoattach_scalar_function = ScalarFunction("attach_auto", {LogicalType::VARCHAR}, LogicalType::VARCHAR, AutoattachScalarFun);
    ExtensionUtil::RegisterFunction(instance, autoattach_scalar_function);

    // Watch files
    // Create the file system watcher instance
    // efsw::FileWatcher allow a first boolean parameter that indicates if it should start with the
    // generic file watcher instead of the platform specific backend
    efsw::FileWatcher* fileWatcher = new efsw::FileWatcher();

    // Create the instance of your efsw::FileWatcherListener implementation
    UpdateListener* listener = new UpdateListener();

    // Add a folder to watch, and get the efsw::WatchID
    // It will watch the /tmp folder recursively ( the third parameter indicates that is recursive )
    // Reporting the files and directories changes to the instance of the listener
    efsw::WatchID watchID = fileWatcher->addWatch( "/tmp", listener, true );
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
