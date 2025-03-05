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
  private:
    duckdb::DuckDB &db;
    std::string db_alias;
  public:
    UpdateListener(duckdb::DuckDB &db, const std::string &alias) 
        : db(db), db_alias(alias) {}

    void attach(const std::string& new_db_path) {
        duckdb::Connection con(db);
        auto watched_db = db.instance->GetDatabaseManager().GetDatabase(*con.context, db_alias);
        auto current_db_path = duckdb::Catalog::GetCatalog(*watched_db).GetDBPath();
        // Run a Query() to fetch the current attached file

        // Check if the filename is lexicographically greater than the current attached file
        if (new_db_path > current_db_path) {
            con.Query("ATTACH OR REPLACE '" + new_db_path + "' AS " + db_alias);
        }

    }

    void handleFileAction( efsw::WatchID watchid, const std::string& dir,
                           const std::string& filename, efsw::Action action,
                           std::string oldFilename ) override {
        
        switch ( action ) {
            case efsw::Actions::Add:
                std::cerr << "DIR (" << dir << ") FILE (" << filename << ") has event Added"
                          << std::endl;
                if (filename.find(".duckdb") != std::string::npos && 
                    filename.substr(filename.length() - 7) == ".duckdb") {
                    attach(dir + "/" + filename);
                }
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
    auto &pattern_vector = args.data[1];
    
    BinaryExecutor::Execute<string_t, string_t, string_t>(
        name_vector, pattern_vector, result, args.size(),
        [&](string_t name, string_t pattern) {
            // TODO: call the extension's addWatch method
            addWatch(name.GetString(), pattern.GetString());
            return StringVector::AddString(result, 
                "Autoattach: watching for pattern " + pattern.GetString() + 
                " (" + name.GetString() + ") ��");
        });
}

static void LoadInternal(DatabaseInstance &instance) {
    // Register scalar function with 2 arguments
    auto autoattach_scalar_function = ScalarFunction("attach_auto", 
        {LogicalType::VARCHAR, LogicalType::VARCHAR}, 
        LogicalType::VARCHAR, 
        AutoattachScalarFun);
    ExtensionUtil::RegisterFunction(instance, autoattach_scalar_function);
}

void AutoattachExtension::Load(DuckDB &db) {
    this->db_instance = &db;
    this->fileWatcher = new efsw::FileWatcher();
    LoadInternal(*db.instance);
}

void AutoattachExtension::Unload() {
    if (fileWatcher) {
        delete fileWatcher;
        fileWatcher = nullptr;
    }
    db_instance = nullptr;  // Clear database reference
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

void AutoattachExtension::addWatch(const std::string& path, const std::string& alias) {
    auto listener = new UpdateListener(*this->db_instance, alias);
    // TODO: bootstrap the listener with the first file to attach
    fileWatcher->addWatch(path, listener, false);
    // Start watching asynchronously the directories
    fileWatcher->watch();
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
