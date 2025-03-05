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


class UpdateListener : public efsw::FileWatchListener {
  private:
    duckdb::ClientContext* context;
    // duckdb::shared_ptr<duckdb::DuckDB> db;
    std::string current_attached_file;
    std::string db_alias;
  public:
    UpdateListener(duckdb::ClientContext* context, const std::string &alias) 
        : context(context), db_alias(alias) {}

    void attach(const std::string& new_db_path) {
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

class ListenerClientContextState : public duckdb::ClientContextState {
    private:
    efsw::FileWatcher* fileWatcher = nullptr;
    public:
    ListenerClientContextState() : fileWatcher(new efsw::FileWatcher()) {}
    efsw::FileWatcher* getFileWatcher() { return fileWatcher; }
    void addWatch(const std::string& path, const std::string& alias, duckdb::ClientContext& context) {
        auto listener = new UpdateListener(&context, alias);
        // TODO: bootstrap the listener with the first file to attach
        std::cerr << "Adding watch to: " << path << std::endl;
        fileWatcher->addWatch(path, listener, false);
        // Start watching asynchronously the directories
        fileWatcher->watch();
        std::cerr << "Watching directories: " << fileWatcher->directories().size() << std::endl;
    }
};

// SELECT attach_auto('watched_db', '/Users/xevix/dbtest');
namespace duckdb {
inline void AutoattachScalarFun(DataChunk &args, ExpressionState &state, Vector &result) {
    // TODO: sanitize input, check if path exists, etc.
    auto &name_vector = args.data[0];
    auto &pattern_vector = args.data[1];

    auto const &listener_state = state.GetContext().registered_state->GetOrCreate<ListenerClientContextState>("listener_client_context_state");

    BinaryExecutor::Execute<string_t, string_t, string_t>(
        name_vector, pattern_vector, result, args.size(),
        [&](string_t name, string_t pattern) {
            listener_state->addWatch(pattern.GetString(), name.GetString(), state.GetContext());
            return StringVector::AddString(result, 
                "alias: " + name.GetString());
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
    // this->db_instance = &db;
    // this->fileWatcher = new efsw::FileWatcher();
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
