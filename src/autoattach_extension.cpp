#define DUCKDB_EXTENSION_MAIN

#include "autoattach_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/main/extension_util.hpp"
#include "duckdb/main/query_result.hpp"
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/main/attached_database.hpp"
#include <efsw/efsw.hpp>
#include <boost/filesystem.hpp>

#include "update_listener.hpp"
#include "listener_client_context_state.hpp"

// SELECT attach_auto('watched_db', '/Users/xevix/dbtest');
namespace duckdb {
inline void AutoattachScalarFun(DataChunk &args, ExpressionState &state, Vector &result) {
	// TODO: sanitize input, check if path exists, etc.
	auto &name_vector = args.data[0];
	auto &pattern_vector = args.data[1];

	auto const &listener_state = state.GetContext().registered_state->GetOrCreate<ListenerClientContextState>(
	    "listener_client_context_state", &state.GetContext());

	BinaryExecutor::Execute<string_t, string_t, string_t>(
	    name_vector, pattern_vector, result, args.size(), [&](string_t name, string_t pattern) {
		    listener_state->addWatch(pattern.GetString(), name.GetString());
		    return StringVector::AddString(result, "alias: " + name.GetString());
	    });
}

static void LoadInternal(DatabaseInstance &instance) {
	auto &config = DBConfig::GetConfig(instance);
	config.AddExtensionOption("s3_poll_interval", "S3 poll interval (in seconds)", LogicalType::UBIGINT,
	                          Value::UBIGINT(60));
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
