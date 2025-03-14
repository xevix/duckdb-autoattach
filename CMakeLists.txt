cmake_minimum_required(VERSION 3.5)

# Set extension name here
set(TARGET_NAME autoattach)

find_package(efsw REQUIRED)
find_package(Boost REQUIRED COMPONENTS filesystem asio)

set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)

project(${TARGET_NAME})
include_directories(src/include)

set(EXTENSION_SOURCES src/update_listener.cpp src/listener_client_context_state.cpp src/autoattach_extension.cpp src/s3_watcher.cpp)

build_static_extension(${TARGET_NAME} ${EXTENSION_SOURCES})
build_loadable_extension(${TARGET_NAME} " " ${EXTENSION_SOURCES})

target_link_libraries(${EXTENSION_NAME} efsw::efsw)
target_link_libraries(${LOADABLE_EXTENSION_NAME} efsw::efsw)
target_link_libraries(${EXTENSION_NAME} Boost::filesystem Boost::asio)
target_link_libraries(${LOADABLE_EXTENSION_NAME} Boost::filesystem Boost::asio)
install(
  TARGETS ${EXTENSION_NAME}
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
