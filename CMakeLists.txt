cmake_minimum_required(VERSION 2.8.8 FATAL_ERROR)

cmake_policy(VERSION 2.8.8)
project(maps-express)
set(configuration "Release")

set(CMAKE_C_OUTPUT_EXTENSION_REPLACE 1)
set(CMAKE_CXX_OUTPUT_EXTENSION_REPLACE 1)

file(GLOB_RECURSE maps-express_src
    ${PROJECT_SOURCE_DIR}/src/*.h
    ${PROJECT_SOURCE_DIR}/src/*.cpp)
add_executable(maps-express ${maps-express_src})

list(APPEND maps-express__include_dirs
  "${CMAKE_SOURCE_DIR}/mapnik-vector-tile/src"
  "${CMAKE_SOURCE_DIR}/src/include"
  "${CMAKE_SOURCE_DIR}/mapnik-vector-tile/deps/protozero/include"
  "${CMAKE_SOURCE_DIR}/mapnik-vector-tile/build/Release/obj/gen/"
  "${CMAKE_SOURCE_DIR}/mapnik-vector-tile/deps/clipper/cpp"
  "/usr/include/mapnik/agg")
set_property(TARGET maps-express APPEND PROPERTY INCLUDE_DIRECTORIES ${maps-express__include_dirs})
set_target_properties(maps-express PROPERTIES COMPILE_DEFINITIONS
    "CLIPPER_INTPOINT_IMPL=mapnik::geometry::point<cInt>;CLIPPER_PATH_IMPL=mapnik::geometry::line_string<cInt>;\
    CLIPPER_PATHS_IMPL=mapnik::geometry::multi_line_string<cInt>;CLIPPER_IMPL_INCLUDE=<mapnik/geometry.hpp>;\
    MAPNIK_PLUGINDIR=\"/usr/lib/mapnik/3.0/input\";")
set_target_properties(maps-express PROPERTIES COMPILE_FLAGS "-std=c++14 -DMAPNIK_VECTOR_TILE_LIBRARY \
    -DMAPNIK_MEMORY_MAPPED_FILE -DMAPNIK_HAS_DLCFN -DBIGINT -DBOOST_REGEX_HAS_ICU -DHAVE_JPEG -DMAPNIK_USE_PROJ4 \
    -DHAVE_PNG -DHAVE_WEBP -DHAVE_TIFF -DLINUX -DMAPNIK_THREADSAFE -DBOOST_SPIRIT_NO_PREDEFINED_TERMINALS=1 \
    -DBOOST_PHOENIX_NO_PREDEFINED_TERMINALS=1 -DBOOST_SPIRIT_USE_PHOENIX_V3=1 -DHAVE_CAIRO -DGRID_RENDERER \
    -fvisibility=hidden -fvisibility-inlines-hidden -Wall -pthread -ftemplate-depth-300 -Wsign-compare -Wshadow \
    -Werror -g")

target_link_libraries(maps-express
      ${CMAKE_SOURCE_DIR}/mapnik-vector-tile/build/Release/obj.target/gyp/libmapnik_vector_tile_impl.a
      ${CMAKE_SOURCE_DIR}/mapnik-vector-tile/build/Release/obj.target/gyp/libvector_tile.a
      -licuuc
      -lz
      -lmapnik
      -pthread
      -lfolly
      -lglog
      -lgflags
      -lcassandra
      -lproxygenlib
      -lproxygenhttpserver
      -lcrypto
      -ljsoncpp
      -lboost_system
      -lboost_filesystem
      -lcouchbase
)

install(TARGETS maps-express DESTINATION /opt/sputnik/maps/maps-express/)
install(FILES ${CMAKE_SOURCE_DIR}/config/config.json
        DESTINATION /opt/sputnik/maps/maps-express/config/)
install(DIRECTORY DESTINATION /opt/sputnik/maps/maps-express/logs
        DIRECTORY_PERMISSIONS WORLD_READ WORLD_WRITE)

