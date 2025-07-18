# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

add_subdirectory(src)
get_source(nes-systest-sources NES_SOURCES_SOURCE_FILES)

find_package(yaml-cpp CONFIG REQUIRED)

add_library(nes-systest-sources ${NES_SOURCES_SOURCE_FILES})
target_link_libraries(nes-systest-sources PRIVATE nes-common nes-data-types nes-configurations nes-memory yaml-cpp::yaml-cpp)

target_include_directories(nes-systest-sources PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include/nebulastream/>
        PRIVATE $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/private>)

if (NES_ENABLE_PRECOMPILED_HEADERS)
    target_precompile_headers(nes-systest-sources REUSE_FROM nes-common)
endif ()
