# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

add_plugin_as_library(TCP Source nes-sources-registry tcp_source_plugin_library TCPSource.cpp)
add_plugin_as_library(TCP SourceValidation nes-sources-registry tcp_source_validation_plugin_library TCPSource.cpp)
add_plugin_as_library(TCP InlineData nes-sources-registry tcp_inline_data_plugin_library TCPSource.cpp)
add_plugin_as_library(TCP FileData nes-sources-registry tcp_file_data_plugin_library TCPSource.cpp)
add_plugin_as_library(TCP GeneratorData nes-sources-registry tcp_generator_data_plugin_library TCPSource.cpp)

target_include_directories(tcp_source_plugin_library PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/)
