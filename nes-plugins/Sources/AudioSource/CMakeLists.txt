# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

add_plugin_as_library(Audio Source nes-sources-registry audio_source_plugin_library AudioSource.cpp)
target_include_directories(audio_source_plugin_library
        PUBLIC include
        PRIVATE .
)
add_plugin_as_library(Audio SourceValidation nes-sources-registry audio_source_validation_plugin_library AudioSource.cpp)
target_include_directories(audio_source_validation_plugin_library
        PUBLIC include
        PRIVATE .
)

add_plugin_as_library(Audio GeneratorData nes-sources-registry audio_generator_data_plugin_library AudioSource.cpp)
target_include_directories(audio_generator_data_plugin_library
        PUBLIC include
        PRIVATE .
)
