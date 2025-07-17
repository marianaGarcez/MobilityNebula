/*
    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        https://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/

#include "include/MEOSWrapper.hpp"

#include <string>
#include <chrono>
#include <ctime>
#include <iomanip>
#include <sstream>
#include <iostream>



namespace MEOS {

    // Global MEOS initialization
    static bool meos_initialized = false;

    static void ensureMeosInitialized() {
        if (!meos_initialized) {
            meos_initialize();
            meos_initialized = true;
        }
    }

    Meos::Meos() { 
        ensureMeosInitialized();
    }

    Meos::~Meos() { 
        meos_finalize();
    }

    std::string Meos::convertSecondsToTimestamp(long long seconds) {
        std::chrono::seconds sec(seconds);
        std::chrono::time_point<std::chrono::system_clock> tp(sec);

        // Convert to time_t for formatting
        std::time_t time = std::chrono::system_clock::to_time_t(tp);

        // Convert to local time
        std::tm local_tm = *std::localtime(&time);

        // Format the time as a string
        std::ostringstream oss;
        oss << std::put_time(&local_tm, "%Y-%m-%d %H:%M:%S");
        return oss.str();
    }

    // TemporalInstant constructor
    Meos::TemporalInstant::TemporalInstant(double lon, double lat, long long ts, int srid) {
        // Ensure MEOS is initialized
        ensureMeosInitialized();
        
        std::string ts_string = Meos::convertSecondsToTimestamp(ts);
        std::string str_pointbuffer = "SRID=" + std::to_string(srid) + ";POINT(" + std::to_string(lon) + " " + std::to_string(lat) + ")@" + ts_string;

        std::cout << "Creating MEOS TemporalInstant from: " << str_pointbuffer << std::endl;

        Temporal *temp = tgeompoint_in(str_pointbuffer.c_str());

        if (temp == nullptr) {
            std::cout << "Failed to parse temporal point with temporal_from_text" << std::endl;
            // Try alternative format or set to null
            instant = nullptr;
        } else {
            instant = temp;
            std::cout << "Successfully created temporal point" << std::endl;
        }
    }

    Meos::TemporalInstant::~TemporalInstant() { 
        if (instant) {
            free(instant); 
        }
    }

    bool Meos::TemporalInstant::intersects(const TemporalInstant& point) const {  
        std::cout << "TemporalInstant::intersects called" << std::endl;
        // Use MEOS eintersects function for temporal points  - this will change 
        bool result = eintersects_tgeo_tgeo((const Temporal *)this->instant, (const Temporal *)point.instant);
        if (result) {
            std::cout << "TemporalInstant intersects" << std::endl;
        }

        return result;
    }
    


 
    // TemporalSequence constructor
    Meos::TemporalSequence::TemporalSequence(double lon, double lat, int t_out) {
        // Ensure MEOS is initialized
        ensureMeosInitialized();
        sequence = nullptr;

        std::cout << "TemporalSequence created with coordinates (" << lon << ", " << lat << ") at time " << t_out << std::endl;
        // TODO:call the aggregation function

        //TODO: with the result of the aggregation function, we can create a temporal sequence
        
    }
    
    // Constructor for creating trajectory from multiple temporal instants
    Meos::TemporalSequence::TemporalSequence(const std::vector<TemporalInstant*>& instants) {
        // Ensure MEOS is initialized
        ensureMeosInitialized();

        sequence = nullptr;
        std::cout << "TemporalSequence created from " << instants.size() << " temporal instants" << std::endl;
    }
   


    Meos::TemporalSequence::~TemporalSequence() { 
        if (sequence) {
            // Check if this is a vector of instants or a single temporal object
            // This is a bit hacky but works for our current implementation
            try {
                // Try to interpret as vector first
                auto* instants_vec = reinterpret_cast<std::vector<Temporal*>*>(sequence);
                // Simple heuristic: if the "vector" has a reasonable size, treat as vector
                if (instants_vec && reinterpret_cast<uintptr_t>(instants_vec) > 0x1000) {
                    // Likely a vector pointer
                    for (auto* instant : *instants_vec) {
                        if (instant) free(instant);
                    }
                    delete instants_vec;
                } else {
                    free(sequence);
                }
            } catch (...) {
                free(sequence);
            }
            sequence = nullptr;
        }
    }

    double Meos::TemporalSequence::length(const TemporalInstant& /* instant */) const {
        // Placeholder implementation
        // Using comment to avoid unused parameter warning
        return 0.0;
    }
    

    // SpatioTemporalBox implementation
    Meos::SpatioTemporalBox::SpatioTemporalBox(const std::string& wkt_string) {
        // Ensure MEOS is initialized
        ensureMeosInitialized();
        // Use MEOS stbox_in function to parse the WKT string
        stbox_ptr = stbox_in(wkt_string.c_str());
    }

    Meos::SpatioTemporalBox::~SpatioTemporalBox() {
        if (stbox_ptr) {
            free(stbox_ptr);
            stbox_ptr = nullptr;
        }
    }


}// namespace MEOS

