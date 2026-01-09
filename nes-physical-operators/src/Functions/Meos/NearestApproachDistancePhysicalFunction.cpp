#include <Functions/Meos/NearestApproachDistancePhysicalFunction.hpp>

#include <ErrorHandling.hpp>
#include <Functions/Meos/GeoFunctionMetrics.hpp>
#include <Functions/Meos/GeoOperatorMetrics.hpp>
#include <ExecutionContext.hpp>
#include <MEOSWrapper.hpp>
#include <Nautilus/DataTypes/VariableSizedData.hpp>
#include <PhysicalFunctionRegistry.hpp>
#include <fmt/format.h>
#include <function.hpp>
#include <iostream>
#include <string>
#include <utility>
#include <val.hpp>

namespace NES {

NearestApproachDistancePhysicalFunction::NearestApproachDistancePhysicalFunction(PhysicalFunction lon1Function,
                                                                                 PhysicalFunction lat1Function,
                                                                                 PhysicalFunction ts1Function,
                                                                                 PhysicalFunction lon2Function,
                                                                                 PhysicalFunction lat2Function,
                                                                                 PhysicalFunction ts2Function) {
    parameterFunctions.reserve(6);
    parameterFunctions.push_back(std::move(lon1Function));
    parameterFunctions.push_back(std::move(lat1Function));
    parameterFunctions.push_back(std::move(ts1Function));
    parameterFunctions.push_back(std::move(lon2Function));
    parameterFunctions.push_back(std::move(lat2Function));
    parameterFunctions.push_back(std::move(ts2Function));
}

VarVal NearestApproachDistancePhysicalFunction::execute(const Record& record, ArenaRef& arena) const {
    std::vector<VarVal> parameterValues;
    parameterValues.reserve(parameterFunctions.size());
    for (const auto& fn : parameterFunctions) {
        parameterValues.emplace_back(fn.execute(record, arena));
    }

    auto lon1 = parameterValues[0].cast<nautilus::val<double>>();
    auto lat1 = parameterValues[1].cast<nautilus::val<double>>();
    auto ts1  = parameterValues[2].cast<nautilus::val<uint64_t>>();
    auto lon2 = parameterValues[3].cast<nautilus::val<double>>();
    auto lat2 = parameterValues[4].cast<nautilus::val<double>>();
    auto ts2  = parameterValues[5].cast<nautilus::val<uint64_t>>();

    const auto result = nautilus::invoke(
        +[](double lon1Val,
            double lat1Val,
            uint64_t ts1Val,
            double lon2Val,
            double lat2Val,
            uint64_t ts2Val) -> double {
            GeoOperatorTimingScope op_timing(GeoFunctionId::NearestApproachDistance);
            GeoFunctionTimingScope timing(GeoFunctionId::NearestApproachDistance);
            try {
                MEOS::Meos::ensureMeosInitialized();

                auto inRange = [](double lo, double la) {
                    return lo >= -180.0 && lo <= 180.0 && la >= -90.0 && la <= 90.0;
                };
                if (!inRange(lon1Val, lat1Val) || !inRange(lon2Val, lat2Val)) {
                    std::cout << "NearestApproachDistance: coordinates out of range" << std::endl;
                    return 0.0;
                }

                std::string ts1Str = MEOS::Meos::convertEpochToTimestamp(ts1Val);
                std::string ts2Str = MEOS::Meos::convertEpochToTimestamp(ts2Val);

                std::string leftWkt  = fmt::format("SRID=4326;Point({} {})@{}", lon1Val, lat1Val, ts1Str);
                std::string rightWkt = fmt::format("SRID=4326;Point({} {})@{}", lon2Val, lat2Val, ts2Str);

                MEOS::Meos::TemporalGeometry left(leftWkt);
                if (!left.getGeometry()) {
                    std::cout << "NearestApproachDistance: left temporal geometry is null" << std::endl;
                    return 0.0;
                }
                MEOS::Meos::TemporalGeometry right(rightWkt);
                if (!right.getGeometry()) {
                    std::cout << "NearestApproachDistance: right temporal geometry is null" << std::endl;
                    return 0.0;
                }

                const auto* temp1 = static_cast<const Temporal*>(left.getGeometry());
                const auto* temp2 = static_cast<const Temporal*>(right.getGeometry());
                //call MEOS nearest approach distance function
                return MEOS::Meos::safe_nad_tgeo_tgeo(temp1, temp2);
            } catch (const std::exception& e) {
                std::cout << "MEOS exception in NearestApproachDistance: " << e.what() << std::endl;
                return -1.0;
            } catch (...) {
                std::cout << "Unknown error in NearestApproachDistance" << std::endl;
                return -1.0;
            }
        },
        lon1,
        lat1,
        ts1,
        lon2,
        lat2,
        ts2);

    return VarVal(result);
}

PhysicalFunctionRegistryReturnType
PhysicalFunctionGeneratedRegistrar::RegisterNearestApproachDistancePhysicalFunction(
    PhysicalFunctionRegistryArguments arguments) {
    PRECONDITION(arguments.childFunctions.size() == 6,
                 "NearestApproachDistancePhysicalFunction requires 6 child functions, but got {}",
                 arguments.childFunctions.size());

    return NearestApproachDistancePhysicalFunction(arguments.childFunctions[0],
                                                   arguments.childFunctions[1],
                                                   arguments.childFunctions[2],
                                                   arguments.childFunctions[3],
                                                   arguments.childFunctions[4],
                                                   arguments.childFunctions[5]);
}

} // namespace NES
