#pragma once

#include <Functions/PhysicalFunction.hpp>
#include <Nautilus/DataTypes/VarVal.hpp>
#include <Nautilus/Interface/Record.hpp>

namespace NES {

/**
 * @brief Physical function to compute nearest approach distance between two
 *        temporal points given as (lon1, lat1, ts1, lon2, lat2, ts2).
 *
 * Uses MEOS::Meos::safe_nad_tgeo_tgeo under the hood.
 */
class NearestApproachDistancePhysicalFunction : public PhysicalFunctionConcept {
public:
    NearestApproachDistancePhysicalFunction(PhysicalFunction lon1Function,
                                            PhysicalFunction lat1Function,
                                            PhysicalFunction ts1Function,
                                            PhysicalFunction lon2Function,
                                            PhysicalFunction lat2Function,
                                            PhysicalFunction ts2Function);

    VarVal execute(const Record& record, ArenaRef& arena) const override;

private:
    std::vector<PhysicalFunction> parameterFunctions;
};

} // namespace NES

