#pragma once

#include <Functions/PhysicalFunction.hpp>
#include <Nautilus/DataTypes/VarVal.hpp>
#include <Nautilus/Interface/Record.hpp>

namespace NES {

class TemporalEDWithinGeometryPhysicalFunction final {
public:
    TemporalEDWithinGeometryPhysicalFunction(PhysicalFunction lonFunction,
                                             PhysicalFunction latFunction,
                                             PhysicalFunction timestampFunction,
                                             PhysicalFunction geometryFunction,
                                             PhysicalFunction distanceFunction);

    [[nodiscard]] VarVal execute(const Record& record, ArenaRef& arena) const;

private:
    std::vector<PhysicalFunction> parameterFunctions;
};

static_assert(PhysicalFunctionConcept<TemporalEDWithinGeometryPhysicalFunction>);

}
