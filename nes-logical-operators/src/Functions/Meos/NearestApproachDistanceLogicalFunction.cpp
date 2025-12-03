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

#include <Functions/Meos/NearestApproachDistanceLogicalFunction.hpp>

#include <DataTypes/DataTypeProvider.hpp>
#include <ErrorHandling.hpp>
#include <LogicalFunctionRegistry.hpp>
#include <Serialization/DataTypeSerializationUtil.hpp>
#include <fmt/format.h>

namespace NES {

NearestApproachDistanceLogicalFunction::NearestApproachDistanceLogicalFunction(LogicalFunction lon1,
                                                                               LogicalFunction lat1,
                                                                               LogicalFunction ts1,
                                                                               LogicalFunction lon2,
                                                                               LogicalFunction lat2,
                                                                               LogicalFunction ts2)
    : dataType(DataTypeProvider::provideDataType(DataType::Type::FLOAT64)) {
    parameters.reserve(6);
    parameters.push_back(std::move(lon1));
    parameters.push_back(std::move(lat1));
    parameters.push_back(std::move(ts1));
    parameters.push_back(std::move(lon2));
    parameters.push_back(std::move(lat2));
    parameters.push_back(std::move(ts2));
}

DataType NearestApproachDistanceLogicalFunction::getDataType() const {
    return dataType;
}

LogicalFunction NearestApproachDistanceLogicalFunction::withDataType(const DataType& newDataType) const {
    auto copy = *this;
    copy.dataType = newDataType;
    return copy;
}

std::vector<LogicalFunction> NearestApproachDistanceLogicalFunction::getChildren() const {
    return parameters;
}

LogicalFunction NearestApproachDistanceLogicalFunction::withChildren(const std::vector<LogicalFunction>& children) const {
    PRECONDITION(children.size() == 6,
                 "NearestApproachDistanceLogicalFunction requires 6 children, but got {}",
                 children.size());
    auto copy = *this;
    copy.parameters = children;
    return copy;
}

std::string_view NearestApproachDistanceLogicalFunction::getType() const {
    return NAME;
}

bool NearestApproachDistanceLogicalFunction::operator==(const LogicalFunctionConcept& rhs) const {
    if (const auto* other = dynamic_cast<const NearestApproachDistanceLogicalFunction*>(&rhs)) {
        return parameters == other->parameters;
    }
    return false;
}

std::string NearestApproachDistanceLogicalFunction::explain(ExplainVerbosity verbosity) const {
    std::string args;
    for (std::size_t i = 0; i < parameters.size(); ++i) {
        if (i > 0) {
            args += ", ";
        }
        args += parameters[i].explain(verbosity);
    }
    return fmt::format("NEARESTAPPROACHDISTANCE({})", args);
}

LogicalFunction NearestApproachDistanceLogicalFunction::withInferredDataType(const Schema& schema) const {
    std::vector<LogicalFunction> newChildren;
    newChildren.reserve(parameters.size());
    for (const auto& child : parameters) {
        newChildren.push_back(child.withInferredDataType(schema));
    }

    INVARIANT(newChildren.size() == 6,
              "NearestApproachDistanceLogicalFunction requires 6 children, but got {}",
              newChildren.size());

    INVARIANT(newChildren[0].getDataType().isNumeric(),
              "lon1 must be numeric, but was: {}", newChildren[0].getDataType());
    INVARIANT(newChildren[1].getDataType().isNumeric(),
              "lat1 must be numeric, but was: {}", newChildren[1].getDataType());
    INVARIANT(newChildren[2].getDataType().isType(DataType::Type::UINT64),
              "ts1 must be UINT64, but was: {}", newChildren[2].getDataType());
    INVARIANT(newChildren[3].getDataType().isNumeric(),
              "lon2 must be numeric, but was: {}", newChildren[3].getDataType());
    INVARIANT(newChildren[4].getDataType().isNumeric(),
              "lat2 must be numeric, but was: {}", newChildren[4].getDataType());
    INVARIANT(newChildren[5].getDataType().isType(DataType::Type::UINT64),
              "ts2 must be UINT64, but was: {}", newChildren[5].getDataType());

    return withChildren(newChildren);
}

SerializableFunction NearestApproachDistanceLogicalFunction::serialize() const {
    SerializableFunction serialized;
    serialized.set_function_type(NAME);
    for (const auto& child : parameters) {
        serialized.add_children()->CopyFrom(child.serialize());
    }
    DataTypeSerializationUtil::serializeDataType(getDataType(), serialized.mutable_data_type());
    return serialized;
}

LogicalFunctionRegistryReturnType
LogicalFunctionGeneratedRegistrar::RegisterNearestApproachDistanceLogicalFunction(
        LogicalFunctionRegistryArguments arguments) {
    PRECONDITION(arguments.children.size() == 6,
                 "NearestApproachDistanceLogicalFunction requires 6 children, but got {}",
                 arguments.children.size());

    return NearestApproachDistanceLogicalFunction(arguments.children[0],
                                                  arguments.children[1],
                                                  arguments.children[2],
                                                  arguments.children[3],
                                                  arguments.children[4],
                                                  arguments.children[5]);
}

} // namespace NES

