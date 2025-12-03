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

#include <Serialization/FunctionSerializationUtil.hpp>
#include <Serialization/TemporalAggregationSerde.hpp>

#include <memory>
#include <vector>

#include <Configurations/Descriptor.hpp>
#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Operators/Windows/Aggregations/WindowAggregationLogicalFunction.hpp>
#include <Serialization/DataTypeSerializationUtil.hpp>
#include <AggregationLogicalFunctionRegistry.hpp>
#include <ErrorHandling.hpp>
#include <LogicalFunctionRegistry.hpp>
#include <SerializableVariantDescriptor.pb.h>

namespace NES::FunctionSerializationUtil
{

LogicalFunction deserializeFunction(const SerializableFunction& serializedFunction)
{
    const auto& functionType = serializedFunction.function_type();

    std::vector<LogicalFunction> deserializedChildren;
    for (const auto& child : serializedFunction.children())
    {
        deserializedChildren.emplace_back(deserializeFunction(child));
    }

    auto dataType = DataTypeSerializationUtil::deserializeDataType(serializedFunction.data_type());

    DescriptorConfig::Config functionDescriptorConfig{};
    for (const auto& [key, value] : serializedFunction.config())
    {
        functionDescriptorConfig[key] = protoToDescriptorConfigType(value);
    }

    auto argument = LogicalFunctionRegistryArguments(functionDescriptorConfig, deserializedChildren, dataType);

    if (auto function = LogicalFunctionRegistry::instance().create(functionType, argument))
    {
        return function.value();
    }
    throw CannotDeserialize("Logical Function: {}", serializedFunction.DebugString());
}

std::shared_ptr<WindowAggregationLogicalFunction>
deserializeWindowAggregationFunction(const SerializableAggregationFunction& serializedFunction)
{
    const auto& type = serializedFunction.type();

    // Special handling for TemporalSequence: extra fields stored inside on_field.config
    if (type == std::string("TemporalSequence"))
    {
        AggregationLogicalFunctionRegistryArguments args;
        const auto fields = TemporalAggregationSerde::parseTemporalSequence(serializedFunction);
        for (const auto& f : fields)
        {
            args.fields.push_back(f);
        }
        if (auto function = AggregationLogicalFunctionRegistry::instance().create(type, args))
        {
            return function.value();
        }
        throw UnknownLogicalOperator();
    }

    // Special handling for TemporalExtKalmanFilter: lat/ts stored in on_field.config
    if (type == std::string("TemporalExtKalmanFilter"))
    {
        AggregationLogicalFunctionRegistryArguments args;

        // on_field: lon
        const auto lonFn = deserializeFunction(serializedFunction.on_field());
        if (auto lon = lonFn.tryGet<FieldAccessLogicalFunction>())
        {
            args.fields.push_back(*lon);
        }
        else
        {
            throw CannotDeserialize("TemporalExtKalmanFilter: on_field is not FieldAccessLogicalFunction");
        }

        // extra fields from on_field.config: lat, ts
        const auto& onFieldCfg = serializedFunction.on_field().config();
        const auto key = std::string("TemporalExtKalmanFilter.extra_fields");
        if (!onFieldCfg.contains(key))
        {
            throw CannotDeserialize("TemporalExtKalmanFilter: missing extra_fields config entry");
        }

        const auto variant = protoToDescriptorConfigType(onFieldCfg.at(key));
        if (!std::holds_alternative<FunctionList>(variant))
        {
            throw CannotDeserialize("TemporalExtKalmanFilter: extra_fields config is not a FunctionList");
        }

        const auto list = std::get<FunctionList>(variant);
        if (list.functions_size() < 2)
        {
            throw CannotDeserialize("TemporalExtKalmanFilter: expected two functions (lat, ts) in extra_fields");
        }

        const auto latFn = deserializeFunction(list.functions(0));
        if (auto lat = latFn.tryGet<FieldAccessLogicalFunction>())
        {
            args.fields.push_back(*lat);
        }
        else
        {
            throw CannotDeserialize("TemporalExtKalmanFilter: latitude extra_field is not FieldAccessLogicalFunction");
        }

        const auto tsFn = deserializeFunction(list.functions(1));
        if (auto ts = tsFn.tryGet<FieldAccessLogicalFunction>())
        {
            args.fields.push_back(*ts);
        }
        else
        {
            throw CannotDeserialize("TemporalExtKalmanFilter: timestamp extra_field is not FieldAccessLogicalFunction");
        }

        // as_field: alias
        const auto asFn = deserializeFunction(serializedFunction.as_field());
        if (auto as = asFn.tryGet<FieldAccessLogicalFunction>())
        {
            args.fields.push_back(*as);
        }
        else
        {
            throw CannotDeserialize("TemporalExtKalmanFilter: as_field is not FieldAccessLogicalFunction");
        }

        if (auto function = AggregationLogicalFunctionRegistry::instance().create(type, args))
        {
            return function.value();
        }
        throw UnknownLogicalOperator();
    }

    auto onField = deserializeFunction(serializedFunction.on_field());
    auto asField = deserializeFunction(serializedFunction.as_field());

    if (auto fieldAccess = onField.tryGet<FieldAccessLogicalFunction>())
    {
        if (auto asFieldAccess = asField.tryGet<FieldAccessLogicalFunction>())
        {
            AggregationLogicalFunctionRegistryArguments args;
            args.fields = {fieldAccess.value(), asFieldAccess.value()};

            if (auto function = AggregationLogicalFunctionRegistry::instance().create(type, args))
            {
                return function.value();
            }
        }
    }
    throw UnknownLogicalOperator();
}

}
