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
#include <Operators/Windows/Aggregations/Meos/TemporalExtKalmanFilterAggregationLogicalFunction.hpp>
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

    // Special handling for TemporalExtKalmanFilter: lat/ts and parameters stored in on_field.config
    if (type == std::string("TemporalExtKalmanFilter"))
    {
        // on_field: lon (with extra config)
        const auto lonFn = deserializeFunction(serializedFunction.on_field());
        const auto lon = lonFn.tryGet<FieldAccessLogicalFunction>();
        if (!lon)
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
        const auto lat = latFn.tryGet<FieldAccessLogicalFunction>();
        if (!lat)
        {
            throw CannotDeserialize("TemporalExtKalmanFilter: latitude extra_field is not FieldAccessLogicalFunction");
        }

        const auto tsFn = deserializeFunction(list.functions(1));
        const auto ts = tsFn.tryGet<FieldAccessLogicalFunction>();
        if (!ts)
        {
            throw CannotDeserialize("TemporalExtKalmanFilter: timestamp extra_field is not FieldAccessLogicalFunction");
        }

        // Kalman filter parameters: gate, q, variance, to_drop (all optional, defaulted)
        double gate = 3.0;
        double q = 0.01;
        double variance = 1.0;
        bool toDrop = false;

        const auto gateKey = std::string("TemporalExtKalmanFilter.gate");
        if (onFieldCfg.contains(gateKey))
        {
            const auto gateVariant = protoToDescriptorConfigType(onFieldCfg.at(gateKey));
            if (std::holds_alternative<double>(gateVariant))
            {
                gate = std::get<double>(gateVariant);
            }
            else
            {
                throw CannotDeserialize("TemporalExtKalmanFilter: gate parameter is not a double");
            }
        }

        const auto qKey = std::string("TemporalExtKalmanFilter.q");
        if (onFieldCfg.contains(qKey))
        {
            const auto qVariant = protoToDescriptorConfigType(onFieldCfg.at(qKey));
            if (std::holds_alternative<double>(qVariant))
            {
                q = std::get<double>(qVariant);
            }
            else
            {
                throw CannotDeserialize("TemporalExtKalmanFilter: q parameter is not a double");
            }
        }

        const auto varianceKey = std::string("TemporalExtKalmanFilter.variance");
        if (onFieldCfg.contains(varianceKey))
        {
            const auto varianceVariant = protoToDescriptorConfigType(onFieldCfg.at(varianceKey));
            if (std::holds_alternative<double>(varianceVariant))
            {
                variance = std::get<double>(varianceVariant);
            }
            else
            {
                throw CannotDeserialize("TemporalExtKalmanFilter: variance parameter is not a double");
            }
        }

        const auto toDropKey = std::string("TemporalExtKalmanFilter.to_drop");
        if (onFieldCfg.contains(toDropKey))
        {
            const auto toDropVariant = protoToDescriptorConfigType(onFieldCfg.at(toDropKey));
            if (std::holds_alternative<bool>(toDropVariant))
            {
                toDrop = std::get<bool>(toDropVariant);
            }
            else
            {
                throw CannotDeserialize("TemporalExtKalmanFilter: to_drop parameter is not a bool");
            }
        }

        // as_field: alias
        const auto asFn = deserializeFunction(serializedFunction.as_field());
        const auto as = asFn.tryGet<FieldAccessLogicalFunction>();
        if (!as)
        {
            throw CannotDeserialize("TemporalExtKalmanFilter: as_field is not FieldAccessLogicalFunction");
        }

        return std::make_shared<TemporalExtKalmanFilterAggregationLogicalFunction>(
            *lon, *lat, *ts, *as, gate, q, variance, toDrop);
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
