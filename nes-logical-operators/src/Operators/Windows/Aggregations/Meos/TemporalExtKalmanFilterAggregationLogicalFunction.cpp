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

#include <Operators/Windows/Aggregations/Meos/TemporalExtKalmanFilterAggregationLogicalFunction.hpp>

#include <memory>
#include <string>
#include <string_view>

#include <AggregationLogicalFunctionRegistry.hpp>
#include <Configurations/Descriptor.hpp>
#include <DataTypes/DataTypeProvider.hpp>
#include <DataTypes/Schema.hpp>
#include <ErrorHandling.hpp>
#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Functions/LogicalFunction.hpp>
#include <SerializableVariantDescriptor.pb.h>

namespace NES {

TemporalExtKalmanFilterAggregationLogicalFunction::TemporalExtKalmanFilterAggregationLogicalFunction(
    const FieldAccessLogicalFunction& lon,
    const FieldAccessLogicalFunction& lat,
    const FieldAccessLogicalFunction& ts,
    const FieldAccessLogicalFunction& asField)
    : WindowAggregationLogicalFunction(
          lon.getDataType(),
          DataTypeProvider::provideDataType(partialAggregateStampType),
          DataTypeProvider::provideDataType(finalAggregateStampType),
          lon,
          asField)
    , lonField(lon)
    , latField(lat)
    , timestampField(ts) {
}

std::shared_ptr<WindowAggregationLogicalFunction>
TemporalExtKalmanFilterAggregationLogicalFunction::create(
    const FieldAccessLogicalFunction& lon,
    const FieldAccessLogicalFunction& lat,
    const FieldAccessLogicalFunction& ts) {
    // Default alias will be adjusted in inferStamp
    return std::make_shared<TemporalExtKalmanFilterAggregationLogicalFunction>(lon, lat, ts, lon);
}

std::string_view TemporalExtKalmanFilterAggregationLogicalFunction::getName() const noexcept {
    return NAME;
}

void TemporalExtKalmanFilterAggregationLogicalFunction::inferStamp(const Schema& schema) {
    // Infer data types for lon, lat, ts
    lonField = lonField.withInferredDataType(schema).get<FieldAccessLogicalFunction>();
    latField = latField.withInferredDataType(schema).get<FieldAccessLogicalFunction>();
    timestampField = timestampField.withInferredDataType(schema).get<FieldAccessLogicalFunction>();

    onField = lonField;

    if (!lonField.getDataType().isNumeric()
        || !latField.getDataType().isNumeric()
        || !timestampField.getDataType().isNumeric()) {
        throw CannotInferSchema(
            "TemporalExtKalmanFilterAggregationLogicalFunction: lon, lat, and timestamp fields must be numeric.");
    }

    // Qualify alias field similar to TemporalSequence
    const auto onFieldName = onField.getFieldName();
    const auto asFieldName = asField.getFieldName();
    const auto attributeNameResolver =
        onFieldName.substr(0, onFieldName.find(Schema::ATTRIBUTE_NAME_SEPARATOR) + 1);

    if (asFieldName.find(Schema::ATTRIBUTE_NAME_SEPARATOR) == std::string::npos) {
        asField = asField.withFieldName(attributeNameResolver + asFieldName)
                      .get<FieldAccessLogicalFunction>();
    } else {
        const auto fieldName =
            asFieldName.substr(asFieldName.find_last_of(Schema::ATTRIBUTE_NAME_SEPARATOR) + 1);
        asField = asField.withFieldName(attributeNameResolver + fieldName)
                      .get<FieldAccessLogicalFunction>();
    }

    asField = asField.withDataType(getFinalAggregateStamp()).get<FieldAccessLogicalFunction>();
    inputStamp = onField.getDataType();
}

NES::SerializableAggregationFunction
TemporalExtKalmanFilterAggregationLogicalFunction::serialize() const {
    SerializableAggregationFunction saf;
    saf.set_type(std::string(NAME));

    // on_field: longitude (with extra lat/ts packed in config)
    SerializableFunction lonProto;
    lonProto.CopyFrom(LogicalFunction(lonField).serialize());

    // Pack extra fields (lat, ts) into on_field.config as a FunctionList
    FunctionList extraList;
    *extraList.add_functions() = LogicalFunction(latField).serialize();
    *extraList.add_functions() = LogicalFunction(timestampField).serialize();

    const auto key = std::string("TemporalExtKalmanFilter.extra_fields");
    (*lonProto.mutable_config())[key] = descriptorConfigTypeToProto(extraList);
    saf.mutable_on_field()->CopyFrom(lonProto);

    // as_field: alias
    SerializableFunction asProto;
    asProto.CopyFrom(LogicalFunction(asField).serialize());
    saf.mutable_as_field()->CopyFrom(asProto);

    return saf;
}

// Registry hook
AggregationLogicalFunctionRegistryReturnType
AggregationLogicalFunctionGeneratedRegistrar::RegisterTemporalExtKalmanFilterAggregationLogicalFunction(
    AggregationLogicalFunctionRegistryArguments arguments) {
    if (arguments.fields.size() == 4) {
        auto ptr = std::make_shared<TemporalExtKalmanFilterAggregationLogicalFunction>(
            arguments.fields[0], arguments.fields[1], arguments.fields[2], arguments.fields[3]);
        return ptr;
    }
    throw CannotDeserialize(
        "TemporalExtKalmanFilterAggregationLogicalFunction requires lon, lat, timestamp, and alias fields but got {}",
        arguments.fields.size());
}

} // namespace NES
