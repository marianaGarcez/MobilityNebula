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

#include <Aggregation/Function/ArrayAggregationPhysicalFunction.hpp>

#include <ErrorHandling.hpp>
#include <Util/Logger/Logger.hpp>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <stdexcept>
#include <utility>

#include <MemoryLayout/ColumnLayout.hpp>
#include <nautilus/Interface/MemoryProvider/ColumnTupleBufferMemoryProvider.hpp>
#include <nautilus/Interface/MemoryProvider/TupleBufferMemoryProvider.hpp>
#include <nautilus/Interface/PagedVector/PagedVector.hpp>
#include <nautilus/Interface/PagedVector/PagedVectorRef.hpp>
#include <nautilus/Interface/Record.hpp>
#include <nautilus/function.hpp>

#include <AggregationPhysicalFunctionRegistry.hpp>
#include <ErrorHandling.hpp>
#include <val.hpp>
#include <val_concepts.hpp>
#include <val_ptr.hpp>

namespace NES
{

constexpr static std::string_view StateFieldName = "value";

ArrayAggregationPhysicalFunction::ArrayAggregationPhysicalFunction(
    DataType inputType,
    DataType resultType,
    PhysicalFunction inputFunction,
    Nautilus::Record::RecordFieldIdentifier resultFieldIdentifier,
    std::shared_ptr<Nautilus::Interface::MemoryProvider::TupleBufferMemoryProvider> memProviderPagedVector)
    : AggregationPhysicalFunction(std::move(inputType), std::move(resultType), std::move(inputFunction), std::move(resultFieldIdentifier))
    , memProviderPagedVector(std::move(memProviderPagedVector))
{
}

void ArrayAggregationPhysicalFunction::lift(
    const nautilus::val<AggregationState*>& aggregationState, ExecutionContext& executionContext, const Nautilus::Record& record)
{
    std::cerr << "ArrayAggregationPhysicalFunction::lift - Starting lift operation" << std::endl;
    NES_DEBUG("ArrayAggregationPhysicalFunction::lift - Starting lift operation");
    const auto memArea = static_cast<nautilus::val<int8_t*>>(aggregationState);
    Record aggregateStateRecord(
        {{std::string(StateFieldName), inputFunction.execute(record, executionContext.pipelineMemoryProvider.arena)}});
    const Nautilus::Interface::PagedVectorRef pagedVectorRef(memArea, memProviderPagedVector);
    pagedVectorRef.writeRecord(aggregateStateRecord, executionContext.pipelineMemoryProvider.bufferProvider);
    NES_DEBUG("ArrayAggregationPhysicalFunction::lift - Completed lift operation");
    std::cerr << "ArrayAggregationPhysicalFunction::lift - Completed lift operation" << std::endl;
}

void ArrayAggregationPhysicalFunction::combine(
    const nautilus::val<AggregationState*> aggregationState1,
    const nautilus::val<AggregationState*> aggregationState2,
    PipelineMemoryProvider&)
{
    /// Calling the copyFrom function of the paged vector to combine the two paged vectors by copying the content of the second paged vector to the first paged vector
    nautilus::invoke(
        +[](AggregationState* state1, AggregationState* state2) -> void
        {
            /// Reinterpret the aggregation states as PagedVector pointers
            auto* vector1 = reinterpret_cast<Nautilus::Interface::PagedVector*>(state1);
            auto* vector2 = reinterpret_cast<Nautilus::Interface::PagedVector*>(state2);
            vector1->copyFrom(*vector2);
        },
        aggregationState1,
        aggregationState2);
}

Nautilus::Record ArrayAggregationPhysicalFunction::lower(
    const nautilus::val<AggregationState*> aggregationState, PipelineMemoryProvider& pipelineMemoryProvider)
{
    NES_DEBUG("ArrayAggregationPhysicalFunction::lower - Starting lower operation");
    
    /// Get the PagedVector pointer from the aggregation state
    const auto pagedVectorPtr = nautilus::invoke(
        +[](AggregationState* state) -> Nautilus::Interface::PagedVector*
        {
            return reinterpret_cast<Nautilus::Interface::PagedVector*>(state);
        },
        aggregationState);
    const Nautilus::Interface::PagedVectorRef pagedVectorRef(pagedVectorPtr, memProviderPagedVector);
    const auto allFieldNames = memProviderPagedVector->getMemoryLayout()->getSchema().getFieldNames();
    const auto numberOfEntries = invoke(
        +[](const Nautilus::Interface::PagedVector* pagedVector)
        {
            const auto numberOfEntriesVal = pagedVector->getTotalNumberOfEntries();
            return numberOfEntriesVal;
        },
        pagedVectorPtr);
    
    invoke(+[](size_t n) { 
        NES_DEBUG("ArrayAggregationPhysicalFunction::lower - Number of entries: {}", n); 
    }, numberOfEntries);

    // Handle empty array case
    if (numberOfEntries == nautilus::val<size_t>(0))
    {
        NES_DEBUG("ArrayAggregationPhysicalFunction::lower - Empty array case, returning empty result");
        // Allocate minimal space for empty array
        auto variableSized = pipelineMemoryProvider.arena.allocateVariableSizedData(0);
        Nautilus::Record resultRecord;
        resultRecord.write(resultFieldIdentifier, variableSized);
        return resultRecord;
    }

    // Get element size based on input type
    size_t elementSize = 0;
    switch (inputType.type)
    {
        case DataType::Type::UINT8:
            elementSize = sizeof(uint8_t);
            break;
        case DataType::Type::INT8:
            elementSize = sizeof(int8_t);
            break;
        case DataType::Type::UINT16:
            elementSize = sizeof(uint16_t);
            break;
        case DataType::Type::INT16:
            elementSize = sizeof(int16_t);
            break;
        case DataType::Type::UINT32:
            elementSize = sizeof(uint32_t);
            break;
        case DataType::Type::INT32:
            elementSize = sizeof(int32_t);
            break;
        case DataType::Type::UINT64:
            elementSize = sizeof(uint64_t);
            break;
        case DataType::Type::INT64:
            elementSize = sizeof(int64_t);
            break;
        case DataType::Type::FLOAT32:
            elementSize = sizeof(float);
            break;
        case DataType::Type::FLOAT64:
            elementSize = sizeof(double);
            break;
        case DataType::Type::BOOLEAN:
            elementSize = sizeof(bool);
            break;
        default:
            throw std::runtime_error("Unsupported type in ArrayAggregationPhysicalFunction");
    }

    // Calculate total size - multiply nautilus::val with regular size_t
    auto totalSize = numberOfEntries * elementSize;
    invoke(+[](size_t total) { 
        NES_DEBUG("ArrayAggregationPhysicalFunction::lower - Total size for array: {}", total); 
    }, totalSize);
    NES_DEBUG("ArrayAggregationPhysicalFunction::lower - Element size: {}", elementSize);
    
    auto variableSized = pipelineMemoryProvider.arena.allocateVariableSizedData(totalSize);
    auto current = variableSized.getContent();

    invoke(+[](size_t n) { 
        NES_DEBUG("ArrayAggregationPhysicalFunction::lower - Starting loop to copy {} elements", n); 
    }, numberOfEntries);
    
    // Use indexed access to avoid iterator bugs
    for (nautilus::val<size_t> i = 0; i < numberOfEntries; i = i + 1)
    {
        invoke(+[](size_t idx) { 
            NES_DEBUG("ArrayAggregationPhysicalFunction::lower - Processing element {}", idx); 
        }, i);
        
        const auto itemRecord = pagedVectorRef.readRecord(i, allFieldNames);
        const auto itemValue = itemRecord.read(std::string(StateFieldName));
        itemValue.customVisit(
            [&]<typename T>(const T& type) -> VarVal
            {
                if constexpr (std::is_same_v<T, VariableSizedData>)
                {
                    throw std::runtime_error("VariableSizedData is not supported in ArrayAggregationPhysicalFunction");
                }
                else
                {
                    *static_cast<nautilus::val<typename T::raw_type*>>(current) = type;
                    current += sizeof(typename T::raw_type);
                }
                return type;
            });
    }
    
    NES_DEBUG("ArrayAggregationPhysicalFunction::lower - Completed copying all elements");

    Nautilus::Record resultRecord;
    resultRecord.write(resultFieldIdentifier, variableSized);

    NES_DEBUG("ArrayAggregationPhysicalFunction::lower - Completed lower operation, returning result");
    return resultRecord;
}

void ArrayAggregationPhysicalFunction::reset(const nautilus::val<AggregationState*> aggregationState, PipelineMemoryProvider&)
{
    NES_DEBUG("ArrayAggregationPhysicalFunction::reset - Resetting aggregation state");
    nautilus::invoke(
        +[](AggregationState* pagedVectorMemArea) -> void
        {
            /// Ensure proper alignment before placement new
            INVARIANT(reinterpret_cast<uintptr_t>(pagedVectorMemArea) % alignof(Nautilus::Interface::PagedVector) == 0,
                      "PagedVector memory must be properly aligned");
            /// Allocates a new PagedVector in the memory area provided by the pointer to the pagedvector
            auto* pagedVector = reinterpret_cast<Nautilus::Interface::PagedVector*>(pagedVectorMemArea);
            new (pagedVector) Nautilus::Interface::PagedVector();
        },
        aggregationState);
    NES_DEBUG("ArrayAggregationPhysicalFunction::reset - Reset completed");
}

size_t ArrayAggregationPhysicalFunction::getSizeOfStateInBytes() const
{
    // Ensure proper alignment for std::vector inside PagedVector
    constexpr size_t alignment = alignof(std::max_align_t);
    size_t size = sizeof(Nautilus::Interface::PagedVector);
    // Round up to nearest multiple of alignment
    return ((size + alignment - 1) / alignment) * alignment;
}
void ArrayAggregationPhysicalFunction::cleanup(nautilus::val<AggregationState*> aggregationState)
{
    nautilus::invoke(
        +[](AggregationState* pagedVectorMemArea) -> void
        {
            /// Calls the destructor of the PagedVector
            auto* pagedVector = reinterpret_cast<Nautilus::Interface::PagedVector*>(
                pagedVectorMemArea); /// NOLINT(cppcoreguidelines-pro-type-reinterpret-cast)
            pagedVector->~PagedVector();
        },
        aggregationState);
}

AggregationPhysicalFunctionRegistryReturnType AggregationPhysicalFunctionGeneratedRegistrar::RegisterArray_AggAggregationPhysicalFunction(
    AggregationPhysicalFunctionRegistryArguments arguments)
{
    auto memoryLayoutSchema = Schema().addField(std::string(StateFieldName), arguments.inputType);
    auto layout = std::make_shared<Memory::MemoryLayouts::ColumnLayout>(8192, memoryLayoutSchema);
    const std::shared_ptr<Nautilus::Interface::MemoryProvider::TupleBufferMemoryProvider> memoryProvider
        = std::make_shared<Nautilus::Interface::MemoryProvider::ColumnTupleBufferMemoryProvider>(layout);

    return std::make_shared<ArrayAggregationPhysicalFunction>(
        std::move(arguments.inputType),
        std::move(arguments.resultType),
        arguments.inputFunction,
        arguments.resultFieldIdentifier,
        memoryProvider);
}

}
