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
    const auto memArea = static_cast<nautilus::val<int8_t*>>(aggregationState);
    Record aggregateStateRecord(
        {{std::string(StateFieldName), inputFunction.execute(record, executionContext.pipelineMemoryProvider.arena)}});
    const Nautilus::Interface::PagedVectorRef pagedVectorRef(memArea, memProviderPagedVector);
    pagedVectorRef.writeRecord(aggregateStateRecord, executionContext.pipelineMemoryProvider.bufferProvider);
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

    // Handle empty array case
    if (numberOfEntries == nautilus::val<size_t>(0))
    {
        // Allocate minimal space for empty array
        auto variableSized = pipelineMemoryProvider.arena.allocateVariableSizedData(0);
        Nautilus::Record resultRecord;
        resultRecord.write(resultFieldIdentifier, variableSized);
        return resultRecord;
    }

    // First, count the actual number of elements and calculate total size
    size_t totalSize = 0;
    size_t elementCount = 0;
    
    // Count elements and calculate size
    const auto countEndIt = pagedVectorRef.end(allFieldNames);
    for (auto candidateIt = pagedVectorRef.begin(allFieldNames); candidateIt != countEndIt; ++candidateIt)
    {
        const auto itemRecord = *candidateIt;
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
                    totalSize += sizeof(typename T::raw_type);
                    elementCount++;
                }
                return type;
            });
    }

    auto variableSized = pipelineMemoryProvider.arena.allocateVariableSizedData(totalSize);

    /// Copy from paged vector - create new iterators for second pass
    auto current = variableSized.getContent();
    
    // Second iteration to actually copy the data
    const auto copyEndIt = pagedVectorRef.end(allFieldNames);
    for (auto candidateIt = pagedVectorRef.begin(allFieldNames); candidateIt != copyEndIt; ++candidateIt)
    {
        const auto itemRecord = *candidateIt;
        const auto itemValue = itemRecord.read(std::string(StateFieldName));
        auto _ = itemValue.customVisit(
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

    Nautilus::Record resultRecord;
    resultRecord.write(resultFieldIdentifier, variableSized);

    return resultRecord;
}

void ArrayAggregationPhysicalFunction::reset(const nautilus::val<AggregationState*> aggregationState, PipelineMemoryProvider&)
{
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
