#pragma once

#include <ydb/core/tx/columnshard/columnshard_schema.h>
#include <ydb/core/tx/columnshard/defs.h>
#include <ydb/core/tx/columnshard/normalizer/abstract/abstract.h>

namespace NKikimr::NOlap::NPrepareDeferredPortionLoading {

class TNormalizer: public TNormalizationController::INormalizerComponent {
private:
    using TBase = TNormalizationController::INormalizerComponent;

public:
    static TString GetClassNameStatic() {
        return ::ToString(ENormalizerSequentialId::PrepareDeferredPortionLoading);
    }

    virtual std::optional<ENormalizerSequentialId> DoGetEnumSequentialId() const override {
        return ENormalizerSequentialId::PrepareDeferredPortionLoading;
    }

    virtual TString GetClassName() const override {
        return GetClassNameStatic();
    }

    static inline INormalizerComponent::TFactory::TRegistrator<TNormalizer> Registrator =
        INormalizerComponent::TFactory::TRegistrator<TNormalizer>(GetClassNameStatic());

public:
    TNormalizer(const TNormalizationController::TInitContext& info)
        : TBase(info)
        , DsGroupSelector(info.GetStorageInfo())
    {
    }

    virtual TConclusion<std::vector<INormalizerTask::TPtr>> DoInit(
        const TNormalizationController& controller, NTabletFlatExecutor::TTransactionContext& txc) override;

private:
    NColumnShard::TBlobGroupSelector DsGroupSelector;
};

}   // namespace NKikimr::NOlap::NPrepareDeferredPortionLoading
