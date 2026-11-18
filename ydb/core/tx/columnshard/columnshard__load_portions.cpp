#include "columnshard_impl.h"

#include "engines/column_engine_logs.h"
#include "hooks/abstract/abstract.h"

namespace NKikimr::NColumnShard {

class TTxLoadPortionsBatch: public TTransactionBase<TColumnShard> {
private:
    const TBlobGroupSelector DsGroupSelector;
    TDeferredPortionLoader::TBatch Batch;

public:
    TTxLoadPortionsBatch(TColumnShard* self)
        : TBase(self)
        , DsGroupSelector(self->Info())
    {
    }

    bool Execute(TTransactionContext& txc, const TActorContext& /*ctx*/) override {
        return Self->DeferredPortionLoader.ReadBatch(txc, DsGroupSelector, Batch);
    }

    void Complete(const TActorContext& ctx) override {
        Self->OnPortionsBatchLoaded(std::move(Batch), ctx);
    }

    TTxType GetTxType() const override {
        return TXTYPE_LOAD_PORTIONS_BATCH;
    }
};

class TTxRecalculateEssentialStats: public TTransactionBase<TColumnShard> {
public:
    using TBase::TBase;

    bool Execute(TTransactionContext& txc, const TActorContext& /*ctx*/) override {
        txc.DB.NoMoreReadsForTx();
        NOlap::TDbWrapper db(txc.DB, nullptr);
        Self->DataStats->RecalculateEssentials(db);
        return true;
    }

    void Complete(const TActorContext& /*ctx*/) override {
    }

    TTxType GetTxType() const override {
        return TXTYPE_RECALCULATE_ESSENTIAL_STATS;
    }
};

void TColumnShard::StartDeferredPortionLoading(const TActorContext& ctx) {
    if (!DeferredPortionLoader.Start()) {
        OnAllPortionsLoaded(ctx);
        return;
    }
    ContinueDeferredPortionLoading(ctx);
}

void TColumnShard::ContinueDeferredPortionLoading(const TActorContext& ctx) {
    if (DeferredPortionLoader.TryStartBatch()) {
        Execute(new TTxLoadPortionsBatch(this), ctx);
    }
}

void TColumnShard::OnPortionsBatchLoaded(TDeferredPortionLoader::TBatch&& batch, const TActorContext& ctx) {
    const bool allLoaded = DeferredPortionLoader.OnBatchLoaded(std::move(batch));
    NYDBTest::TControllers::GetColumnShardController()->OnPortionsBatchLoaded(TabletID());
    ScanGate.ReleaseLoadedPortions();
    DrainWriteQueue(ctx);
    if (allLoaded) {
        OnAllPortionsLoaded(ctx);
    } else {
        ContinueDeferredPortionLoading(ctx);
    }
}

void TColumnShard::OnAllPortionsLoaded(const TActorContext& ctx) {
    PublishShardCounters();
    Execute(new TTxRecalculateEssentialStats(this), ctx);
    TryStartFindEmptyHistoryIntervals(ctx);
    ctx.Send(SelfId(), new TEvPrivate::TEvPrefetchPortionsMetadata());
    ui64 portionsCount = 0;
    for (auto&& [_, granule] : TablesManager.GetPrimaryIndexAsVerified<NOlap::TColumnEngineForLogs>().GetTables()) {
        portionsCount += granule->GetPortions().size();
    }
    StartupTimeline.OnAllPortionsLoaded(portionsCount, DeferredPortionLoader.GetBatchesLoaded());
    EnqueueBackgroundActivities();
}

}   // namespace NKikimr::NColumnShard
