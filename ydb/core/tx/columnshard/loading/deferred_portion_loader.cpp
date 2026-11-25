#include "deferred_portion_loader.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/tx/columnshard/columnshard_schema.h>
#include <ydb/core/tx/columnshard/engines/column_engine_logs.h>
#include <ydb/core/tx/columnshard/engines/db_wrapper.h>
#include <ydb/core/tx/columnshard/engines/index_portions_by_last_key.h>
#include <ydb/core/tx/columnshard/hooks/abstract/abstract.h>
#include <ydb/core/tx/columnshard/tables_manager.h>

namespace NKikimr::NColumnShard {

namespace {

using NOlap::NDataLocks::ELockCategory;

class TDeferredLoadingLock final: public NOlap::NDataLocks::ILock {
private:
    const NOlap::TColumnEngineForLogs& Engine;

    std::optional<TString> DoIsLocked(const NOlap::TPortionInfo& portion, ELockCategory category, const THashSet<TString>&) const override {
        if (category == ELockCategory::Compaction && portion.GetMeta().GetDeletionsCount()) {
            const auto& loaded = Engine.GetGranuleVerified(portion.GetPathId()).GetLoadedKeyRanges();
            if (!loaded.CoversAll() && !loaded.Covers(portion.IndexKeyStart(), portion.IndexKeyEnd())) {
                return GetLockName();
            }
        }
        return std::nullopt;
    }

    std::optional<TString> DoIsLocked(const NOlap::TGranuleMeta&, ELockCategory, const THashSet<TString>&) const override {
        return std::nullopt;
    }

    bool DoIsEmpty() const override {
        return false;
    }

public:
    explicit TDeferredLoadingLock(const NOlap::TColumnEngineForLogs& engine)
        : ILock("DeferredPortionLoading", ELockCategory::Compaction)
        , Engine(engine)
    {
    }
};

}   // namespace

TDeferredPortionLoader::~TDeferredPortionLoader() {
    ReleaseCompactionLock();
}

void TDeferredPortionLoader::ReleaseCompactionLock() {
    if (CompactionLock) {
        CompactionLock->Release(DataLocksManager);
        CompactionLock.reset();
    }
}

bool TDeferredPortionLoader::EnableIfSupported() {
    Enabled = HasAppData() && AppDataVerified().ColumnShardConfig.GetDeferredPortionLoadingSettings().GetEnabled();
    const auto& engine = TablesManager.GetPrimaryIndexAsVerified<NOlap::TColumnEngineForLogs>();
    for (const auto& [_, granule] : engine.GetTables()) {
        // in_mem reads the column chunks at the start, so it needs all the portions loaded right away
        Enabled &= !granule->LoadsChunksAtStart();
    }
    if (Enabled) {
        CompactionLock = DataLocksManager.RegisterLock<TDeferredLoadingLock>(engine);
    }
    return Enabled;
}

bool TDeferredPortionLoader::Start() {
    YDB_LOG_VERIFY(Enabled);
    YDB_LOG_VERIFY(CompactionLock);
    YDB_LOG_VERIFY(TablesToLoad.empty());
    const auto& engine = TablesManager.GetPrimaryIndexAsVerified<NOlap::TColumnEngineForLogs>();
    for (const auto& [pathId, granule] : engine.GetTables()) {
        if (!granule->GetLoadedKeyRanges().CoversAll()) {
            TablesToLoad.emplace_back(pathId);
        }
    }
    if (TablesToLoad.empty()) {
        ReleaseCompactionLock();
        return false;
    }
    return true;
}

bool TDeferredPortionLoader::TryStartBatch() {
    if (BatchInFlight || TablesToLoad.empty()) {
        return false;
    }
    // a test holds the loading here; the periodic wakeup resumes it
    if (!NYDBTest::TControllers::GetColumnShardController()->IsBackgroundEnabled(NYDBTest::ICSController::EBackground::PortionLoading)) {
        return false;
    }
    BatchInFlight = true;
    return true;
}

bool TDeferredPortionLoader::ReadBatch(
    NTabletFlatExecutor::TTransactionContext& txc, const NOlap::IBlobGroupSelector& dsGroupSelector, TBatch& batch) const {
    YDB_LOG_VERIFY(BatchInFlight);
    const auto& engine = TablesManager.GetPrimaryIndexAsVerified<NOlap::TColumnEngineForLogs>();
    batch.PathId = TablesToLoad.front();
    batch.Portions.clear();
    const auto below = NextBatchBelow.find(batch.PathId);
    std::vector<ui64> portionIds;
    const ui32 rowsLimit = std::max<ui32>(1, AppDataVerified().ColumnShardConfig.GetDeferredPortionLoadingSettings().GetPortionsPerBatch());
    if (!NOlap::TIndexPortionsByLastKey::ReadBatch(txc.DB, batch.PathId,
            below == NextBatchBelow.end() ? std::nullopt : std::optional<TOwnedCellVec>(below->second), rowsLimit, portionIds, batch.LastKey)) {
        return false;
    }
    NIceDb::TNiceDb db(txc.DB);
    // every row is touched before the restart, so that one restart requests all missing pages
    bool ready = true;
    for (const ui64 portionId : portionIds) {
        ready &= db.Table<Schema::IndexPortions>().Key(batch.PathId.GetRawValue(), portionId).Select().IsReady();
    }
    if (!ready) {
        return false;
    }
    NOlap::TDbWrapper wrapper(txc.DB, &dsGroupSelector);
    const NOlap::TVersionedIndex& versionedIndex = engine.GetVersionedIndex();
    for (const ui64 portionId : portionIds) {
        YDB_LOG_VERIFY(wrapper.LoadPortionVerified(NOlap::TPortionAddress(batch.PathId, portionId),
            [&](std::unique_ptr<NOlap::TPortionInfoConstructor>&& portion, const NKikimrTxColumnShard::TIndexPortionMeta& metaProto) {
                const NOlap::TIndexInfo& indexInfo = portion->GetSchema(versionedIndex)->GetIndexInfo();
                YDB_LOG_VERIFY(portion->MutableMeta().LoadMetadata(metaProto, indexInfo, dsGroupSelector));
                batch.Portions.emplace_back(portion->Build());
            }));
    }
    return true;
}

bool TDeferredPortionLoader::OnBatchLoaded(TBatch&& batch) {
    YDB_LOG_VERIFY(BatchInFlight);
    YDB_LOG_VERIFY(!TablesToLoad.empty() && TablesToLoad.front() == batch.PathId,
        {"pathId", batch.PathId});
    BatchInFlight = false;
    ++BatchesLoaded;
    TablesToLoad.pop_front();
    auto& engine = TablesManager.MutablePrimaryIndexAsVerified<NOlap::TColumnEngineForLogs>();
    if (!batch.LastKey) {
        engine.AddPortionsLoadedInBatch(batch.PathId, batch.Portions, NOlap::TLoadedKeyRanges::BuildEverything());
        NextBatchBelow.erase(batch.PathId);
    } else {
        // the batch ends with every portion whose last key is LastKey
        YDB_LOG_VERIFY(!batch.Portions.empty());
        engine.AddPortionsLoadedInBatch(
            batch.PathId, batch.Portions, NOlap::TLoadedKeyRanges::BuildFrom(batch.Portions.back()->GetMeta().IndexKeyEnd()));
        NextBatchBelow.insert_or_assign(batch.PathId, std::move(*batch.LastKey));
        TablesToLoad.emplace_back(batch.PathId);
    }
    if (TablesToLoad.empty()) {
        ReleaseCompactionLock();
        return true;
    }
    return false;
}

}   // namespace NKikimr::NColumnShard
