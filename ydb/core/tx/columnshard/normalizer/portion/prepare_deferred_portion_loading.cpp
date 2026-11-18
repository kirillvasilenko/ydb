#include "prepare_deferred_portion_loading.h"

#include <ydb/core/tx/columnshard/engines/column_engine_logs.h>
#include <ydb/core/tx/columnshard/engines/db_wrapper.h>
#include <ydb/core/tx/columnshard/engines/index_portions_by_last_key.h>
#include <ydb/core/tx/columnshard/engines/portions/constructor_portion.h>
#include <ydb/core/tx/columnshard/tables_manager.h>

namespace NKikimr::NOlap::NPrepareDeferredPortionLoading {

namespace {

constexpr ui64 PortionsPerTask = 1000;

class TEmptyIndexTable: public INormalizerChanges {
private:
    const std::vector<TNameTypeInfo> PkColumns;

public:
    TEmptyIndexTable(const std::vector<TNameTypeInfo>& pkColumns)
        : PkColumns(pkColumns)
    {
    }

    virtual bool ApplyOnExecute(NTabletFlatExecutor::TTransactionContext& txc, const TNormalizationController&) const override {
        if (TIndexPortionsByLastKey::Exists(txc.DB)) {
            TIndexPortionsByLastKey::Truncate(txc.DB);
        } else {
            TIndexPortionsByLastKey::Create(txc.DB, PkColumns);
        }
        return true;
    }

    virtual ui64 GetSize() const override {
        return 1;
    }
};

class TFillIndexTable: public INormalizerChanges {
private:
    const std::vector<TPortionInfo::TPtr> Portions;

public:
    TFillIndexTable(std::vector<TPortionInfo::TPtr>&& portions)
        : Portions(std::move(portions))
    {
    }

    virtual bool ApplyOnExecute(NTabletFlatExecutor::TTransactionContext& txc, const TNormalizationController&) const override {
        for (const auto& portion : Portions) {
            TIndexPortionsByLastKey::Upsert(txc.DB, *portion);
        }
        return true;
    }

    virtual ui64 GetSize() const override {
        return Portions.size();
    }
};

std::vector<INormalizerTask::TPtr> BuildIndexTableTasks(
    const std::vector<TNameTypeInfo>& pkColumns, const std::vector<TPortionInfo::TPtr>& portions) {
    std::vector<INormalizerTask::TPtr> tasks;
    tasks.emplace_back(std::make_shared<TTrivialNormalizerTask>(std::make_shared<TEmptyIndexTable>(pkColumns)));
    for (ui64 begin = 0; begin < portions.size(); begin += PortionsPerTask) {
        const ui64 end = std::min<ui64>(begin + PortionsPerTask, portions.size());
        tasks.emplace_back(std::make_shared<TTrivialNormalizerTask>(
            std::make_shared<TFillIndexTable>(std::vector<TPortionInfo::TPtr>(portions.begin() + begin, portions.begin() + end))));
    }
    return tasks;
}

bool LoadPortions(NTabletFlatExecutor::TTransactionContext& txc, const TVersionedIndex& versionedIndex,
    const IBlobGroupSelector& dsGroupSelector, std::vector<TPortionInfo::TPtr>& portions) {
    TDbWrapper wrapper(txc.DB, nullptr);
    return wrapper.LoadPortions(
        [&](std::unique_ptr<TPortionInfoConstructor>&& portion, const NKikimrTxColumnShard::TIndexPortionMeta& metaProto) {
            const TIndexInfo& indexInfo = portion->GetSchema(versionedIndex)->GetIndexInfo();
            YDB_LOG_VERIFY(portion->MutableMeta().LoadMetadata(metaProto, indexInfo, dsGroupSelector));
            portions.emplace_back(portion->Build());
            return true;
        });
}

}   // namespace

TConclusion<std::vector<INormalizerTask::TPtr>> TNormalizer::DoInit(
    const TNormalizationController& controller, NTabletFlatExecutor::TTransactionContext& txc) {
    using namespace NColumnShard;
    NIceDb::TNiceDb db(txc.DB);
    if (!Schema::Precharge<Schema::IndexPortions>(db, txc.DB.GetScheme())) {
        return TConclusionStatus::Fail("Not ready");
    }

    TTablesManager tablesManager(controller.GetStoragesManager(), controller.GetDataAccessorsManager(),
        std::make_shared<TShardDataStats>(controller.GetStoragesManager(), 0), 0);
    if (!tablesManager.InitFromDB(db, nullptr)) {
        return TConclusionStatus::Fail("Not ready");
    }
    if (!tablesManager.HasPrimaryIndex()) {
        return std::vector<INormalizerTask::TPtr>();
    }

    const auto& versionedIndex = tablesManager.GetPrimaryIndexAsVerified<TColumnEngineForLogs>().GetVersionedIndex();
    std::vector<TPortionInfo::TPtr> portions;
    if (!LoadPortions(txc, versionedIndex, DsGroupSelector, portions)) {
        return TConclusionStatus::Fail("Not ready");
    }
    return BuildIndexTableTasks(versionedIndex.GetLastSchema()->GetIndexInfo().GetPrimaryKeyColumns(), portions);
}

}   // namespace NKikimr::NOlap::NPrepareDeferredPortionLoading
