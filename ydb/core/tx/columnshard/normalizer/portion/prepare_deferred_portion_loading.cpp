#include "prepare_deferred_portion_loading.h"

#include <ydb/core/tx/columnshard/engines/column_engine_logs.h>
#include <ydb/core/tx/columnshard/engines/db_wrapper.h>
#include <ydb/core/tx/columnshard/engines/portions/constructor_portion.h>
#include <ydb/core/tx/columnshard/tables_manager.h>

namespace NKikimr::NOlap::NPrepareDeferredPortionLoading {

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
    TDbWrapper wrapper(txc.DB, nullptr);
    if (!wrapper.LoadPortions([&](std::unique_ptr<TPortionInfoConstructor>&& portion, const NKikimrTxColumnShard::TIndexPortionMeta& metaProto) {
            const TIndexInfo& indexInfo = portion->GetSchema(versionedIndex)->GetIndexInfo();
            YDB_LOG_VERIFY(portion->MutableMeta().LoadMetadata(metaProto, indexInfo, DsGroupSelector));
            portions.emplace_back(portion->Build());
            return true;
        })) {
        return TConclusionStatus::Fail("Not ready");
    }
    return std::vector<INormalizerTask::TPtr>();
}

}   // namespace NKikimr::NOlap::NPrepareDeferredPortionLoading
