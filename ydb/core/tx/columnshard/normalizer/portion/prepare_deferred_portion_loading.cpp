#include "prepare_deferred_portion_loading.h"

#include <ydb/core/protos/tx_columnshard.pb.h>
#include <ydb/core/tx/columnshard/columnshard_schema.h>
#include <ydb/core/tx/columnshard/engines/column_engine_logs.h>
#include <ydb/core/tx/columnshard/engines/db_wrapper.h>
#include <ydb/core/tx/columnshard/engines/index_portions_by_last_key.h>
#include <ydb/core/tx/columnshard/engines/portions/constructor_portion.h>
#include <ydb/core/tx/columnshard/engines/portions/written.h>
#include <ydb/core/tx/columnshard/engines/storage/data_stats/shard_data_stats.h>
#include <ydb/core/tx/columnshard/engines/storage/granule/granule.h>
#include <ydb/core/tx/columnshard/tables_manager.h>

namespace NKikimr::NOlap::NPrepareDeferredPortionLoading {

namespace {

constexpr ui64 RowsPerTask = 1000;

struct TOperationRow {
    ui64 WriteId = 0;
    NKikimrTxColumnShard::TInternalOperationData Metadata;
};

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

class TSaveLastInsertWriteId: public INormalizerChanges {
private:
    const ui64 LastInsertWriteId;

public:
    TSaveLastInsertWriteId(const ui64 lastInsertWriteId)
        : LastInsertWriteId(lastInsertWriteId)
    {
    }

    virtual bool ApplyOnExecute(NTabletFlatExecutor::TTransactionContext& txc, const TNormalizationController&) const override {
        TDbWrapper(txc.DB, nullptr).WriteCounter(TColumnEngineForLogs::LAST_INSERT_WRITE_ID, LastInsertWriteId);
        return true;
    }

    virtual ui64 GetSize() const override {
        return 1;
    }
};

class TSaveOperations: public INormalizerChanges {
private:
    const std::vector<TOperationRow> Operations;

public:
    TSaveOperations(std::vector<TOperationRow>&& operations)
        : Operations(std::move(operations))
    {
    }

    virtual bool ApplyOnExecute(NTabletFlatExecutor::TTransactionContext& txc, const TNormalizationController&) const override {
        using namespace NColumnShard;
        NIceDb::TNiceDb db(txc.DB);
        for (const auto& operation : Operations) {
            db.Table<Schema::Operations>()
                .Key(operation.WriteId)
                .Update(NIceDb::TUpdate<Schema::Operations::Metadata>(operation.Metadata.SerializeAsString()));
        }
        return true;
    }

    virtual ui64 GetSize() const override {
        return Operations.size();
    }
};

class TTruncateEssentialStats: public INormalizerChanges {
public:
    virtual bool ApplyOnExecute(NTabletFlatExecutor::TTransactionContext& txc, const TNormalizationController&) const override {
        txc.DB.Truncate(NColumnShard::Schema::EssentialStats::TableId);
        return true;
    }

    virtual ui64 GetSize() const override {
        return 1;
    }
};

class TRecalculateEssentials: public INormalizerChanges {
private:
    const std::shared_ptr<NColumnShard::TShardDataStats> DataStats;

public:
    TRecalculateEssentials(const std::shared_ptr<NColumnShard::TShardDataStats>& dataStats)
        : DataStats(dataStats)
    {
    }

    virtual bool ApplyOnExecute(NTabletFlatExecutor::TTransactionContext& txc, const TNormalizationController&) const override {
        TDbWrapper wrapper(txc.DB, nullptr);
        DataStats->RecalculateEssentials(wrapper);
        return true;
    }

    virtual ui64 GetSize() const override {
        return 1;
    }
};

template <class TChanges, class TRow>
void AddTasks(std::vector<INormalizerTask::TPtr>& tasks, const std::vector<TRow>& rows) {
    for (ui64 begin = 0; begin < rows.size(); begin += RowsPerTask) {
        const ui64 end = std::min<ui64>(begin + RowsPerTask, rows.size());
        tasks.emplace_back(
            std::make_shared<TTrivialNormalizerTask>(std::make_shared<TChanges>(std::vector<TRow>(rows.begin() + begin, rows.begin() + end))));
    }
}

std::vector<INormalizerTask::TPtr> BuildIndexTableTasks(
    const std::vector<TNameTypeInfo>& pkColumns, const std::vector<TPortionInfo::TPtr>& portions) {
    std::vector<INormalizerTask::TPtr> tasks;
    tasks.emplace_back(std::make_shared<TTrivialNormalizerTask>(std::make_shared<TEmptyIndexTable>(pkColumns)));
    AddTasks<TFillIndexTable>(tasks, portions);
    return tasks;
}

ui64 GetMaxInsertWriteId(const std::vector<TPortionInfo::TPtr>& portions) {
    ui64 result = 0;
    for (const auto& portion : portions) {
        if (portion->GetPortionType() == EPortionType::Written) {
            result = std::max(result, (ui64) static_cast<const TWrittenPortionInfo&>(*portion).GetInsertWriteId());
        }
    }
    return result;
}

bool LoadLastInsertWriteId(NTabletFlatExecutor::TTransactionContext& txc, ui64& lastInsertWriteId) {
    return TDbWrapper(txc.DB, nullptr).LoadCounters([&](const ui32 id, const ui64 value) {
        if (id == TColumnEngineForLogs::LAST_INSERT_WRITE_ID) {
            lastInsertWriteId = value;
        }
    });
}

void FillOperationPortionIds(std::vector<TOperationRow>& operations, const std::vector<TPortionInfo::TPtr>& portions) {
    // insert write ids were unique only within a table before C5
    THashMap<std::pair<ui64, ui64>, ui64> uncommittedPortionIds;
    for (const auto& portion : portions) {
        if (portion->GetPortionType() == EPortionType::Written && !portion->IsCommitted()) {
            const auto& written = static_cast<const TWrittenPortionInfo&>(*portion);
            uncommittedPortionIds.emplace(
                std::make_pair(written.GetPathId().GetRawValue(), (ui64)written.GetInsertWriteId()), written.GetPortionId());
        }
    }
    for (auto& operation : operations) {
        const ui64 pathId = TInternalPathId::FromProto(operation.Metadata).GetRawValue();
        operation.Metadata.ClearPortionIds();
        for (const ui64 insertWriteId : operation.Metadata.GetInternalWriteIds()) {
            const auto it = uncommittedPortionIds.find(std::make_pair(pathId, insertWriteId));
            YDB_LOG_VERIFY(it != uncommittedPortionIds.end(),
                {"writeId", operation.WriteId},
                {"pathId", pathId},
                {"insertWriteId", insertWriteId});
            operation.Metadata.AddPortionIds(it->second);
        }
    }
}

bool LoadOperations(NTabletFlatExecutor::TTransactionContext& txc, std::vector<TOperationRow>& operations) {
    using namespace NColumnShard;
    NIceDb::TNiceDb db(txc.DB);
    auto rowset = db.Table<Schema::Operations>().Select();
    if (!rowset.IsReady()) {
        return false;
    }
    while (!rowset.EndOfSet()) {
        auto& operation = operations.emplace_back();
        operation.WriteId = rowset.GetValue<Schema::Operations::WriteId>();
        YDB_LOG_VERIFY(operation.Metadata.ParseFromString(rowset.GetValue<Schema::Operations::Metadata>()));
        if (!rowset.Next()) {
            return false;
        }
    }
    return true;
}

void AddPortionsToStats(const std::vector<TPortionInfo::TPtr>& portions, NColumnShard::TShardDataStats& dataStats) {
    for (const auto& portion : portions) {
        dataStats.AddPortionOnLoad(
            portion, portion->IsCommitted() ? TGranuleMeta::GetCommittedPortionGroup(*portion) : NColumnShard::EPortionGroup::Uncommitted);
    }
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
    bool ready = true;
    ready = ready & Schema::Precharge<Schema::IndexPortions>(db, txc.DB.GetScheme());
    ready = ready & Schema::Precharge<Schema::Operations>(db, txc.DB.GetScheme());
    if (!ready) {
        return TConclusionStatus::Fail("Not ready");
    }

    const auto dataStats = std::make_shared<TShardDataStats>(controller.GetStoragesManager(), TabletId);
    TTablesManager tablesManager(controller.GetStoragesManager(), controller.GetDataAccessorsManager(), dataStats, 0);
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
    ui64 lastInsertWriteId = 0;
    if (!LoadLastInsertWriteId(txc, lastInsertWriteId)) {
        return TConclusionStatus::Fail("Not ready");
    }
    std::vector<TOperationRow> operations;
    if (!LoadOperations(txc, operations)) {
        return TConclusionStatus::Fail("Not ready");
    }

    auto tasks = BuildIndexTableTasks(versionedIndex.GetLastSchema()->GetIndexInfo().GetPrimaryKeyColumns(), portions);
    tasks.emplace_back(std::make_shared<TTrivialNormalizerTask>(
        std::make_shared<TSaveLastInsertWriteId>(std::max(lastInsertWriteId, GetMaxInsertWriteId(portions)))));
    FillOperationPortionIds(operations, portions);
    AddTasks<TSaveOperations>(tasks, operations);
    AddPortionsToStats(portions, *dataStats);
    tasks.emplace_back(std::make_shared<TTrivialNormalizerTask>(std::make_shared<TTruncateEssentialStats>()));
    tasks.emplace_back(std::make_shared<TTrivialNormalizerTask>(std::make_shared<TRecalculateEssentials>(dataStats)));
    return tasks;
}

}   // namespace NKikimr::NOlap::NPrepareDeferredPortionLoading
