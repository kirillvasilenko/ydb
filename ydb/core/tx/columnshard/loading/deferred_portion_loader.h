#pragma once
#include <ydb/core/scheme/scheme_tablecell.h>
#include <ydb/core/tablet_flat/tablet_flat_executor.h>
#include <ydb/core/tx/columnshard/common/path_id.h>
#include <ydb/core/tx/columnshard/data_locks/manager/manager.h>
#include <ydb/core/tx/columnshard/engines/portions/portion_info.h>

#include <util/generic/noncopyable.h>

#include <deque>
#include <optional>
#include <vector>

namespace NKikimr::NOlap {
class IBlobGroupSelector;
}

namespace NKikimr::NColumnShard {

class TTablesManager;

// DeferredPortionLoadingSettings.Enabled: TTxInit leaves the portions on disk, they are loaded in batches after the start
class TDeferredPortionLoader: private TNonCopyable {
public:
    struct TBatch {
        TInternalPathId PathId;
        std::vector<NOlap::TPortionInfo::TPtr> Portions;
        // nullopt: nothing of the table is left below the batch
        std::optional<TOwnedCellVec> LastKey;
    };

private:
    TTablesManager& TablesManager;
    NOlap::NDataLocks::TManager& DataLocksManager;
    std::shared_ptr<NOlap::NDataLocks::TManager::TGuard> CompactionLock;
    bool Enabled = false;
    bool BatchInFlight = false;
    ui32 BatchesLoaded = 0;
    // round-robin: the front table gets the next batch
    std::deque<TInternalPathId> TablesToLoad;
    // absent: the next batch of the table starts from the top
    THashMap<TInternalPathId, TOwnedCellVec> NextBatchBelow;

    void ReleaseCompactionLock();

public:
    TDeferredPortionLoader(TTablesManager& tablesManager, NOlap::NDataLocks::TManager& dataLocksManager)
        : TablesManager(tablesManager)
        , DataLocksManager(dataLocksManager)
    {
    }

    ~TDeferredPortionLoader();

    // TTxInit, before the engine builds its loader; false: the engine loads every portion in TTxInit
    bool EnableIfSupported();

    bool IsEnabled() const {
        return Enabled;
    }

    // after the switch to work; false: every table is loaded already
    bool Start();
    // false: a batch is in flight, nothing is left, or a test holds the loading
    bool TryStartBatch();
    // TTxLoadPortionsBatch::Execute; false: pages are missing, run it again in the next tx
    bool ReadBatch(NTabletFlatExecutor::TTransactionContext& txc, const NOlap::IBlobGroupSelector& dsGroupSelector, TBatch& batch) const;
    // TTxLoadPortionsBatch::Complete; true: every table is loaded
    bool OnBatchLoaded(TBatch&& batch);

    ui32 GetBatchesLoaded() const {
        return BatchesLoaded;
    }
};

}   // namespace NKikimr::NColumnShard
