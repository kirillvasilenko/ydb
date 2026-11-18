#pragma once

#include "data_stats_by_storage.h"
#include "essential_stats.h"

#include <ydb/core/base/blobstorage_grouptype.h>
#include <ydb/core/tx/columnshard/common/path_id.h>
#include <ydb/core/tx/columnshard/counters/engine_logs.h>

#include <ydb/library/accessor/positive_integer.h>

#include <util/generic/refcount.h>

#include <map>
#include <optional>

namespace NKikimrTableStats {
class TTableStats;
}

namespace NKikimr::NOlap {
class IStoragesManager;
class IDbWrapper;
}   // namespace NKikimr::NOlap

namespace NKikimr::NColumnShard {

class TTabletCountersHandle;

// The shard's numbers about its portions: Execute changes the data stats and persists their essentials, Complete the rest
class TShardDataStats {
private:
    const std::shared_ptr<NOlap::IStoragesManager> StoragesManager;
    const ui64 TabletId;
    const TEngineLogsCounters Counters;

    // the rows of the EssentialStats table: path id -> blob storage id -> stats
    THashMap<TInternalPathId, THashMap<TString, TEssentialStats>> Essentials;
    THashSet<std::pair<TInternalPathId, TString>> StorageStatsToPersist;

    THashMap<TInternalPathId, TDataStatsByStorage> Granules;
    TDataStatsByStorage Total;
    THashMap<TString, ui64> SmallBlobThresholds;
    // RAM taken by the metadata of the shard's portions; writes are rejected when the sum over all shards reaches a limit
    i64 PortionsMetadataRamBytes = 0;
    static inline TAtomicCounter AllShardsPortionsMetadataRamBytes = 0;
    // schema version -> portions written with it
    std::map<ui64, TPositiveControlInteger> SchemaVersionsInUse;
    TEngineLogsCounters::TPortionSizeHistograms PortionSizeHistograms;

    // We assume that all the blob storage groups for a database have the same layout (erasure, etc.)
    std::optional<TBlobStorageGroupType> BlobStorageLayout;

    const std::optional<TBlobStorageGroupType>& GetBlobStorageLayout();
    ui64 NormalizeSmallBlobsCount(const ui64 rawCount);
    ui64 GetSmallBlobThreshold(const TString& storageId);
    void AddToGroup(const NOlap::TPortionInfo& portion, const EPortionGroup group);
    void RemoveFromGroup(const NOlap::TPortionInfo& portion, const EPortionGroup group);
    const TEssentialStats& GetEssentials(const TInternalPathId pathId, const TString& storageId) const;
    void ChangeEssentials(const NOlap::TPortionInfo& portion, const bool add);
    void DoAddPortionOnComplete(const std::shared_ptr<NOlap::TPortionInfo>& portion, const EPortionGroup group);
    void DoRemovePortionOnComplete(const std::shared_ptr<NOlap::TPortionInfo>& portion, const EPortionGroup group);

public:
    TShardDataStats(const std::shared_ptr<NOlap::IStoragesManager>& storagesManager, const ui64 tabletId);
    ~TShardDataStats();

    // Execute: a portion row is written or erased; PersistEssentials writes the changed rows before the Execute ends
    void AddPortionOnExecute(const NOlap::TPortionInfo& portion, const EPortionGroup group);
    void RemovePortionOnExecute(const NOlap::TPortionInfo& portion, const EPortionGroup group);
    // Execute: the row changes neither the portion's sizes nor its storage
    void ChangeGroupOnExecute(const NOlap::TPortionInfo& portion, const EPortionGroup from, const EPortionGroup to);
    void PersistEssentials(NOlap::IDbWrapper& db);

    // a portion read from the database: its row is in the essentials already
    void AddPortionOnLoad(const std::shared_ptr<NOlap::TPortionInfo>& portion, const EPortionGroup group);

    // Complete: the portion enters or leaves the memory
    void AddPortionOnComplete(const std::shared_ptr<NOlap::TPortionInfo>& portion, const EPortionGroup group);
    void RemovePortionOnComplete(const std::shared_ptr<NOlap::TPortionInfo>& portion, const EPortionGroup group);

    bool LoadEssentials(NOlap::IDbWrapper& db);
    // Execute, every portion loaded: the essentials become the group stats
    void RecalculateEssentials(NOlap::IDbWrapper& db);

    const TDataStatsByStorage& GetGranuleStats(const TInternalPathId pathId) const;

    const TStorageStats& GetStorageStats(const TString& storageId) const {
        return Total.GetStorageStats(storageId);
    }

    bool HasSchemaVersion(const ui64 fromVersion, const ui64 version) const;

    static ui64 GetAllShardsPortionsMetadataRamBytes() {
        return AllShardsPortionsMetadataRamBytes.Val();
    }

    void FillTableEssentials(const TInternalPathId pathId, NKikimrTableStats::TTableStats& tableStats) const;
    // also counts the small blobs waiting for deletion
    void FillTotalEssentials(NKikimrTableStats::TTableStats& tableStats);
    void PublishShardCounters(const TTabletCountersHandle& counters) const;
    ui64 GetBlobStorageBytes() const;
};

}   // namespace NKikimr::NColumnShard
