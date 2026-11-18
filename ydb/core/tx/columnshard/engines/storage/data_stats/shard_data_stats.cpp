#include "shard_data_stats.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/protos/config.pb.h>
#include <ydb/core/protos/table_stats.pb.h>
#include <ydb/core/tx/columnshard/blobs_action/abstract/storages_manager.h>
#include <ydb/core/tx/columnshard/counters/tablet_counters.h>
#include <ydb/core/tx/columnshard/engines/db_wrapper.h>
#include <ydb/core/tx/columnshard/engines/portions/portion_info.h>

#include <ydb/library/actors/core/log.h>

namespace NKikimr::NColumnShard {

namespace {

const TString& GetBlobStorageId() {
    return NOlap::NBlobOperations::TGlobal::DefaultStorageId;
}

// only the blob storages are persisted; the tiers are not
bool IsBlobStorage(const TString& storageId) {
    return storageId == GetBlobStorageId();
}

void FillPortionStats(NKikimrTableStats::TTableStats& to, const ui64 rows, const ui64 blobBytes, const NOlap::TSmallBlobsStat& smallBlobs) {
    to.SetRowCount(rows);
    to.SetDataSize(blobBytes);
    to.SetSmallBlobsVolumeBytes(smallBlobs.VolumeBytes);
    to.SetSmallBlobsCount(smallBlobs.Count);
}

void SetPortionCounters(const TTabletCountersHandle& counters, const NOlap::TSimplePortionsGroupInfo& portions, const ESimpleCounters count,
    const ESimpleCounters rows, const ESimpleCounters bytes, const ESimpleCounters rawBytes) {
    counters.SetCounter(count, portions.GetCount());
    counters.SetCounter(rows, portions.GetRecordsCount());
    counters.SetCounter(bytes, portions.GetBlobBytes());
    counters.SetCounter(rawBytes, portions.GetRawBytes());
}

}   // namespace

TShardDataStats::TShardDataStats(const std::shared_ptr<NOlap::IStoragesManager>& storagesManager, const ui64 tabletId)
    : StoragesManager(storagesManager)
    , TabletId(tabletId)
    , PortionSizeHistograms(Counters.BuildPortionSizeHistograms())
{
    YDB_LOG_VERIFY(StoragesManager);
}

TShardDataStats::~TShardDataStats() {
    AllShardsPortionsMetadataRamBytes.Sub(PortionsMetadataRamBytes);
}

void TShardDataStats::AddToGroup(const NOlap::TPortionInfo& portion, const EPortionGroup group) {
    const ui64 smallBlobBytes = portion.GetSmallBlobBytesInBlobStorage(GetSmallBlobThreshold(GetBlobStorageId()));
    Granules[portion.GetPathId()].AddPortion(portion, group, smallBlobBytes);
    Total.AddPortion(portion, group, smallBlobBytes);
}

void TShardDataStats::RemoveFromGroup(const NOlap::TPortionInfo& portion, const EPortionGroup group) {
    auto granuleIt = Granules.find(portion.GetPathId());
    YDB_LOG_VERIFY(granuleIt != Granules.end(),
        {"pathId", portion.GetPathId()});
    auto& granule = granuleIt->second;
    const ui64 smallBlobBytes = portion.GetSmallBlobBytesInBlobStorage(GetSmallBlobThreshold(GetBlobStorageId()));
    granule.RemovePortion(portion, group, smallBlobBytes);
    if (granule.IsEmpty()) {
        Granules.erase(granuleIt);
    }
    Total.RemovePortion(portion, group, smallBlobBytes);
}

void TShardDataStats::ChangeEssentials(const NOlap::TPortionInfo& portion, const bool add) {
    const TInternalPathId pathId = portion.GetPathId();
    const TString storageId = portion.GetTierNameDef(GetBlobStorageId());
    if (IsBlobStorage(storageId)) {
        auto& stats = Essentials[pathId][storageId];
        if (add) {
            stats.AddPortion(portion);
        } else {
            stats.RemovePortion(portion);
        }
        StorageStatsToPersist.emplace(pathId, storageId);
    }
    if (const ui64 smallBlobBytes = portion.GetSmallBlobBytesInBlobStorage(GetSmallBlobThreshold(GetBlobStorageId()))) {
        auto& stats = Essentials[pathId][GetBlobStorageId()];
        if (add) {
            stats.AddSmallBlob(smallBlobBytes);
        } else {
            stats.RemoveSmallBlob(smallBlobBytes);
        }
        StorageStatsToPersist.emplace(pathId, GetBlobStorageId());
    }
}

void TShardDataStats::AddPortionOnExecute(const NOlap::TPortionInfo& portion, const EPortionGroup group) {
    AddToGroup(portion, group);
    ChangeEssentials(portion, true);
}

void TShardDataStats::RemovePortionOnExecute(const NOlap::TPortionInfo& portion, const EPortionGroup group) {
    RemoveFromGroup(portion, group);
    ChangeEssentials(portion, false);
}

void TShardDataStats::ChangeGroupOnExecute(const NOlap::TPortionInfo& portion, const EPortionGroup from, const EPortionGroup to) {
    RemoveFromGroup(portion, from);
    AddToGroup(portion, to);
}

void TShardDataStats::AddPortionOnLoad(const std::shared_ptr<NOlap::TPortionInfo>& portion, const EPortionGroup group) {
    AddToGroup(*portion, group);
    DoAddPortionOnComplete(portion, group);
}

void TShardDataStats::AddPortionOnComplete(const std::shared_ptr<NOlap::TPortionInfo>& portion, const EPortionGroup group) {
    YDB_LOG_VERIFY(StorageStatsToPersist.empty(),
        {"error", "an Execute changed portions without PersistEssentials"});
    DoAddPortionOnComplete(portion, group);
}

void TShardDataStats::RemovePortionOnComplete(const std::shared_ptr<NOlap::TPortionInfo>& portion, const EPortionGroup group) {
    YDB_LOG_VERIFY(StorageStatsToPersist.empty(),
        {"error", "an Execute changed portions without PersistEssentials"});
    DoRemovePortionOnComplete(portion, group);
}

void TShardDataStats::PersistEssentials(NOlap::IDbWrapper& db) {
    for (const auto& [pathId, storageId] : StorageStatsToPersist) {
        auto storagesIt = Essentials.find(pathId);
        YDB_LOG_VERIFY(storagesIt != Essentials.end(),
            {"pathId", pathId});
        auto& storages = storagesIt->second;
        auto statsIt = storages.find(storageId);
        YDB_LOG_VERIFY(statsIt != storages.end(),
            {"pathId", pathId},
            {"storageId", storageId});
        const TEssentialStats& stats = statsIt->second;
        if (stats.IsEmpty()) {
            db.EraseEssentialStats(pathId, storageId);
            storages.erase(statsIt);
            if (storages.empty()) {
                Essentials.erase(storagesIt);
            }
        } else {
            db.WriteEssentialStats(pathId, storageId, stats.SerializeToProto());
        }
    }
    StorageStatsToPersist.clear();
}

bool TShardDataStats::LoadEssentials(NOlap::IDbWrapper& db) {
    THashMap<TInternalPathId, THashMap<TString, TEssentialStats>> loaded;
    if (!db.LoadEssentialStats([&](const TInternalPathId pathId, const TString& storageId, NKikimrTxColumnShard::TEssentialStats&& stats) {
            YDB_LOG_VERIFY(loaded[pathId].emplace(storageId, TEssentialStats(stats)).second,
                {"pathId", pathId},
                {"storageId", storageId});
        })) {
        return false;
    }
    Essentials = std::move(loaded);
    return true;
}

void TShardDataStats::RecalculateEssentials(NOlap::IDbWrapper& db) {
    YDB_LOG_VERIFY(StorageStatsToPersist.empty());
    THashMap<TInternalPathId, THashMap<TString, TEssentialStats>> recounted;
    for (const auto& [pathId, granule] : Granules) {
        for (const auto& [storageId, storage] : granule.GetStorages()) {
            const TEssentialStats stats(storage);
            if (IsBlobStorage(storageId) && !stats.IsEmpty()) {
                recounted[pathId].emplace(storageId, stats);
            }
        }
    }
    const auto rewrite = [&](const TInternalPathId pathId, const TString& storageId, const TEssentialStats& persisted,
                             const TEssentialStats& stats) {
        YDB_LOG_NOTICE_COMP(NKikimrServices::TX_COLUMNSHARD, "",
            {"event", "essential_stats_recalculated"},
            {"pathId", pathId},
            {"storageId", storageId},
            {"persisted", persisted.DebugString()},
            {"recounted", stats.DebugString()},
            {"tabletId", TabletId});
        if (stats.IsEmpty()) {
            db.EraseEssentialStats(pathId, storageId);
        } else {
            db.WriteEssentialStats(pathId, storageId, stats.SerializeToProto());
        }
    };
    for (const auto& [pathId, storages] : Essentials) {
        const auto recountedStorages = recounted.find(pathId);
        for (const auto& [storageId, persisted] : storages) {
            if (recountedStorages == recounted.end() || !recountedStorages->second.contains(storageId)) {
                rewrite(pathId, storageId, persisted, {});
            }
        }
    }
    for (const auto& [pathId, storages] : recounted) {
        for (const auto& [storageId, stats] : storages) {
            const TEssentialStats& persisted = GetEssentials(pathId, storageId);
            if (persisted != stats) {
                rewrite(pathId, storageId, persisted, stats);
            }
        }
    }
    Essentials = std::move(recounted);
}

void TShardDataStats::DoAddPortionOnComplete(const std::shared_ptr<NOlap::TPortionInfo>& portion, const EPortionGroup group) {
    SchemaVersionsInUse[portion->GetSchemaVersionVerified()].Inc();
    PortionsMetadataRamBytes += portion->GetMetadataMemorySize();
    Counters.OnIndexMetadataUsageBytes(AllShardsPortionsMetadataRamBytes.Add(portion->GetMetadataMemorySize()));
    // bad portions slow down writes, and an uncommitted one is not up to compaction yet
    if (group != EPortionGroup::Uncommitted) {
        PortionSizeHistograms.OnNewPortion(portion);
    }
}

void TShardDataStats::DoRemovePortionOnComplete(const std::shared_ptr<NOlap::TPortionInfo>& portion, const EPortionGroup group) {
    auto schemaVersion = SchemaVersionsInUse.find(portion->GetSchemaVersionVerified());
    YDB_LOG_VERIFY(schemaVersion != SchemaVersionsInUse.end());
    if (schemaVersion->second.Dec() == 0) {
        SchemaVersionsInUse.erase(schemaVersion);
    }
    PortionsMetadataRamBytes -= portion->GetMetadataMemorySize();
    YDB_LOG_VERIFY(PortionsMetadataRamBytes >= 0);
    Counters.OnIndexMetadataUsageBytes(AllShardsPortionsMetadataRamBytes.Sub(portion->GetMetadataMemorySize()));
    if (group != EPortionGroup::Uncommitted) {
        PortionSizeHistograms.OnDropPortion(portion);
    }
}

ui64 TShardDataStats::GetSmallBlobThreshold(const TString& storageId) {
    auto it = SmallBlobThresholds.find(storageId);
    if (it == SmallBlobThresholds.end()) {
        it = SmallBlobThresholds.emplace(storageId, StoragesManager->GetOperatorVerified(storageId)->GetSmallBlobThresholdBytes()).first;
    }
    return it->second;
}

const TDataStatsByStorage& TShardDataStats::GetGranuleStats(const TInternalPathId pathId) const {
    static const TDataStatsByStorage empty;
    auto it = Granules.find(pathId);
    return it == Granules.end() ? empty : it->second;
}

bool TShardDataStats::HasSchemaVersion(const ui64 fromVersion, const ui64 version) const {
    YDB_LOG_VERIFY(fromVersion <= version);
    auto it = SchemaVersionsInUse.lower_bound(fromVersion);
    return it != SchemaVersionsInUse.end() && it->first <= version;
}

const TEssentialStats& TShardDataStats::GetEssentials(const TInternalPathId pathId, const TString& storageId) const {
    static const TEssentialStats empty;
    auto storages = Essentials.find(pathId);
    if (storages == Essentials.end()) {
        return empty;
    }
    auto stats = storages->second.find(storageId);
    return stats == storages->second.end() ? empty : stats->second;
}

void TShardDataStats::FillTableEssentials(const TInternalPathId pathId, NKikimrTableStats::TTableStats& tableStats) const {
    const TEssentialStats& essentials = GetEssentials(pathId, GetBlobStorageId());
    FillPortionStats(tableStats, essentials.GetRows(), essentials.GetBlobBytes(), essentials.GetSmallBlobs());
}

void TShardDataStats::FillTotalEssentials(NKikimrTableStats::TTableStats& tableStats) {
    ui64 rows = 0;
    NOlap::TSmallBlobsStat smallBlobs;
    for (const auto& [pathId, _] : Essentials) {
        const TEssentialStats& essentials = GetEssentials(pathId, GetBlobStorageId());
        rows += essentials.GetRows();
        smallBlobs.VolumeBytes += essentials.GetSmallBlobs().VolumeBytes;
        smallBlobs.Count += essentials.GetSmallBlobs().Count;
    }
    FillPortionStats(tableStats, rows, GetBlobStorageBytes(), smallBlobs);

    NOlap::TSmallBlobsStat toDelete;
    for (auto&& i : StoragesManager->GetStorages()) {
        const auto stat = i.second->CalcSmallBlobsToDelete();
        toDelete.VolumeBytes += stat.VolumeBytes;
        toDelete.Count += stat.Count;
    }
    tableStats.SetSmallBlobsVolumeBytes(tableStats.GetSmallBlobsVolumeBytes() + toDelete.VolumeBytes);
    tableStats.SetSmallBlobsCount(NormalizeSmallBlobsCount(tableStats.GetSmallBlobsCount() + toDelete.Count));
}

const std::optional<TBlobStorageGroupType>& TShardDataStats::GetBlobStorageLayout() {
    if (!BlobStorageLayout) {
        BlobStorageLayout = StoragesManager->GetDefaultOperator()->GetBlobStorageLayout();
    }
    return BlobStorageLayout;
}

ui64 TShardDataStats::NormalizeSmallBlobsCount(const ui64 rawCount) {
    const auto& layout = GetBlobStorageLayout();
    if (!layout) {
        return rawCount;
    }
    const auto& quota = AppDataVerified().SmallBlobsQuotaConfig;
    ui32 coef = 1;
    switch (layout->GetErasure()) {
        case NKikimr::TErasureType::ErasureMirror3dc:
            coef = quota.GetMirror3dcStorageHardQuotaCoef();
            break;
        case NKikimr::TErasureType::Erasure4Plus2Block:
            coef = quota.GetBlock42StorageHardQuotaCoef();
            break;
        default:
            break;
    }
    coef = std::max<ui32>(1, coef);
    const ui32 vdisks = std::max<ui32>(1, layout->BlobSubgroupSize());
    // 1. Every vdisk in a group keeps a record for a blob in the index in RAM, so we multiply by the number of vdisks.
    // 2. Hard storage quota is set in term of "user data", not "actual blob storage consumption".
    // When the quota is set, it is usually "actual blobs storage allowance" / some coefficient (depending on the erasure).
    // So, here we take that coefficient into account too.
    return rawCount * vdisks / coef;
}

void TShardDataStats::PublishShardCounters(const TTabletCountersHandle& counters) const {
    counters.SetCounter(COUNTER_INDEX_TABLES, Granules.size());

    const auto& blobStorageStats = GetStorageStats(GetBlobStorageId());
    const NOlap::TSimplePortionsGroupInfo blobStorage = blobStorageStats.GetAll();
    counters.SetCounter(COUNTER_DATA_BYTES, blobStorage.GetDataBlobBytes());
    counters.SetCounter(COUNTER_INDEX_BYTES, blobStorage.GetIndexBlobBytes());

    const NOlap::TSimplePortionsGroupInfo written = blobStorageStats.GetWritten();
    const NOlap::TSimplePortionsGroupInfo compacted = blobStorageStats.GetCompacted();
    const NOlap::TSimplePortionsGroupInfo removed = Total.GetRemoved();
    NOlap::TSimplePortionsGroupInfo evicted;
    for (const auto& [storageId, storage] : Total.GetStorages()) {
        if (storageId != GetBlobStorageId()) {
            evicted += storage.GetWritten() + storage.GetCompacted();
        }
    }
    SetPortionCounters(counters, written, COUNTER_INSERTED_PORTIONS, COUNTER_INSERTED_ROWS, COUNTER_INSERTED_BYTES, COUNTER_INSERTED_RAW_BYTES);
    // COMPACTED is never produced
    SetPortionCounters(counters, {}, COUNTER_COMPACTED_PORTIONS, COUNTER_COMPACTED_ROWS, COUNTER_COMPACTED_BYTES, COUNTER_COMPACTED_RAW_BYTES);
    SetPortionCounters(counters, compacted, COUNTER_SPLIT_COMPACTED_PORTIONS, COUNTER_SPLIT_COMPACTED_ROWS, COUNTER_SPLIT_COMPACTED_BYTES,
        COUNTER_SPLIT_COMPACTED_RAW_BYTES);
    SetPortionCounters(counters, removed, COUNTER_INACTIVE_PORTIONS, COUNTER_INACTIVE_ROWS, COUNTER_INACTIVE_BYTES, COUNTER_INACTIVE_RAW_BYTES);
    SetPortionCounters(counters, evicted, COUNTER_EVICTED_PORTIONS, COUNTER_EVICTED_ROWS, COUNTER_EVICTED_BYTES, COUNTER_EVICTED_RAW_BYTES);

    YDB_LOG_DEBUG_COMP(NKikimrServices::TX_COLUMNSHARD, "Index",
        {"tablesCount", Granules.size()},
        {"uncommittedStats", Total.GetUncommitted(GetBlobStorageId()).DebugString()},
        {"writtenStats", written.DebugString()},
        {"splitCompactedStats", compacted.DebugString()},
        {"inactiveStats", removed.DebugString()},
        {"evictedStats", evicted.DebugString()},
        {"tabletId", TabletId});
}

ui64 TShardDataStats::GetBlobStorageBytes() const {
    ui64 result = 0;
    for (const auto& [pathId, _] : Essentials) {
        result += GetEssentials(pathId, GetBlobStorageId()).GetBlobBytes();
    }
    return result;
}

}   // namespace NKikimr::NColumnShard
