#pragma once

#include "data_stats_by_storage.h"

#include <ydb/core/tx/columnshard/engines/protos/essential_stats.pb.h>

namespace NKikimr::NOlap {
class TPortionInfo;
}

namespace NKikimr::NColumnShard {

// What SchemeShard and Hive get about one table in one blob storage; persisted, so it covers the portions not loaded yet
class TEssentialStats {
private:
    ui64 Rows = 0;
    ui64 BlobBytes = 0;
    ui64 SmallBlobsBytes = 0;
    ui64 SmallBlobsCount = 0;

public:
    TEssentialStats() = default;
    explicit TEssentialStats(const NKikimrTxColumnShard::TEssentialStats& proto);
    explicit TEssentialStats(const TStorageStats& storage);

    void AddPortion(const NOlap::TPortionInfo& portion);
    void RemovePortion(const NOlap::TPortionInfo& portion);
    void AddSmallBlob(const ui64 bytes);
    void RemoveSmallBlob(const ui64 bytes);

    bool IsEmpty() const;
    bool operator==(const TEssentialStats& other) const = default;
    NKikimrTxColumnShard::TEssentialStats SerializeToProto() const;
    TString DebugString() const;

    ui64 GetRows() const {
        return Rows;
    }

    ui64 GetBlobBytes() const {
        return BlobBytes;
    }

    NOlap::TSmallBlobsStat GetSmallBlobs() const {
        return { .VolumeBytes = SmallBlobsBytes, .Count = SmallBlobsCount };
    }
};

}   // namespace NKikimr::NColumnShard
