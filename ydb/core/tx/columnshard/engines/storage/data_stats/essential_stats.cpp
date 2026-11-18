#include "essential_stats.h"

#include <ydb/core/tx/columnshard/engines/portions/portion_info.h>

#include <util/string/builder.h>

namespace NKikimr::NColumnShard {

namespace {

// a counting mistake must not fail the shard
void Subtract(ui64& value, const ui64 delta) {
    value -= std::min(value, delta);
}

}   // namespace

TEssentialStats::TEssentialStats(const NKikimrTxColumnShard::TEssentialStats& proto)
    : Rows(proto.GetRows())
    , BlobBytes(proto.GetBlobBytes())
    , SmallBlobsBytes(proto.GetSmallBlobsBytes())
    , SmallBlobsCount(proto.GetSmallBlobsCount())
{
}

TEssentialStats::TEssentialStats(const TStorageStats& storage)
    : Rows(storage.GetAll().GetRecordsCount())
    , BlobBytes(storage.GetAll().GetBlobBytes())
    , SmallBlobsBytes(storage.GetSmallBlobs().VolumeBytes)
    , SmallBlobsCount(storage.GetSmallBlobs().Count)
{
}

void TEssentialStats::AddPortion(const NOlap::TPortionInfo& portion) {
    Rows += portion.GetRecordsCount();
    BlobBytes += portion.GetTotalBlobBytes();
}

void TEssentialStats::RemovePortion(const NOlap::TPortionInfo& portion) {
    Subtract(Rows, portion.GetRecordsCount());
    Subtract(BlobBytes, portion.GetTotalBlobBytes());
}

void TEssentialStats::AddSmallBlob(const ui64 bytes) {
    SmallBlobsBytes += bytes;
    ++SmallBlobsCount;
}

void TEssentialStats::RemoveSmallBlob(const ui64 bytes) {
    Subtract(SmallBlobsBytes, bytes);
    Subtract(SmallBlobsCount, 1);
}

bool TEssentialStats::IsEmpty() const {
    return !Rows && !BlobBytes && !SmallBlobsBytes && !SmallBlobsCount;
}

NKikimrTxColumnShard::TEssentialStats TEssentialStats::SerializeToProto() const {
    NKikimrTxColumnShard::TEssentialStats result;
    result.SetRows(Rows);
    result.SetBlobBytes(BlobBytes);
    result.SetSmallBlobsBytes(SmallBlobsBytes);
    result.SetSmallBlobsCount(SmallBlobsCount);
    return result;
}

TString TEssentialStats::DebugString() const {
    return TStringBuilder() << "{rows=" << Rows << ";blob_bytes=" << BlobBytes << ";small_blobs_bytes=" << SmallBlobsBytes
                            << ";small_blobs_count=" << SmallBlobsCount << "}";
}

}   // namespace NKikimr::NColumnShard
