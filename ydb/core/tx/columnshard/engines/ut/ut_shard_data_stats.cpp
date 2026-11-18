#include <ydb/core/protos/table_stats.pb.h>
#include <ydb/core/tx/columnshard/engines/storage/data_stats/shard_data_stats.h>
#include <ydb/core/tx/columnshard/test_helper/portion_test_helper.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NOlap {

Y_UNIT_TEST_SUITE(TShardDataStatsTests) {
    const TInternalPathId PathId = TInternalPathId::FromRawValue(1);
    const TSnapshot Committed(1, 1);
    const TSnapshot Removed(2, 1);
    const TString BlobStorage = NBlobOperations::TGlobal::DefaultStorageId;
    const TString Tier = "tier1";
    using NColumnShard::EPortionGroup;
    using NTest::TestPortionColumnBlobBytes;

    std::shared_ptr<TPortionInfo> MakeCompacted(const ui64 portionId, const ui32 recordsCount, const std::optional<TSnapshot>& removeSnapshot,
        const TString& tierName = {}, const ui32 blobStorageIndexBytes = 0) {
        return NTest::MakeTestCompactedPortion(
            PathId, portionId, 1, 2, recordsCount, Committed, removeSnapshot, tierName, blobStorageIndexBytes);
    }

    NKikimrTableStats::TTableStats GetTableStats(const NColumnShard::TShardDataStats& stats) {
        NKikimrTableStats::TTableStats tableStats;
        stats.FillTableEssentials(PathId, tableStats);
        return tableStats;
    }

    Y_UNIT_TEST(EachPortionGoesToItsStorageAndGroup) {
        NColumnShard::TShardDataStats stats(TTestStoragesManager::GetInstance(), 1);
        stats.AddPortionOnExecute(*NTest::MakeTestWrittenPortion(PathId, 1, 1, Committed), EPortionGroup::Written);
        stats.AddPortionOnExecute(*MakeCompacted(2, 2, std::nullopt), EPortionGroup::Compacted);
        stats.AddPortionOnExecute(*NTest::MakeTestWrittenPortion(PathId, 3, 4, Committed, Tier), EPortionGroup::Written);
        stats.AddPortionOnExecute(*MakeCompacted(4, 8, std::nullopt, Tier), EPortionGroup::Compacted);
        stats.AddPortionOnExecute(*MakeCompacted(5, 16, Removed), EPortionGroup::Removed);
        stats.AddPortionOnExecute(*MakeCompacted(6, 32, Removed, Tier), EPortionGroup::Removed);

        const auto& granule = stats.GetGranuleStats(PathId);
        UNIT_ASSERT_VALUES_EQUAL(granule.GetWritten(BlobStorage).GetRecordsCount(), 1);
        UNIT_ASSERT_VALUES_EQUAL(granule.GetCompacted(BlobStorage).GetRecordsCount(), 2);
        UNIT_ASSERT_VALUES_EQUAL(granule.GetRemoved(BlobStorage).GetRecordsCount(), 16);
        UNIT_ASSERT_VALUES_EQUAL(granule.GetWritten(Tier).GetRecordsCount(), 4);
        UNIT_ASSERT_VALUES_EQUAL(granule.GetCompacted(Tier).GetRecordsCount(), 8);
        UNIT_ASSERT_VALUES_EQUAL(granule.GetRemoved(Tier).GetRecordsCount(), 32);
        UNIT_ASSERT_VALUES_EQUAL(granule.GetAll().GetRecordsCount(), 1 + 2 + 4 + 8 + 16 + 32);
    }

    Y_UNIT_TEST(EmptyAfterRemovingEveryPortion) {
        NColumnShard::TShardDataStats stats(TTestStoragesManager::GetInstance(), 1);
        const auto written = NTest::MakeTestWrittenPortion(PathId, 1, 1, Committed);
        const auto removed = MakeCompacted(2, 2, Removed, Tier, 50);
        stats.AddPortionOnExecute(*written, EPortionGroup::Written);
        stats.AddPortionOnExecute(*removed, EPortionGroup::Removed);
        stats.RemovePortionOnExecute(*written, EPortionGroup::Written);
        UNIT_ASSERT(!stats.GetGranuleStats(PathId).IsEmpty());
        stats.RemovePortionOnExecute(*removed, EPortionGroup::Removed);
        UNIT_ASSERT(stats.GetGranuleStats(PathId).IsEmpty());
    }

    Y_UNIT_TEST(PortionBelowThresholdIsSmallBlob) {
        NColumnShard::TShardDataStats stats(TTestStoragesManager::GetInstance(), 1);
        stats.AddPortionOnExecute(*MakeCompacted(1, 1, std::nullopt, {}, 50), EPortionGroup::Compacted);
        UNIT_ASSERT_VALUES_EQUAL(stats.GetGranuleStats(PathId).GetSmallBlobs().VolumeBytes, TestPortionColumnBlobBytes + 50);
        UNIT_ASSERT_VALUES_EQUAL(stats.GetGranuleStats(PathId).GetSmallBlobs().Count, 1);
    }

    Y_UNIT_TEST(PortionAboveThresholdIsNotSmallBlob) {
        NColumnShard::TShardDataStats stats(TTestStoragesManager::GetInstance(), 1);
        stats.AddPortionOnExecute(*MakeCompacted(1, 1, std::nullopt, {}, 1 << 20), EPortionGroup::Compacted);
        UNIT_ASSERT_VALUES_EQUAL(stats.GetGranuleStats(PathId).GetSmallBlobs().Count, 0);
        UNIT_ASSERT_VALUES_EQUAL(stats.GetStorageStats(BlobStorage).GetSmallBlobs().Count, 0);
    }

    Y_UNIT_TEST(EvictedPortionIndexesAreBlobStorageSmallBlobs) {
        NColumnShard::TShardDataStats stats(TTestStoragesManager::GetInstance(), 1);
        stats.AddPortionOnExecute(*MakeCompacted(1, 1, std::nullopt, Tier, 50), EPortionGroup::Compacted);
        UNIT_ASSERT_VALUES_EQUAL(stats.GetGranuleStats(PathId).GetSmallBlobs(BlobStorage).VolumeBytes, 50);
        UNIT_ASSERT_VALUES_EQUAL(stats.GetGranuleStats(PathId).GetSmallBlobs(Tier).VolumeBytes, 0);
        UNIT_ASSERT_VALUES_EQUAL(stats.GetStorageStats(Tier).GetSmallBlobs().Count, 0);
    }

    Y_UNIT_TEST(ShardTotalsSumGranulesByStorage) {
        NColumnShard::TShardDataStats stats(TTestStoragesManager::GetInstance(), 1);
        const auto secondPathId = TInternalPathId::FromRawValue(2);
        stats.AddPortionOnExecute(*NTest::MakeTestWrittenPortion(PathId, 1, 1, Committed), EPortionGroup::Written);
        stats.AddPortionOnExecute(*NTest::MakeTestWrittenPortion(secondPathId, 2, 2, Committed), EPortionGroup::Written);
        stats.AddPortionOnExecute(*MakeCompacted(3, 4, std::nullopt, Tier, 50), EPortionGroup::Compacted);
        stats.AddPortionOnExecute(
            *NTest::MakeTestCompactedPortion(secondPathId, 4, 1, 2, 8, Committed, Removed, Tier, 25), EPortionGroup::Removed);

        const auto& blobStorage = stats.GetStorageStats(BlobStorage);
        UNIT_ASSERT_VALUES_EQUAL(blobStorage.GetWritten().GetRecordsCount(), 3);
        UNIT_ASSERT_VALUES_EQUAL(blobStorage.GetSmallBlobs().Count, 4);
        UNIT_ASSERT_VALUES_EQUAL(blobStorage.GetSmallBlobs().VolumeBytes, 2 * TestPortionColumnBlobBytes + 50 + 25);
        const auto& tier = stats.GetStorageStats(Tier);
        UNIT_ASSERT_VALUES_EQUAL(tier.GetCompacted().GetRecordsCount(), 4);
        UNIT_ASSERT_VALUES_EQUAL(tier.GetRemoved().GetRecordsCount(), 8);
        UNIT_ASSERT_VALUES_EQUAL(tier.GetSmallBlobs().Count, 0);
    }

    Y_UNIT_TEST(ReAddedPortionCountsOnce) {
        NColumnShard::TShardDataStats stats(TTestStoragesManager::GetInstance(), 1);
        const auto portion = MakeCompacted(1, 1, std::nullopt, Tier, 50);
        stats.AddPortionOnExecute(*portion, EPortionGroup::Compacted);
        stats.RemovePortionOnExecute(*portion, EPortionGroup::Compacted);
        UNIT_ASSERT(stats.GetGranuleStats(PathId).IsEmpty());
        UNIT_ASSERT_VALUES_EQUAL(stats.GetStorageStats(Tier).GetAll().GetCount(), 0);
        UNIT_ASSERT_VALUES_EQUAL(stats.GetStorageStats(BlobStorage).GetSmallBlobs().Count, 0);

        stats.AddPortionOnExecute(*portion, EPortionGroup::Compacted);
        UNIT_ASSERT_VALUES_EQUAL(stats.GetStorageStats(Tier).GetAll().GetCount(), 1);
        UNIT_ASSERT_VALUES_EQUAL(stats.GetStorageStats(BlobStorage).GetSmallBlobs().Count, 1);
        UNIT_ASSERT_VALUES_EQUAL(stats.GetGranuleStats(PathId).GetSmallBlobs().Count, 1);
    }

    Y_UNIT_TEST(UncommittedPortionIsCountedAsStored) {
        NColumnShard::TShardDataStats stats(TTestStoragesManager::GetInstance(), 1);
        stats.AddPortionOnExecute(*NTest::MakeTestUncommittedPortion(PathId, 1, 1), EPortionGroup::Uncommitted);
        const auto& granule = stats.GetGranuleStats(PathId);
        UNIT_ASSERT_VALUES_EQUAL(granule.GetUncommitted().GetRecordsCount(), 1);
        UNIT_ASSERT_VALUES_EQUAL(granule.GetAll().GetRecordsCount(), 1);
        UNIT_ASSERT_VALUES_EQUAL(granule.GetSmallBlobs().Count, 1);
    }

    Y_UNIT_TEST(UncommittedPortionMovesToWrittenOnCommit) {
        NColumnShard::TShardDataStats stats(TTestStoragesManager::GetInstance(), 1);
        const auto portion = NTest::MakeTestUncommittedPortion(PathId, 1, 1);
        stats.AddPortionOnExecute(*portion, EPortionGroup::Uncommitted);
        portion->SetCommitSnapshot(Committed);
        stats.ChangeGroupOnExecute(*portion, EPortionGroup::Uncommitted, EPortionGroup::Written);
        const auto& granule = stats.GetGranuleStats(PathId);
        UNIT_ASSERT(granule.GetUncommitted().IsEmpty());
        UNIT_ASSERT_VALUES_EQUAL(granule.GetWritten().GetRecordsCount(), 1);
        UNIT_ASSERT_VALUES_EQUAL(granule.GetSmallBlobs().Count, 1);
    }

    Y_UNIT_TEST(UncommittedPortionMovesToRemovedOnAbort) {
        NColumnShard::TShardDataStats stats(TTestStoragesManager::GetInstance(), 1);
        const auto portion = NTest::MakeTestUncommittedPortion(PathId, 1, 1);
        stats.AddPortionOnExecute(*portion, EPortionGroup::Uncommitted);
        portion->SetRemoveSnapshot(Removed);
        portion->SetCommitSnapshot(Removed);
        stats.ChangeGroupOnExecute(*portion, EPortionGroup::Uncommitted, EPortionGroup::Removed);
        const auto& granule = stats.GetGranuleStats(PathId);
        UNIT_ASSERT(granule.GetUncommitted().IsEmpty());
        UNIT_ASSERT_VALUES_EQUAL(granule.GetRemoved().GetRecordsCount(), 1);
        UNIT_ASSERT_VALUES_EQUAL(granule.GetSmallBlobs().Count, 1);
    }

    // the recalculation replaces the essentials with the group stats, so both must count a portion the same in every group
    Y_UNIT_TEST(EssentialsEqualGroupStats) {
        NColumnShard::TShardDataStats stats(TTestStoragesManager::GetInstance(), 1);
        stats.AddPortionOnExecute(*NTest::MakeTestUncommittedPortion(PathId, 1, 1), EPortionGroup::Uncommitted);
        stats.AddPortionOnExecute(*NTest::MakeTestWrittenPortion(PathId, 2, 2, Committed), EPortionGroup::Written);
        stats.AddPortionOnExecute(*MakeCompacted(3, 4, std::nullopt, {}, 1 << 20), EPortionGroup::Compacted);
        stats.AddPortionOnExecute(*MakeCompacted(4, 8, std::nullopt, Tier, 50), EPortionGroup::Compacted);
        stats.AddPortionOnExecute(*MakeCompacted(5, 16, Removed), EPortionGroup::Removed);
        const auto& groups = stats.GetGranuleStats(PathId);
        const auto essentials = GetTableStats(stats);
        UNIT_ASSERT_VALUES_EQUAL(essentials.GetRowCount(), 1 + 2 + 4 + 16);
        UNIT_ASSERT_VALUES_EQUAL(essentials.GetRowCount(), groups.GetAll(BlobStorage).GetRecordsCount());
        UNIT_ASSERT_VALUES_EQUAL(essentials.GetDataSize(), groups.GetAll(BlobStorage).GetBlobBytes());
        UNIT_ASSERT_VALUES_EQUAL(essentials.GetSmallBlobsCount(), groups.GetSmallBlobs(BlobStorage).Count);
        UNIT_ASSERT_VALUES_EQUAL(essentials.GetSmallBlobsVolumeBytes(), groups.GetSmallBlobs(BlobStorage).VolumeBytes);
    }

    // commit and abort keep the portion's row as it is
    Y_UNIT_TEST(GroupChangeKeepsEssentials) {
        NColumnShard::TShardDataStats stats(TTestStoragesManager::GetInstance(), 1);
        const auto committed = NTest::MakeTestUncommittedPortion(PathId, 1, 1);
        const auto aborted = NTest::MakeTestUncommittedPortion(PathId, 2, 2);
        stats.AddPortionOnExecute(*committed, EPortionGroup::Uncommitted);
        stats.AddPortionOnExecute(*aborted, EPortionGroup::Uncommitted);
        const auto before = GetTableStats(stats).ShortDebugString();
        committed->SetCommitSnapshot(Committed);
        stats.ChangeGroupOnExecute(*committed, EPortionGroup::Uncommitted, EPortionGroup::Written);
        aborted->SetRemoveSnapshot(Removed);
        aborted->SetCommitSnapshot(Removed);
        stats.ChangeGroupOnExecute(*aborted, EPortionGroup::Uncommitted, EPortionGroup::Removed);
        UNIT_ASSERT_VALUES_EQUAL(GetTableStats(stats).ShortDebugString(), before);
    }

    // the portions read at the start are in the rows already
    Y_UNIT_TEST(LoadedPortionIsNotAddedToEssentials) {
        NColumnShard::TShardDataStats stats(TTestStoragesManager::GetInstance(), 1);
        stats.AddPortionOnLoad(NTest::MakeTestWrittenPortion(PathId, 1, 1, Committed), EPortionGroup::Written);
        UNIT_ASSERT_VALUES_EQUAL(stats.GetGranuleStats(PathId).GetWritten().GetRecordsCount(), 1);
        UNIT_ASSERT_VALUES_EQUAL(GetTableStats(stats).GetRowCount(), 0);
    }

    Y_UNIT_TEST(EssentialsEmptyAfterRemovingEveryPortion) {
        NColumnShard::TShardDataStats stats(TTestStoragesManager::GetInstance(), 1);
        const auto written = NTest::MakeTestWrittenPortion(PathId, 1, 1, Committed);
        const auto evicted = MakeCompacted(2, 2, std::nullopt, Tier, 50);
        stats.AddPortionOnExecute(*written, EPortionGroup::Written);
        stats.AddPortionOnExecute(*evicted, EPortionGroup::Compacted);
        stats.RemovePortionOnExecute(*written, EPortionGroup::Written);
        UNIT_ASSERT_VALUES_EQUAL(GetTableStats(stats).GetSmallBlobsCount(), 1);
        stats.RemovePortionOnExecute(*evicted, EPortionGroup::Compacted);
        UNIT_ASSERT_VALUES_EQUAL(GetTableStats(stats).GetSmallBlobsCount(), 0);
    }
}

}   // namespace NKikimr::NOlap
