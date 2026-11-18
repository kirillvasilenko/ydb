#include <ydb/core/protos/tx_columnshard.pb.h>
#include <ydb/core/tx/columnshard/columnshard.h>
#include <ydb/core/tx/columnshard/columnshard_impl.h>
#include <ydb/core/tx/columnshard/columnshard_schema.h>
#include <ydb/core/tx/columnshard/engines/column_engine_logs.h>
#include <ydb/core/tx/columnshard/hooks/testing/controller.h>
#include <ydb/core/tx/columnshard/normalizer/abstract/abstract.h>
#include <ydb/core/tx/columnshard/test_helper/columnshard_ut_common.h>

#include <inttypes.h>

namespace NKikimr {

using namespace NColumnShard;
using namespace NTxUT;

namespace {

const ui64 TableId = 1;

class TModifierController: public NYDBTest::NColumnShard::TController {
public:
    NYDBTest::ILocalDBModifier::TPtr Modifier;

    NYDBTest::ILocalDBModifier::TPtr BuildLocalBaseModifier() const override {
        return Modifier;
    }
};

// what a shard written before the index table looks like
class TDropIndexTable: public NYDBTest::ILocalDBModifier {
public:
    void Apply(NTabletFlatExecutor::TTransactionContext& txc) const override {
        txc.DB.Alter().DropTable(Schema::PortionsByLastKeyTableId);
    }
};

// what an operation written before C5 looks like
class TClearOperationPortionIds: public NYDBTest::ILocalDBModifier {
public:
    void Apply(NTabletFlatExecutor::TTransactionContext& txc) const override {
        NIceDb::TNiceDb db(txc.DB);
        std::vector<std::pair<ui64, NKikimrTxColumnShard::TInternalOperationData>> operations;
        auto rowset = db.Table<Schema::Operations>().Select();
        UNIT_ASSERT(rowset.IsReady());
        while (!rowset.EndOfSet()) {
            auto& [writeId, metadata] = operations.emplace_back();
            writeId = rowset.GetValue<Schema::Operations::WriteId>();
            UNIT_ASSERT(metadata.ParseFromString(rowset.GetValue<Schema::Operations::Metadata>()));
            UNIT_ASSERT(rowset.Next());
        }
        for (auto& [writeId, metadata] : operations) {
            metadata.ClearPortionIds();
            db.Table<Schema::Operations>().Key(writeId).Update(NIceDb::TUpdate<Schema::Operations::Metadata>(metadata.SerializeAsString()));
        }
    }
};

class TTestShard {
public:
    NYDBTest::TControllers::TGuard<TModifierController> Controller = NYDBTest::TControllers::RegisterCSControllerGuard<TModifierController>();
    TTestBasicRuntime Runtime;
    TActorId Sender;
    const TestTableDescription Table;

private:
    ui64 WriteId = 0;
    ui64 TxId = 100;
    // the default lock of WriteData
    static constexpr ui64 LockId = 1;

public:
    TTestShard() {
        Controller->DisableBackground(NYDBTest::ICSController::EBackground::Compaction);
        TTester::Setup(Runtime);
        Sender = Runtime.AllocateEdgeActor();
        CreateTestBootstrapper(Runtime, CreateTestTabletInfo(TTestTxConfig::TxTablet0, TTabletTypes::ColumnShard), &CreateColumnShard);
        TDispatchOptions options;
        options.FinalEvents.push_back(TDispatchOptions::TFinalEventCondition(TEvTablet::EvBoot));
        Runtime.DispatchEvents(options);
        Y_UNUSED(SetupSchema(Runtime, Sender, TableId, Table));
    }

    void WriteAndCommit() {
        std::vector<ui64> writeIds;
        UNIT_ASSERT(WriteData(Runtime, Sender, WriteId++, TableId, MakeTestBlob({ 0, 1000 }, Table.Schema), Table.Schema, true, &writeIds));
        PlanCommit(Runtime, Sender, ProposeCommit(Runtime, Sender, TxId, writeIds), TxId);
        ++TxId;
    }

    // proposed, so that CleanOrphanedOperations keeps it, and never planned
    void WriteAndPropose() {
        std::vector<ui64> writeIds;
        UNIT_ASSERT(WriteData(Runtime, Sender, WriteId++, TableId, MakeTestBlob({ 0, 1000 }, Table.Schema), Table.Schema, true, &writeIds));
        Y_UNUSED(ProposeCommit(Runtime, Sender, TxId++, writeIds, LockId));
    }

    std::vector<ui64> GetLockPortionIds() {
        const auto* lock = Controller->GetTheOnlyShard()->GetOperationsManager().GetLockOptional(LockId);
        UNIT_ASSERT(lock);
        UNIT_ASSERT_VALUES_EQUAL(lock->GetWriteOperations().size(), 1);
        return lock->GetWriteOperations().front()->GetPortionIds();
    }

    void Reboot() {
        RebootTablet(Runtime, TTestTxConfig::TxTablet0, Sender);
    }

    // what a binary without the normalizer saves: the normalizers up to the one before it are done
    void RunNormalizerAgainOnReboot() {
        UpdateLocalDbTableRow(Runtime, TTestTxConfig::TxTablet0, "Value",
            Sprintf("'('('Id (Uint32 '%" PRIu32 ")))", static_cast<ui32>(Schema::EValueIds::LastNormalizerSequentialId)),
            Sprintf("'('('Digit (Uint64 '%" PRIu64 ")))", static_cast<ui64>(NOlap::ENormalizerSequentialId::PrepareDeferredPortionLoading) - 1));
    }

    ui64 AssertIndexMatchesPortions() {
        return NTxUT::AssertIndexMatchesPortions(Runtime, TTestTxConfig::TxTablet0, Table.Pk);
    }

    std::optional<ui64> ReadLastInsertWriteId() {
        return ReadIndexCounter(Runtime, TTestTxConfig::TxTablet0, NOlap::TColumnEngineForLogs::LAST_INSERT_WRITE_ID);
    }

    void SetLastInsertWriteId(const ui64 value) {
        UpdateLocalDbTableRow(Runtime, TTestTxConfig::TxTablet0, "IndexCounters", LastInsertWriteIdKey(),
            Sprintf("'('('ValueUI64 (Uint64 '%" PRIu64 ")))", value));
    }

    void EraseLastInsertWriteId() {
        EraseLocalDbTableRow(Runtime, TTestTxConfig::TxTablet0, "IndexCounters", LastInsertWriteIdKey());
    }

private:
    static TString LastInsertWriteIdKey() {
        return Sprintf(
            "'('('Index (Uint32 '0)) '('Counter (Uint32 '%" PRIu32 ")))", static_cast<ui32>(NOlap::TColumnEngineForLogs::LAST_INSERT_WRITE_ID));
    }
};

}   // namespace

Y_UNIT_TEST_SUITE(TPrepareDeferredPortionLoading) {
    Y_UNIT_TEST(CreatesIndexTableOnShardWithoutIt) {
        TTestShard shard;
        for (ui32 i = 0; i < 3; ++i) {
            shard.WriteAndCommit();
        }

        // with a modifier the saved normalizer id is ignored, so every normalizer runs on this start
        shard.Controller->Modifier = std::make_shared<TDropIndexTable>();
        shard.Reboot();
        UNIT_ASSERT_VALUES_EQUAL(shard.AssertIndexMatchesPortions(), 3);
    }

    Y_UNIT_TEST(RebuildsStaleIndexTableWhenRunAgain) {
        TTestShard shard;
        for (ui32 i = 0; i < 3; ++i) {
            shard.WriteAndCommit();
        }

        // a row of a portion that no longer exists, as an old binary leaves behind
        UpdateLocalDbTableRow(shard.Runtime, TTestTxConfig::TxTablet0, "IndexPortionsByLastKey",
            "'('('PathId (Uint64 '1)) '('LastKey_timestamp (Timestamp '1)) '('LastKey_resource_type (Utf8 'a)) "
            "'('LastKey_resource_id (Utf8 'a)) '('LastKey_uid (Utf8 'a)) '('PortionId (Uint64 '999999)))", "'()");
        shard.RunNormalizerAgainOnReboot();
        shard.Reboot();
        UNIT_ASSERT_VALUES_EQUAL(shard.AssertIndexMatchesPortions(), 3);
    }

    Y_UNIT_TEST(RestoresLastInsertWriteIdOfShardWithoutIt) {
        TTestShard shard;
        for (ui32 i = 0; i < 3; ++i) {
            shard.WriteAndCommit();
        }
        const std::optional<ui64> lastInsertWriteId = shard.ReadLastInsertWriteId();
        UNIT_ASSERT(lastInsertWriteId);

        shard.EraseLastInsertWriteId();
        shard.RunNormalizerAgainOnReboot();
        shard.Reboot();
        UNIT_ASSERT_VALUES_EQUAL(shard.ReadLastInsertWriteId().value_or(0), *lastInsertWriteId);
    }

    Y_UNIT_TEST(KeepsLastInsertWriteIdAboveThePortions) {
        TTestShard shard;
        for (ui32 i = 0; i < 3; ++i) {
            shard.WriteAndCommit();
        }

        // the portions with the highest ids may be gone, compacted away
        shard.SetLastInsertWriteId(1000);
        shard.RunNormalizerAgainOnReboot();
        shard.Reboot();
        UNIT_ASSERT_VALUES_EQUAL(shard.ReadLastInsertWriteId().value_or(0), 1000);
    }

    Y_UNIT_TEST(FillsPortionIdsOfOperationsWithoutThem) {
        TTestShard shard;
        shard.WriteAndPropose();
        const std::vector<ui64> portionIds = shard.GetLockPortionIds();
        UNIT_ASSERT(!portionIds.empty());

        shard.Controller->Modifier = std::make_shared<TClearOperationPortionIds>();
        shard.Reboot();
        UNIT_ASSERT(shard.GetLockPortionIds() == portionIds);
    }
}

}   // namespace NKikimr
