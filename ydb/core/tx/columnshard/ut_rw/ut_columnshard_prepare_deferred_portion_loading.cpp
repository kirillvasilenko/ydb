#include <ydb/core/tx/columnshard/columnshard.h>
#include <ydb/core/tx/columnshard/columnshard_schema.h>
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

class TTestShard {
public:
    NYDBTest::TControllers::TGuard<TModifierController> Controller = NYDBTest::TControllers::RegisterCSControllerGuard<TModifierController>();
    TTestBasicRuntime Runtime;
    TActorId Sender;
    const TestTableDescription Table;

private:
    ui64 WriteId = 0;
    ui64 TxId = 100;

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

    void Reboot() {
        RebootTablet(Runtime, TTestTxConfig::TxTablet0, Sender);
    }

    ui64 AssertIndexMatchesPortions() {
        return NTxUT::AssertIndexMatchesPortions(Runtime, TTestTxConfig::TxTablet0, Table.Pk);
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
        // what an old binary saves: the normalizers up to the one before ours are done
        UpdateLocalDbTableRow(shard.Runtime, TTestTxConfig::TxTablet0, "Value",
            Sprintf("'('('Id (Uint32 '%" PRIu32 ")))", static_cast<ui32>(Schema::EValueIds::LastNormalizerSequentialId)),
            Sprintf("'('('Digit (Uint64 '%" PRIu64 ")))", static_cast<ui64>(NOlap::ENormalizerSequentialId::PrepareDeferredPortionLoading) - 1));
        shard.Reboot();
        UNIT_ASSERT_VALUES_EQUAL(shard.AssertIndexMatchesPortions(), 3);
    }
}

}   // namespace NKikimr
