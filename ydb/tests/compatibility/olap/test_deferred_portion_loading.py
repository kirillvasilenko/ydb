import pytest

from ydb.tests.library.common.types import Erasure
from ydb.tests.library.compatibility.fixtures import RestartToAnotherVersionFixture
from ydb.tests.oss.ydb_sdk_import import ydb

FLAG_SINCE = (27, 1)
FIRST, SECOND = 0, 1
ROWS_PER_WRITE = 100


class TestDeferredPortionLoading(RestartToAnotherVersionFixture):
    @pytest.fixture(autouse=True)
    def setup(self):
        if self._flags(FIRST) == self._flags(SECOND) == [False]:
            pytest.skip("neither binary has deferred portion loading")
        self.steps = []
        for first_flag in self._flags(FIRST):
            for second_flag in self._flags(SECOND):
                if self.all_binary_paths[FIRST] == self.all_binary_paths[SECOND] and first_flag == second_flag:
                    continue  # the same binary with the same flag: plain restarts
                self.steps += [(FIRST, first_flag), (SECOND, second_flag), (FIRST, first_flag)]
        self.flag = self.steps[0][1]
        yield from self.setup_cluster(nodes=1, erasure=Erasure.NONE, column_shard_config=self._column_shard_config())

    def _flags(self, binary):
        return [False, True] if self.versions[binary] >= FLAG_SINCE else [False]

    def _column_shard_config(self):
        config = {"disabled_on_scheme_shard": False}
        if self.flag:
            config["deferred_portion_loading_settings"] = {"enabled": True, "portions_per_batch": 1}
        return config

    def _start(self, binary, flag):
        if (binary, flag) == (self.current_binary_paths_index, self.flag):
            return
        self.flag = flag
        self.config.yaml_config["column_shard_config"] = self._column_shard_config()
        if binary != self.current_binary_paths_index:
            self.change_cluster_version()
        else:
            self.stop_driver()
            self.cluster.update_configurator_and_restart(self.config)
            self.driver = self.create_driver()

    def _execute(self, query):
        with ydb.QuerySessionPool(self.driver) as session_pool:
            return session_pool.execute_with_retries(query)

    def _write(self, rows):
        ids = set(range(len(rows), len(rows) + ROWS_PER_WRITE))
        column_types = ydb.BulkUpsertColumns().add_column("id", ydb.PrimitiveType.Uint64)
        self.driver.table_client.bulk_upsert(self.table_path, [{"id": i} for i in ids], column_types)
        return rows | ids

    def _read(self):
        return {row.id for result_set in self._execute(f"SELECT id FROM `{self.table_path}`") for row in result_set.rows}

    def test_every_row_after_version_changes(self):
        self.table_path = f"{self.database_path}/table"
        self._execute(f"CREATE TABLE `{self.table_path}` (id Uint64 NOT NULL, PRIMARY KEY (id)) WITH (STORE = COLUMN)")
        rows = set()
        for binary, flag in self.steps:
            self._start(binary, flag)
            assert self._read() == rows
            rows = self._write(rows)
            assert self._read() == rows
