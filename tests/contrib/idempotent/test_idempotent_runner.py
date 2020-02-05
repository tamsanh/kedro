# Copyright 2018-2019 QuantumBlack Visual Analytics Limited
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
# OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE, AND
# NONINFRINGEMENT. IN NO EVENT WILL THE LICENSOR OR OTHER CONTRIBUTORS
# BE LIABLE FOR ANY CLAIM, DAMAGES, OR OTHER LIABILITY, WHETHER IN AN
# ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF, OR IN
# CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
#
# The QuantumBlack Visual Analytics Limited ("QuantumBlack") name and logo
# (either separately or in combination, "QuantumBlack Trademarks") are
# trademarks of QuantumBlack. The License does not grant you any right or
# license to the QuantumBlack Trademarks. You may not use the QuantumBlack
# Trademarks or any confusingly similar mark as a trademark for your product,
#     or use the QuantumBlack Trademarks in any other manner that might cause
# confusion in the marketplace, including but not limited to in advertising,
# on websites, or on software.
#
# See the License for the specific language governing permissions and
# limitations under the License.

# pylint: disable=unused-argument
from random import random
from typing import Any, Dict
from datetime import datetime
import pandas as pd
import pytest

from kedro.io import (
    AbstractDataSet,
    DataCatalog,
    DataSetError,
    LambdaDataSet,
    MemoryDataSet,
    TextLocalDataSet,
)
from kedro.pipeline import Pipeline, node
from kedro.contrib.idempotent.idempotent_runner import (
    IdempotentSequentialRunner,
    ForcedIdempotentSequentialRunner,
    get_hash_value,
)


@pytest.fixture
def memory_catalog():
    ds1 = MemoryDataSet({"data": 42})
    ds2 = MemoryDataSet([1, 2, 3, 4, 5])
    return DataCatalog({"ds1": ds1, "ds2": ds2})


@pytest.fixture
def pandas_df_feed_dict():
    pandas_df = pd.DataFrame({"Name": ["Alex", "Bob"], "Age": [15, 25]})
    return {"ds3": pandas_df}


@pytest.fixture
def conflicting_feed_dict(pandas_df_feed_dict):
    ds1 = MemoryDataSet({"data": 0})
    ds3 = pandas_df_feed_dict["ds3"]
    return {"ds1": ds1, "ds3": ds3}


def random_str():
    return str(random())


def source():
    return "stuff"


def identity(arg):
    return arg


def sink(arg):
    pass


def return_none(arg):
    return None


def multi_input_list_output(arg1, arg2):
    return [arg1, arg2]


@pytest.fixture
def branchless_no_input_pipeline():
    """The pipeline runs in the order A->B->C->D->E."""
    return Pipeline(
        [
            node(identity, "D", "E", name="node1"),
            node(identity, "C", "D", name="node2"),
            node(identity, "A", "B", name="node3"),
            node(identity, "B", "C", name="node4"),
            node(random, None, "A", name="node5"),
        ]
    )


@pytest.fixture
def branchless_pipeline():
    return Pipeline(
        [
            node(identity, "ds1", "ds2", name="node1"),
            node(identity, "ds2", "ds3", name="node2"),
        ]
    )


@pytest.fixture
def saving_result_pipeline():
    return Pipeline([node(identity, "ds", "dsX")])


@pytest.fixture
def saving_none_pipeline():
    return Pipeline(
        [node(random, None, "A"), node(return_none, "A", "B"), node(identity, "B", "C")]
    )


@pytest.fixture
def input_filepath_txt(tmp_path):
    return str(tmp_path / "input.txt")


@pytest.fixture
def output_filepath_txt(tmp_path):
    return str(tmp_path / "output.txt")


@pytest.fixture(params=[dict()])
def txt_input_data_set(input_filepath_txt, request):
    return TextLocalDataSet(filepath=input_filepath_txt, **request.param)


@pytest.fixture(params=[dict()])
def txt_output_data_set(output_filepath_txt, request):
    return TextLocalDataSet(filepath=output_filepath_txt, **request.param)


def create_stateful_identity():
    state = {"runs": 0}

    def _stateful_identity(arg):
        state["runs"] = state["runs"] + 1
        return arg

    return state, _stateful_identity


@pytest.fixture(scope="class")
def dict_data():
    dict_1 = {"a": [1.11, 2.22], "b": {"c": datetime.now()}, "d": "hello"}
    dict_2 = dict_1.copy()
    dict_2["d"] = "world"
    return {"1": dict_1, "2": dict_2}


@pytest.fixture(scope="class")
def list_data():
    list_1 = [1.11, datetime.now(), "hello", {"a": datetime.date(datetime.now())}]

    list_2 = list_1.copy()
    list_2.append("1")
    return {"1": list_1, "2": list_2}


class TestGetHashValue:
    def test_dict(self, dict_data):
        assert get_hash_value(dict_data["1"]) == get_hash_value(dict_data["1"])
        assert get_hash_value(dict_data["2"]) == get_hash_value(dict_data["2"])
        assert get_hash_value(dict_data["1"]) != get_hash_value(dict_data["2"])

    def test_list(self, list_data):
        assert get_hash_value(list_data["1"]) == get_hash_value(list_data["1"])
        assert get_hash_value(list_data["2"]) == get_hash_value(list_data["2"])
        assert get_hash_value(list_data["1"]) != get_hash_value(list_data["2"])

    def test_df(self, dict_data, list_data):
        df_1 = pd.DataFrame(
            {
                "dict": [dict_data["1"], dict_data["2"]],
                "list": [list_data["1"], list_data["2"]],
                "dates": [datetime.now(), datetime.date(datetime.now())],
            }
        )

        df_2 = pd.DataFrame(
            {"int": [1, 2, 3], "float": [1.1, 2.2, 3.3], "string": ["aa", "bb", "cc"]}
        )

        assert get_hash_value(df_1) == get_hash_value(df_1)
        assert get_hash_value(df_2) == get_hash_value(df_2)
        assert get_hash_value(df_1) != get_hash_value(df_2)


class TestForcedSeqentialRunner:
    def test_no_input_change(
        self, txt_input_data_set, txt_output_data_set, output_filepath_txt
    ):
        """Every single node should be run"""

        catalog = DataCatalog(
            {
                "memory": MemoryDataSet(data="24"),
                "input_ds": txt_input_data_set,
                "output_ds": txt_output_data_set,
            }
        )

        state, indentity_with_side_effect = create_stateful_identity()

        pipeline = Pipeline(
            [
                node(indentity_with_side_effect, "memory", "input_ds", name="node1"),
                node(indentity_with_side_effect, "input_ds", "output_ds", name="node2"),
            ]
        )

        runner = ForcedIdempotentSequentialRunner()

        runner.run(pipeline, catalog)
        run_id_state_round_1 = runner.state_storage.run_id_state.copy()

        runner.run(pipeline, catalog)
        run_id_state_round_2 = runner.state_storage.run_id_state.copy()

        assert state["runs"] == 4

        assert run_id_state_round_1["memory"] == run_id_state_round_2["memory"]
        assert run_id_state_round_1["input_ds"] != run_id_state_round_2["input_ds"]
        assert run_id_state_round_1["output_ds"] != run_id_state_round_2["output_ds"]

    def test_no_mds_input_change(self, txt_output_data_set, output_filepath_txt):
        """Every single node should be run"""

        state, indentity_with_side_effect = create_stateful_identity()

        catalog = DataCatalog(
            {
                "m1": MemoryDataSet(data="42"),
                "m2": MemoryDataSet(),
                "output_ds": txt_output_data_set,
            }
        )

        pipeline = Pipeline(
            [
                node(indentity_with_side_effect, "m1", "m2", name="node1"),
                node(indentity_with_side_effect, "m2", "output_ds", name="node2"),
            ]
        )

        runner = ForcedIdempotentSequentialRunner()

        runner.run(pipeline, catalog)
        run_id_state_round_1 = runner.state_storage.run_id_state.copy()

        runner.run(pipeline, catalog)
        run_id_state_round_2 = runner.state_storage.run_id_state.copy()

        assert state["runs"] == 4
        assert run_id_state_round_1["m2"] == run_id_state_round_2["m2"]
        assert run_id_state_round_1["output_ds"] != run_id_state_round_2["output_ds"]


class TestSeqentialRunnerHashingValue:
    def test_date_object_in_dict(self):
        input_data = {
            "today": datetime.date(datetime.now()),
            "str": "hello",
            "list": [1.11, "world", {"hello world": datetime.now()}],
        }

        state, indentity_with_side_effect = create_stateful_identity()

        pipeline = Pipeline([node(indentity_with_side_effect, "m1", "m2")])

        catalog = DataCatalog(
            {"m1": MemoryDataSet(data=input_data), "m2": MemoryDataSet()}
        )

        runner = IdempotentSequentialRunner()

        runner.run(pipeline, catalog)
        count_round_1 = state["runs"]
        run_id_state_round_1 = runner.state_storage.run_id_state.copy()

        runner.run(pipeline, catalog)
        count_round_2 = state["runs"]
        run_id_state_round_2 = runner.state_storage.run_id_state.copy()

        assert count_round_1 == 1
        assert count_round_2 == 2
        assert run_id_state_round_1["m2"] == run_id_state_round_2["m2"]
        assert catalog.load("m2") == input_data

    def test_date_object_in_list(self):
        state, indentity_with_side_effect = create_stateful_identity()

        input_data = [
            "hello",
            123.456,
            4,
            datetime.date(datetime.now()),
            ["world", {"hello world": datetime.now()}],
        ]

        pipline = Pipeline([node(indentity_with_side_effect, "m1", "m2")])

        catalog = DataCatalog(
            {"m1": MemoryDataSet(data=input_data), "m2": MemoryDataSet()}
        )

        runner = IdempotentSequentialRunner()

        runner.run(pipline, catalog)
        count_round_1 = state["runs"]
        run_id_state_round_1 = runner.state_storage.run_id_state.copy()

        runner.run(pipline, catalog)
        count_round_2 = state["runs"]
        run_id_state_round_2 = runner.state_storage.run_id_state.copy()

        assert count_round_1 == 1
        assert count_round_2 == 2
        assert run_id_state_round_1["m2"] == run_id_state_round_2["m2"]
        assert catalog.load("m2") == input_data

    def test_date_object_in_df(self):
        state, indentity_with_side_effect = create_stateful_identity()

        input_data = pd.DataFrame(
            {
                "hello": [123.456, 4],
                "date": [datetime.date(datetime.now()), datetime.now()],
                "list": [[1, 2], [3, 4]],
            }
        )

        pipeline = Pipeline([node(indentity_with_side_effect, "m1", "m2")])

        catalog = DataCatalog(
            {"m1": MemoryDataSet(data=input_data), "m2": MemoryDataSet()}
        )

        runner = IdempotentSequentialRunner()

        runner.run(pipeline, catalog)
        count_round_1 = state["runs"]
        run_id_state_round_1 = runner.state_storage.run_id_state.copy()

        runner.run(pipeline, catalog)
        count_round_2 = state["runs"]
        run_id_state_round_2 = runner.state_storage.run_id_state.copy()

        assert count_round_1 == 1
        assert count_round_2 == 2
        assert run_id_state_round_1["m2"] == run_id_state_round_2["m2"]
        assert catalog.load("m2").shape == input_data.shape


class TestSeqentialRunnerMutipleRun:
    def test_output_mds(self):
        input_data = 24

        state, indentity_with_side_effect = create_stateful_identity()

        catalog = DataCatalog(
            {"m1": MemoryDataSet(data=input_data), "m2": MemoryDataSet()}
        )

        pipeline = Pipeline([node(indentity_with_side_effect, "m1", "m2")])

        runner = IdempotentSequentialRunner()
        runner.run(pipeline, catalog)
        count_round_1 = state["runs"]
        run_id_state_round_1 = runner.state_storage.run_id_state.copy()

        runner.run(pipeline, catalog)
        count_round_2 = state["runs"]
        run_id_state_round_2 = runner.state_storage.run_id_state.copy()

        assert count_round_1 != count_round_2
        assert run_id_state_round_1["m2"] == run_id_state_round_2["m2"]

    def test_changed_mds_output(self, branchless_no_input_pipeline):
        """If a node output as least 1 MemoryDataSet, it will be run regardless of updates from its inputs"""

        runner = IdempotentSequentialRunner()

        runner.run(branchless_no_input_pipeline, DataCatalog())
        run_id_state_round_1 = runner.state_storage.run_id_state.copy()

        runner.run(branchless_no_input_pipeline, DataCatalog())
        run_id_state_round_2 = runner.state_storage.run_id_state.copy()

        for dms_input in ["A", "B", "C", "D", "E"]:
            assert run_id_state_round_1[dms_input] != run_id_state_round_2[dms_input]

    def test_input_change(self, txt_input_data_set, txt_output_data_set):
        """Nodes with no MemoryDataset as outputs, but has changes in inputs, should be run."""

        catalog = DataCatalog(
            {"input_ds": txt_input_data_set, "output_ds": txt_output_data_set}
        )

        pipeline = Pipeline(
            [
                node(random_str, None, "memory", name="node1"),
                node(identity, "memory", "input_ds", name="node2"),
                node(identity, "input_ds", "output_ds", name="node3"),
            ]
        )

        runner = IdempotentSequentialRunner()

        runner.run(pipeline, catalog)
        run_id_state_round_1 = runner.state_storage.run_id_state.copy()

        runner.run(pipeline, catalog)
        run_id_state_round_2 = runner.state_storage.run_id_state.copy()

        # node1 outputs MemoryDataSet, will be run
        assert run_id_state_round_1["memory"] != run_id_state_round_2["memory"]
        # node2's input has changed, will be run
        assert run_id_state_round_1["input_ds"] != run_id_state_round_2["input_ds"]
        # node3's input has changed, will be run
        assert run_id_state_round_1["output_ds"] != run_id_state_round_2["output_ds"]

    def test_no_input_change(
        self, txt_input_data_set, txt_output_data_set, output_filepath_txt
    ):
        """Nodes with no MemoryDataset as outputs, and has no changes in inputs, should not be run"""

        input_data = "24"

        catalog = DataCatalog(
            {
                "memory": MemoryDataSet(data=input_data),
                "input_ds": txt_input_data_set,
                "output_ds": txt_output_data_set,
            }
        )

        pipeline = Pipeline(
            [
                node(identity, "memory", "input_ds", name="node1"),
                node(identity, "input_ds", "output_ds", name="node2"),
            ]
        )

        runner = IdempotentSequentialRunner()

        runner.run(pipeline, catalog)
        run_id_state_round_1 = runner.state_storage.run_id_state.copy()

        runner.run(pipeline, catalog)
        run_id_state_round_2 = runner.state_storage.run_id_state.copy()

        # assert Path(output_filepath_txt).read_text("utf-8") == input_data

        assert run_id_state_round_1["memory"] == run_id_state_round_2["memory"]
        # node1 won't be run
        assert run_id_state_round_1["input_ds"] == run_id_state_round_2["input_ds"]
        # node2 won't be run
        assert run_id_state_round_1["output_ds"] == run_id_state_round_2["output_ds"]

    def test_no_mds_input_change(self, txt_output_data_set, output_filepath_txt):
        """
        Node with MemoryDataSet input should not be run
        as long as the input has not changed.

        Node produces that intermediate MemoryDataSet should run.
        """
        catalog = DataCatalog(
            {
                "m1": MemoryDataSet(data="42"),
                "m2": MemoryDataSet(),
                "output_ds": txt_output_data_set,
            }
        )

        pipeline = Pipeline(
            [
                node(identity, "m1", "m2", name="node1"),
                node(identity, "m2", "output_ds", name="node2"),
            ]
        )

        runner = IdempotentSequentialRunner()

        runner.run(pipeline, catalog)
        run_id_state_round_1 = runner.state_storage.run_id_state.copy()

        runner.run(pipeline, catalog)
        run_id_state_round_2 = runner.state_storage.run_id_state.copy()

        # node1 outputs MemoryDataSet, will be run
        assert run_id_state_round_1["m2"] == run_id_state_round_2["m2"]
        # node2 will not be run
        assert run_id_state_round_1["output_ds"] == run_id_state_round_2["output_ds"]

    def test_mds_change(self, txt_output_data_set):
        """Node with a changed MemoryDataSet input should be run."""
        catalog = DataCatalog({"m1": MemoryDataSet(), "output_ds": txt_output_data_set})

        pipeline = Pipeline(
            [
                node(random_str, None, "m1", name="node1"),
                node(identity, "m1", "output_ds", name="node2"),
            ]
        )

        runner = IdempotentSequentialRunner()

        runner.run(pipeline, catalog)
        run_id_state_round_1 = runner.state_storage.run_id_state.copy()

        runner.run(pipeline, catalog)
        run_id_state_round_2 = runner.state_storage.run_id_state.copy()

        # node1 outputs MemoryDataSet, also its input has changed, will be run
        assert run_id_state_round_1["m1"] != run_id_state_round_2["m1"]
        # node2's input has changed, will be run
        assert run_id_state_round_1["output_ds"] != run_id_state_round_2["output_ds"]


class TestSeqentialRunnerBranchlessPipeline:
    def test_no_input_seq(self, branchless_no_input_pipeline):
        outputs = IdempotentSequentialRunner().run(
            branchless_no_input_pipeline, DataCatalog()
        )
        assert "E" in outputs
        assert len(outputs) == 1

    def test_no_data_sets(self, branchless_pipeline):
        catalog = DataCatalog({}, {"ds1": 42})
        outputs = IdempotentSequentialRunner().run(branchless_pipeline, catalog)
        assert "ds3" in outputs
        assert outputs["ds3"] == 42

    def test_no_feed(self, memory_catalog, branchless_pipeline):
        outputs = IdempotentSequentialRunner().run(branchless_pipeline, memory_catalog)
        assert "ds3" in outputs
        assert outputs["ds3"]["data"] == 42

    def test_node_returning_none(self, saving_none_pipeline):
        pattern = "Saving `None` to a `DataSet` is not allowed"
        with pytest.raises(DataSetError, match=pattern):
            IdempotentSequentialRunner().run(saving_none_pipeline, DataCatalog())

    def test_result_saved_not_returned(self, saving_result_pipeline):
        """The pipeline runs ds->dsX but save does not save the output."""

        def _load():
            return 0

        def _save(arg):
            assert arg == 0

        catalog = DataCatalog(
            {
                "ds": LambdaDataSet(load=_load, save=_save),
                "dsX": LambdaDataSet(load=_load, save=_save),
            }
        )
        output = IdempotentSequentialRunner().run(saving_result_pipeline, catalog)
        assert output == {}


@pytest.fixture
def unfinished_outputs_pipeline():
    return Pipeline(
        [
            node(identity, dict(arg="ds4"), "ds8", name="node1"),
            node(sink, "ds7", None, name="node2"),
            node(multi_input_list_output, ["ds3", "ds4"], ["ds6", "ds7"], name="node3"),
            node(identity, "ds2", "ds5", name="node4"),
            node(identity, "ds1", "ds4", name="node5"),
        ]
    )  # Outputs: ['ds8', 'ds5', 'ds6'] == ['ds1', 'ds2', 'ds3']


class TestSeqentialRunnerBranchedPipeline:
    def test_input_seq(
        self, memory_catalog, unfinished_outputs_pipeline, pandas_df_feed_dict
    ):
        memory_catalog.add_feed_dict(pandas_df_feed_dict, replace=True)
        outputs = IdempotentSequentialRunner().run(
            unfinished_outputs_pipeline, memory_catalog
        )
        assert set(outputs.keys()) == {"ds8", "ds5", "ds6"}
        # the pipeline runs ds2->ds5
        assert outputs["ds5"] == [1, 2, 3, 4, 5]
        assert isinstance(outputs["ds8"], dict)
        # the pipeline runs ds1->ds4->ds8
        assert outputs["ds8"]["data"] == 42
        # the pipline runs ds3
        assert isinstance(outputs["ds6"], pd.DataFrame)

    def test_conflict_feed_catalog(
        self, memory_catalog, unfinished_outputs_pipeline, conflicting_feed_dict
    ):
        """ds1 and ds3 will be replaced with new inputs."""
        memory_catalog.add_feed_dict(conflicting_feed_dict, replace=True)
        outputs = IdempotentSequentialRunner().run(
            unfinished_outputs_pipeline, memory_catalog
        )
        assert isinstance(outputs["ds8"], dict)
        assert outputs["ds8"]["data"] == 0
        assert isinstance(outputs["ds6"], pd.DataFrame)

    def test_unsatisfied_inputs(self, unfinished_outputs_pipeline):
        """ds1, ds2 and ds3 were not specified."""
        with pytest.raises(ValueError, match=r"not found in the DataCatalog"):
            IdempotentSequentialRunner().run(unfinished_outputs_pipeline, DataCatalog())


class LoggingDataSet(AbstractDataSet):
    def __init__(self, log, name, value=None):
        self.log = log
        self.name = name
        self.value = value

    def _load(self) -> Any:
        self.log.append(("load", self.name))
        return self.value

    def _save(self, data: Any) -> None:
        self.value = data

    def _release(self) -> None:
        self.log.append(("release", self.name))
        self.value = None

    def _describe(self) -> Dict[str, Any]:
        return {}


class TestSequentialRunnerRelease:
    def test_dont_release_inputs_and_outputs(self):
        log = []
        pipeline = Pipeline(
            [node(identity, "in", "middle"), node(identity, "middle", "out")]
        )
        catalog = DataCatalog(
            {
                "in": LoggingDataSet(log, "in", "stuff"),
                "middle": LoggingDataSet(log, "middle"),
                "out": LoggingDataSet(log, "out"),
            }
        )
        IdempotentSequentialRunner().run(pipeline, catalog)

        # we don't want to see release in or out in here
        assert log == [("load", "in"), ("load", "middle"), ("release", "middle")]

    def test_release_at_earliest_opportunity(self):
        log = []
        pipeline = Pipeline(
            [
                node(source, None, "first"),
                node(identity, "first", "second"),
                node(sink, "second", None),
            ]
        )
        catalog = DataCatalog(
            {
                "first": LoggingDataSet(log, "first"),
                "second": LoggingDataSet(log, "second"),
            }
        )
        IdempotentSequentialRunner().run(pipeline, catalog)

        # we want to see "release first" before "load second"
        assert log == [
            ("load", "first"),
            ("release", "first"),
            ("load", "second"),
            ("release", "second"),
        ]

    def test_count_multiple_loads(self):
        log = []
        pipeline = Pipeline(
            [
                node(source, None, "dataset"),
                node(sink, "dataset", None, name="bob"),
                node(sink, "dataset", None, name="fred"),
            ]
        )
        catalog = DataCatalog({"dataset": LoggingDataSet(log, "dataset")})
        IdempotentSequentialRunner().run(pipeline, catalog)

        # we want to the release after both the loads
        assert log == [("load", "dataset"), ("load", "dataset"), ("release", "dataset")]

    def test_release_transcoded(self):
        log = []
        pipeline = Pipeline(
            [node(source, None, "ds@save"), node(sink, "ds@load", None)]
        )
        catalog = DataCatalog(
            {
                "ds@save": LoggingDataSet(log, "save"),
                "ds@load": LoggingDataSet(log, "load"),
            }
        )

        IdempotentSequentialRunner().run(pipeline, catalog)

        # we want to see both datasets being released
        assert log == [("release", "save"), ("load", "load"), ("release", "load")]

    @pytest.mark.parametrize(
        "pipeline",
        [
            Pipeline([node(identity, "ds1", "ds2", confirms="ds1")]),
            Pipeline(
                [
                    node(identity, "ds1", "ds2"),
                    node(identity, "ds2", None, confirms="ds1"),
                ]
            ),
        ],
    )
    def test_confirms(self, mocker, pipeline):
        fake_dataset_instance = mocker.Mock()
        catalog = DataCatalog(data_sets={"ds1": fake_dataset_instance})
        IdempotentSequentialRunner().run(pipeline, catalog)
        fake_dataset_instance.confirm.assert_called_once_with()
