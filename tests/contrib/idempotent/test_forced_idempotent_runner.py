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
from pathlib import Path

import pandas as pd
import pytest

from kedro.io import (
    DataCatalog,
    MemoryDataSet,
    TextLocalDataSet
)
from kedro.pipeline import Pipeline, node
from kedro.contrib.idempotent.forced_idempotent_runner import ForcedIdempotentSequentialRunner


def random_str():
    return str(random())


def identity(arg):
    return arg


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


output_mds_node_run_count = 0


def reset_counter():
    global output_mds_node_run_count
    output_mds_node_run_count = 0


def indentity_with_side_effect(arg):
    global output_mds_node_run_count
    output_mds_node_run_count += 1
    return arg


class TestForcedSeqentialRunnerMutipleRun:

    def test_no_input_change(self, txt_input_data_set, txt_output_data_set, output_filepath_txt):
        """Every single node should be run"""

        reset_counter()

        catalog = DataCatalog({
            "memory": MemoryDataSet(data="24"),
            "input_ds": txt_input_data_set,
            "output_ds": txt_output_data_set
        })

        pipeline = Pipeline([
            node(indentity_with_side_effect, "memory", "input_ds", name="node1"),
            node(indentity_with_side_effect, "input_ds", "output_ds", name="node2")
        ])

        runner = ForcedIdempotentSequentialRunner()

        runner.run(pipeline, catalog)
        run_id_state_round_1 = runner.state_storage.run_id_state.copy()

        runner.run(pipeline, catalog)
        run_id_state_round_2 = runner.state_storage.run_id_state.copy()

        assert output_mds_node_run_count == 4

        assert run_id_state_round_1['memory'] == run_id_state_round_2['memory']
        assert run_id_state_round_1['input_ds'] != run_id_state_round_2['input_ds']
        assert run_id_state_round_1['output_ds'] != run_id_state_round_2['output_ds']

    def test_no_mds_input_change(self, txt_output_data_set, output_filepath_txt):
        """Every single node should be run"""

        reset_counter()

        catalog = DataCatalog({
            "m1": MemoryDataSet(data="42"),
            "m2": MemoryDataSet(),
            "output_ds": txt_output_data_set
        })

        pipeline = Pipeline([
            node(identity, "m1", "m2", name="node1"),
            node(identity, "m2", "output_ds", name="node2")
        ])

        runner = ForcedIdempotentSequentialRunner()

        runner.run(pipeline, catalog)
        run_id_state_round_1 = runner.state_storage.run_id_state.copy()

        runner.run(pipeline, catalog)
        run_id_state_round_2 = runner.state_storage.run_id_state.copy()

        assert output_mds_node_run_count == 4
        assert run_id_state_round_1['m2'] == run_id_state_round_2['m2']
        assert run_id_state_round_1['output_ds'] != run_id_state_round_2['output_ds']
