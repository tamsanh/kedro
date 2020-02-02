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
"""``SequentialRunner`` is an ``AbstractRunner`` implementation. It can be
used to run the ``Pipeline`` in a sequential manner using a topological sort
of provided nodes.
"""

from collections import Counter
from itertools import chain
from typing import Dict, Any

from kedro.contrib.idempotent.idempotent_state_storage import IdempotentStateStorage
from kedro.io import AbstractDataSet, DataCatalog, MemoryDataSet
from kedro.pipeline import Pipeline
from kedro.pipeline.node import Node
from kedro.runner.runner import AbstractRunner


def run_node_idempotently(node: Node, catalog: DataCatalog, state: IdempotentStateStorage) -> Node:
    """Run a single `Node` with inputs from and outputs to the `catalog`.

    Args:
        node: The ``Node`` to run.
        catalog: A ``DataCatalog`` containing the node's inputs and outputs.
        state: An ``IdempotentStateStorage`` to reference node runs

    Returns:
        The node argument.

    """

    inputs = {name: catalog.load(name) for name in node.inputs}
    outputs = node.run(inputs)
    for name, data in outputs.items():
        catalog.save(name, data)
        state.update_run_id(name)
    for name in node.confirms:
        catalog.confirm(name)

    state.update_inputs(node.name, node.inputs)
    return node


class IdempotentSequentialRunner(AbstractRunner):
    """``IdempotentSequentialRunner`` is a ``SequentialRunner`` implementation.
    It can be used to run the ``Pipeline`` in an idempotent sequential manner using a
    topological sort of provided nodes.
    """

    def __init__(self):
        super().__init__()
        self.state_storage = IdempotentStateStorage()

    def run_idempotently(self, pipeline: Pipeline, catalog: DataCatalog) -> Dict[str, Any]:
        """Run only the nodes with updated inputs from the ``Pipeline`` using the
        ``DataSet``s provided by ``catalog`` and save results back to the same
        objects.

        Args:
            pipeline: The ``Pipeline`` to run.
            catalog: The ``DataCatalog`` from which to fetch data.
        Raises:
            ValueError: Raised when ``Pipeline`` inputs cannot be satisfied.

        Returns:
            Any node outputs that cannot be processed by the ``DataCatalog``.
            These are returned in a dictionary, where the keys are defined
            by the node outputs.

        """

        # Find all inputs that are parameters
        parameter_inputs = [
            i
            for i in pipeline.inputs()
            if i.startswith("params:")
        ]
        # Hash them as the key after loading
        for parameter_input in parameter_inputs:
            # Update our idempotency state with new hashes
            self.state_storage.update_run_id(parameter_input, str(hash(catalog.load(parameter_input))))

        # Then we can compare if inputs have changed

        def inputs_have_changes(node):
            return self.state_storage.node_inputs_have_changed(node.name, node.inputs)

        nodes_to_run = list(filter(inputs_have_changes, pipeline.nodes))
        to_rerun = pipeline.from_nodes(
            *list(map(lambda node: node.name, nodes_to_run))
        )

        return self.run(to_rerun, catalog)

    def create_default_data_set(self, ds_name: str) -> AbstractDataSet:
        """Factory method for creating the default data set for the runner.

        Args:
            ds_name: Name of the missing data set

        Returns:
            An instance of an implementation of AbstractDataSet to be used
            for all unregistered data sets.

        """
        return MemoryDataSet()

    def _run(self, pipeline: Pipeline, catalog: DataCatalog) -> None:
        """The method implementing sequential pipeline running.

        Args:
            pipeline: The ``Pipeline`` to run.
            catalog: The ``DataCatalog`` from which to fetch data.

        Raises:
            Exception: in case of any downstream node failure.
        """
        nodes = pipeline.nodes
        done_nodes = set()

        load_counts = Counter(chain.from_iterable(n.inputs for n in nodes))

        for exec_index, node in enumerate(nodes):
            try:
                run_node_idempotently(node, catalog, self.state_storage)
                done_nodes.add(node)
            except Exception:
                self._suggest_resume_scenario(pipeline, done_nodes)
                raise

            # decrement load counts and release any data sets we've finished with
            for data_set in node.inputs:
                load_counts[data_set] -= 1
                if load_counts[data_set] < 1 and data_set not in pipeline.inputs():
                    catalog.release(data_set)
            for data_set in node.outputs:
                if load_counts[data_set] < 1 and data_set not in pipeline.outputs():
                    catalog.release(data_set)

            self._logger.info(
                "Completed %d out of %d tasks", exec_index + 1, len(nodes)
            )
