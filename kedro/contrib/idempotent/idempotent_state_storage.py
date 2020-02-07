import json
import pandas as pd
from uuid import uuid4
from typing import List, Any

from kedro.io.data_catalog import MemoryDataSet, DataCatalog

IDEMPOTENT_STATE_STORAGE_CATALOG_NAME = "idempotent_state_storage"


class IdempotentStateStorage:
    def __init__(self, catalog: DataCatalog, data_set_name: str = None):
        self.catalog = catalog
        if data_set_name is None:
            data_set_name = IDEMPOTENT_STATE_STORAGE_CATALOG_NAME
        self.data_set_name = data_set_name

        try:
            state_data = catalog.load(IDEMPOTENT_STATE_STORAGE_CATALOG_NAME)
        except Exception as e:
            raise IdempotentStateStorageLoadException(
                f"Failed to load DataCatalog({self.data_set_name})"
            )

        if type(state_data) is not dict:
            raise IdempotentStateStorageValueException(
                f"Content of DataCatalog({self.data_set_name}) should be a dictionary"
            )

        run_id_state = state_data.get(
            "run_id_state",
            {
                # "ds1": "qwer-qwer-wqer",
                # "ds2": "adsf-asdf-adsf"
            },
        )
        input_state = state_data.get(
            "input_state",
            {
                # "node1": {},
                # "node2": {
                #     "ds1": "qwer-qwer-qwer"
            },
        )
        nodes_have_been_run = state_data.get(
            "nodes_have_been_run",
            {
                # "node1": True
            },
        )

        self.run_id_state = run_id_state
        self.input_state = input_state
        self.nodes_have_been_run = nodes_have_been_run

    def save(self):
        state = {
            "run_id_state": self.run_id_state,
            "input_state": self.input_state,
            "nodes_have_been_run": self.nodes_have_been_run,
        }
        self.catalog.save(self.data_set_name, state)

    @staticmethod
    def generate_run_id(data: Any = None):
        if data is None:
            return str(uuid4())
        else:
            return IdempotentStateStorage.get_hash_value(data)

    @staticmethod
    def get_hash_value(data):
        hash_value = str(hash(json.dumps(data, sort_keys=True, default=str)))
        if type(data) == pd.DataFrame:
            try:
                hash_value = pd.util.hash_pandas_object(data, index=True).sum()
            except TypeError:
                pass
        return hash_value

    def update_run_id(self, data_set: str, data: Any = None):
        run_id = IdempotentStateStorage.generate_run_id(data)
        self.run_id_state[data_set] = run_id

    def update_inputs(self, node: str, inputs: List[str]):
        self.input_state[node] = {
            target_input: self.retrieve_run_id(target_input) for target_input in inputs
        }

    def update_node_run_status(self, node: str):
        self.nodes_have_been_run[node] = True

    def retrieve_run_id(self, node):
        k = self.run_id_state.get(node)
        if k is None:
            k = IdempotentStateStorage.generate_run_id()
            self.run_id_state[node] = k
        return k

    def get_expected_inputs(self, node: str):
        return self.input_state.get(node, {})

    def node_has_been_run(self, node: str):
        return self.nodes_have_been_run.get(node, False)

    def node_inputs_have_changed(self, node: str, inputs: List[str]):
        expect_input_items = self.get_expected_inputs(node)

        # If any inputs are new or removed, node should be run
        expected_inputs = [node for node in expect_input_items.keys()]
        if sorted(expected_inputs) != sorted(inputs):
            return True

        expected_run_ids = set(expect_input_items.values())

        actual_run_ids = set(
            [self.retrieve_run_id(input_node) for input_node in inputs]
        )

        return actual_run_ids != expected_run_ids


class IdempotentStateStorageLoadException(Exception):
    pass


class IdempotentStateStorageValueException(Exception):
    pass
