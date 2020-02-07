import pytest
import pandas as pd
from uuid import uuid4
from datetime import datetime

from kedro.contrib.idempotent.idempotent_state_storage import (
    IdempotentStateStorage,
    IdempotentStateStorageLoadException,
    IdempotentStateStorageValueException
)
from kedro.io.data_catalog import DataCatalog, MemoryDataSet


@pytest.fixture
def state_storage_no_update():
    # Pipeline: node3 -> node2 -> node1
    nodes = ["node1", "node2", "node3"]

    run_id_state = {node: str(uuid4()) for index, node in enumerate(nodes)}

    input_state = {
        node: {
            nodes[input_node_index]: run_id_state[nodes[input_node_index]]
            for input_node_index in range(node_index)
        }
        for node_index, node in enumerate(nodes)
    }

    return {"run_id_state": run_id_state, "input_state": input_state}


@pytest.fixture
def state_storage_with_update(state_storage_no_update):
    state = state_storage_no_update.copy()

    for node in state["run_id_state"].keys():
        state["run_id_state"][node] = str(uuid4())

    return state


@pytest.fixture
def catalog_without_state_update(state_storage_no_update):
    return DataCatalog({
        "idempotent_state_storage": MemoryDataSet(data=state_storage_no_update)
    })


@pytest.fixture
def catalog_with_state_update(state_storage_with_update):
    return DataCatalog({
        "idempotent_state_storage": MemoryDataSet(data=state_storage_with_update)
    })


class TestInitIdempotentStateStorage:
    def test_no_catalog(self):
        with pytest.raises(IdempotentStateStorageLoadException):
            assert IdempotentStateStorage(DataCatalog())

    def test_not_dict(self):
        with pytest.raises(IdempotentStateStorageValueException):
            assert IdempotentStateStorage(DataCatalog({
                "idempotent_state_storage": MemoryDataSet(data="")
            }))


class TestIdempotentStateStorage:
    def test_node_inputs_have_not_changed(self, catalog_without_state_update):
        storage = IdempotentStateStorage(catalog_without_state_update)
        assert not storage.node_inputs_have_changed("node3", ["node2", "node1"])

    def test_node_inputs_changed_with_input_removement(self, catalog_without_state_update):
        storage = IdempotentStateStorage(catalog_without_state_update)
        assert storage.node_inputs_have_changed("node3", ["node2"])

    def test_node_inputs_run_id_changed(self, catalog_with_state_update):
        storage = IdempotentStateStorage(catalog_with_state_update)
        assert storage.node_inputs_have_changed("node3", ["node2", "node1"])

    def test_update_run_id(self, catalog_without_state_update, state_storage_no_update):
        storage = IdempotentStateStorage(catalog_without_state_update)
        storage.update_run_id("node1")
        assert storage.run_id_state["node1"] != state_storage_no_update['run_id_state']['node1']

    def test_update_run_id_with_data(self, catalog_without_state_update):
        new_data = 1
        storage = IdempotentStateStorage(catalog_without_state_update)
        storage.update_run_id("node1", new_data)
        assert storage.run_id_state['node1'] == IdempotentStateStorage.generate_run_id(new_data)
        assert storage.node_inputs_have_changed("node3", ["node2", "node1"])

    def test_update_key(self, catalog_with_state_update):
        storage = IdempotentStateStorage(catalog_with_state_update)
        storage.update_inputs("node3", ["node2", "node1"])
        assert not storage.node_inputs_have_changed("node3", ["node2", "node1"])


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


get_hash_value = IdempotentStateStorage.get_hash_value


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
