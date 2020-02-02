import pytest
from uuid import uuid4

from kedro.contrib.idempotent.idempotent_state_storage import IdempotentStateStorage


@pytest.fixture
def state_storage_no_update():
    # Pipeline: node3 -> node2 -> node1
    nodes = ['node1', 'node2', 'node3']

    run_id_state = {
        node: str(uuid4())
        for index, node in enumerate(nodes)
    }

    input_state = {
        node: {
            nodes[input_node_index]: run_id_state[nodes[input_node_index]]
            for input_node_index in range(node_index)
        }
        for node_index, node in enumerate(nodes)
    }

    return {
        "run_id_state": run_id_state,
        "input_state": input_state
    }


@pytest.fixture
def state_storage_with_update(state_storage_no_update):
    state = state_storage_no_update.copy()

    for node in state['run_id_state'].keys():
        state['run_id_state'][node] = str(uuid4())

    return state


class TestIdempotentStateStorage:

    def test_node_inputs_have_not_changed(self, state_storage_no_update):
        storage = IdempotentStateStorage(**state_storage_no_update)
        assert not storage.node_inputs_have_changed('node3', ['node2', 'node1'])

    def test_node_inputs_changed_with_input_removement(self, state_storage_no_update):
        storage = IdempotentStateStorage(**state_storage_no_update)
        assert storage.node_inputs_have_changed('node3', ['node2'])

    def test_node_inputs_run_id_changed(self, state_storage_with_update):
        storage = IdempotentStateStorage(**state_storage_with_update)
        assert storage.node_inputs_have_changed('node3', ['node2', 'node1'])

    def test_update_run_id_for_node(self, state_storage_no_update):
        new_run_id = str(uuid4())
        storage = IdempotentStateStorage(**state_storage_no_update)
        storage.update_run_id("node1", new_run_id)

        assert storage.run_id_state['node1'] == new_run_id
        assert storage.node_inputs_have_changed('node3', ['node2', 'node1'])

    def test_update_key(self, state_storage_with_update):
        storage = IdempotentStateStorage(**state_storage_with_update)
        storage.update_inputs('node3', ['node2', 'node1'])
        assert not storage.node_inputs_have_changed('node3', ['node2', 'node1'])
