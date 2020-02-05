from uuid import uuid4
from typing import List


class IdempotentStateStorage:
    def __init__(self, run_id_state=None, input_state=None, nodes_have_been_run=None):
        if run_id_state is None:
            run_id_state = {
                # "ds1": "qwer-qwer-wqer",
                # "ds2": "adsf-asdf-adsf"
            }
        if input_state is None:
            input_state = {
                # "node1": {},
                # "node2": {
                #     "ds1": "qwer-qwer-qwer"
            }
        if nodes_have_been_run is None:
            nodes_have_been_run = {
                # "node1": True
            }

        self.run_id_state = run_id_state
        self.input_state = input_state
        self.nodes_have_been_run = nodes_have_been_run

    @staticmethod
    def generate_run_id():
        return str(uuid4())

    def update_run_id(self, node: str, run_id: str = None):
        if run_id is None:
            run_id = IdempotentStateStorage.generate_run_id()
        self.run_id_state[node] = run_id

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
