import os
from uuid import uuid4
from typing import List

from kedro.io import JSONLocalDataSet
from kedro.io.core import DataSetError

NODE_STATE_FILE_PATH = os.getcwd() + '/data/01_raw/node_state.json'


class IdempotentStateStorage:

    def __init__(self, run_id_state=None, input_state=None):
        if run_id_state is None:
            run_id_state = {
                # "node1": "qwer-qwer-wqer",
                # "node2": "adsf-asdf-adsf"
            }
        if input_state is None:
            input_state = {
                # "node1": {},
                # "node2": {
                #     "node1": "qwer-qwer-qwer"
            }

        self.run_id_state = run_id_state
        self.input_state = input_state

    @staticmethod
    def generate_run_id():
        return str(uuid4())

    def update_run_id(self, node: str, run_id: str = None):
        if run_id is None:
            run_id = IdempotentStateStorage.generate_run_id()
        self.run_id_state[node] = run_id

    def update_inputs(self, node: str, inputs: List[str]):
        self.input_state[node] = {
            target_input: self.retrieve_run_id(target_input)
            for target_input in inputs
        }

    def retrieve_run_id(self, node):
        k = self.run_id_state.get(node)
        if k is None:
            k = IdempotentStateStorage.generate_run_id()
            self.run_id_state[node] = k
        return k

    def get_expected_inputs(self, node):
        return self.input_state.get(node, {})

    def node_inputs_have_changed(self, node, inputs: List[str]):
        expect_input_items = self.get_expected_inputs(node)

        # If any inputs are new or removed, node should be run
        expected_inputs = [node for node in expect_input_items.keys()]
        if sorted(expected_inputs) != sorted(inputs):
            return True

        expected_run_ids = set(expect_input_items.values())

        actual_run_ids = set([
            self.retrieve_run_id(input_node)
            for input_node in inputs
        ])

        return actual_run_ids != expected_run_ids
