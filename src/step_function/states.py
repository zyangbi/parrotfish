from abc import abstractmethod

from src.configuration.configuration_from_dict import ConfigurationFromDict
from src.parrotfish import Parrotfish


class State:
    """Base class for Task and Parallel states."""

    def __init__(self, name: str):
        self.name = name

    @abstractmethod
    def get_execution_time(self) -> float:
        pass


class Task(State):
    """Task state in Step Function."""

    def __init__(self, name: str, function_name: str, payload: str):
        super().__init__(name)
        self.function_name = function_name
        self.config = {
            "function_name": function_name,
            "vendor": "AWS",
            "region": "us-west-2",
            "payload": payload,
        }
        self.parrotfish = Parrotfish(ConfigurationFromDict(self.config))

    def get_execution_time(self) -> float:
        return 1


class Parallel(State):
    """Parallel state, holding multiple parallel workflows."""

    def __init__(self, name: str):
        super().__init__(name)
        self.branches: list[Workflow] = []

    def add_branch(self, workflow: "Workflow"):
        self.branches.append(workflow)

    def get_execution_time(self) -> float:
        """Returns the longest execution time among all branches."""
        max_time = 0
        for branch in self.branches:
            branch_time = branch.get_execution_time()
            max_time = max(max_time, branch_time)
        return max_time


class Workflow:
    """A workflow, containing a sequence of states."""

    def __init__(self):
        self.states: list[State] = []

    def add_state(self, state: State):
        self.states.append(state)

    def get_execution_time(self) -> float:
        total_time = sum(state.get_execution_time() for state in self.states)
        return total_time
