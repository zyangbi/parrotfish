from abc import ABC, abstractmethod

import numpy as np

from src.configuration.configuration_from_dict import ConfigurationFromDict
from src.parrotfish import Parrotfish


class State(ABC):
    """Base class for Task and Parallel states."""

    def __init__(self, name: str):
        self.name = name

    @abstractmethod
    def get_execution_time(self) -> float:
        pass

    @abstractmethod
    def get_cost(self) -> float:
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
            "termination_threshold": 2,
            "min_sample_per_config": 3,
            "dynamic_sampling_params": {
                "max_sample_per_config": 3,
                "coefficient_of_variation_threshold": 0.1,
            },
        }
        self.parrotfish = Parrotfish(ConfigurationFromDict(self.config))
        self.memory_space = self.parrotfish.explorer.memory_space

        min_memory = self.parrotfish.optimize(apply=False)
        self.index = np.where(self.memory_space == min_memory)[0][0]
        self.execution_times = self.parrotfish.param_function(self.memory_space)
        self.costs = self.execution_times * self.memory_space

    def get_execution_time(self) -> float:
        """Get the execution time at self.memory"""
        return self.execution_times[self.index]

    def get_cost(self) -> float:
        """Get the cost at self.memory"""
        return self.costs[self.index]

    def increase_memory(self) -> bool:
        """Increase memory size to the next index in memory space"""
        if self.index + 1 < len(self.memory_space):
            self.index = self.index + 1
            return True
        else:
            return False

    def decrease_memory(self) -> bool:
        """Decrease memory size to the previous index in memory space"""
        if self.index - 1 >= 0:
            self.index = self.index - 1
            return True
        else:
            return False


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

    def get_cost(self) -> float:
        return sum(branch.get_cost() for branch in self.branches)


class Workflow:
    """A workflow, containing a sequence of states."""

    def __init__(self):
        self.states: list[State] = []

    def add_state(self, state: State):
        self.states.append(state)

    def get_execution_time(self) -> float:
        total_time = sum(state.get_execution_time() for state in self.states)
        return total_time

    def get_cost(self) -> float:
        return sum(state.get_cost() for state in self.states)
