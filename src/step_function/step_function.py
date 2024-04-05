from os import stat_result


class State:
    """Base class for Task and Parallel states."""

    def __init__(self, name: str, execution_time: float = 0):
        self.name = name
        self.execution_time = execution_time

    def get_execution_time(self) -> float:
        return self.execution_time


class Task(State):
    """Task state in Step Function."""

    def __init__(self, name: str):
        super().__init__(name)


class Parallel(State):
    """Parallel state, holding multiple parallel workflows."""

    def __init__(self, name: str):
        super().__init__(name)
        self.branches = []

    def add_branch(self, workflow: "Workflow"):
        self.branches.append(workflow)

    def get_execution_time(self) -> float:
        """Returns the longest execution time among all branches."""
        max_time = 0
        for branch in self.branches:
            branch_time = branch.get_total_execution_time()
            max_time = max(max_time, branch_time)
        return max_time


class Workflow:
    """A workflow, containing a sequence of states."""

    def __init__(self):
        self.states = []

    def add_state(self, state: State):
        self.states.append(state)

    def get_execution_time(self) -> float:
        total_time = sum(state.get_execution_time() for state in self.states)
        return total_time
