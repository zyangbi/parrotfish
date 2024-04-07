from abc import abstractmethod
import boto3
import json
from typing import Any

from attr import define


class State:
    """Base class for Task and Parallel states."""

    def __init__(self, name: str):
        self.name = name

    @abstractmethod
    def get_execution_time(self) -> float:
        pass


class Task(State):
    """Task state in Step Function."""

    def __init__(self, name: str, arn: str):
        super().__init__(name)
        self.arn = arn
        self.param_function = 1  #####

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


def create_state(name, state_def: dict) -> State:
    """Create a State object from a state definition."""
    if state_def["Type"] == "Task":
        arn = state_def["Parameters"]["FunctionName"]
        return Task(name, arn)

    elif state_def["Type"] == "Parallel":
        parallel = Parallel(name)
        for branch_def in state_def["Branches"]:
            branch = create_workflow(branch_def)
            parallel.add_branch(branch)
        return parallel

    else:
        return State(name)  ## should throw an exception


def create_workflow(workflow_def: dict) -> Workflow:
    """Create a Workflow object from a workflow definition."""
    workflow = Workflow()

    state_name = workflow_def["StartAt"]  # starting state
    while 1:
        # add state to workflow
        state_def = workflow_def["States"][state_name]
        workflow.add_state(create_state(state_name, state_def))

        # go to next state
        if "Next" in state_def:
            state_name = state_def["Next"]
        elif "End" in state_def:
            break  # end of workflow
        else:
            break  ## should throw an exception

    return workflow


def get_step_func_def(arn: str) -> dict:
    """Get a step function's definition."""
    try:
        client = boto3.client("stepfunctions")
        response = client.describe_state_machine(stateMachineArn=arn)
        definition = json.loads(response["definition"])
        return definition
    except Exception as e:
        print(f"Error retrieving state machine: {e}")
        return []


arn = "arn:aws:states:us-west-2:898429789601:stateMachine:RegressionTuning"
step_function_def = get_step_func_def(arn)
workflow = create_workflow(step_function_def)


print(workflow)
