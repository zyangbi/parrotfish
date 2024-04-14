import json

import boto3

from src.step_function.states import State, Task, Parallel, Workflow
from src.logging_config import logger
from src.exception.step_function_error import StepFunctionError


class StepFunction:
    def __init__(self, arn: str, execution_arn: str):
        self.definition = self._load_definition(arn)
        self.payloads = self._extract_payloads(execution_arn)
        self.tasks: list[Task] = []
        self.workflow = self._create_workflow(self.definition)

    def reduce_execution_time(
        self, target: float, execution_times: list[float], costs: list[float]
    ) -> bool:
        execution_time = self.get_execution_time()
        while execution_time > target:
            if not self._increase_memory_size_once():
                break
            execution_time = self.get_execution_time()
            execution_times.append(execution_time)
            costs.append(self.get_cost())

        if self.get_execution_time() <= target:
            print("Finish")
            return True
        else:
            print("Execution time threshold too low")
            return False

    def _increase_memory_size_once(self) -> bool:
        """Increase the memory size and return the execution time improvement per unit cost."""
        execution_time = self.get_execution_time()

        min_ratio = float("inf")
        min_ratio_task = None
        for task in self.tasks:
            cost = task.get_cost()
            # increase memory
            if task.increase_memory():
                execution_time_new = self.get_execution_time()
                execution_time_diff = execution_time - execution_time_new  # positive
                cost_new = task.get_cost()
                cost_diff = cost_new - cost  # can be negative
                ratio = cost_diff / execution_time_diff
                # record the task with min cost per unit of execution time
                if ratio < min_ratio:
                    min_ratio = ratio
                    min_ratio_task = task
                task.decrease_memory()

        if min_ratio_task is not None:
            min_ratio_task.increase_memory()
            return True
        else:
            # all tasks' execution time is minimum
            return False

    def get_execution_time(self) -> float:
        return self.workflow.get_execution_time()

    def get_cost(self) -> float:
        return self.workflow.get_cost()

    def _load_definition(self, arn: str) -> dict:
        """Load a step function's definition."""
        try:
            response = boto3.client("stepfunctions").describe_state_machine(
                stateMachineArn=arn
            )
            definition = json.loads(response["definition"])
            return definition

        except Exception as e:
            logger.debug(e.args[0])
            raise StepFunctionError("Error loading definition.")

    def _extract_payloads(self, execution_arn: str) -> dict:
        """Extract input payloads for each state from the Step Function execution history."""
        try:
            history = boto3.client("stepfunctions").get_execution_history(
                executionArn=execution_arn
            )

            payloads = {}
            for event in history["events"]:
                if event["type"] == "TaskStateEntered":
                    name = event["stateEnteredEventDetails"]["name"]
                    payload = json.loads(event["stateEnteredEventDetails"]["input"])
                    payloads[name] = payload
            return payloads

        except Exception as e:
            logger.debug(e.args[0])
            raise StepFunctionError("Error extracting payloads.")

    def _create_workflow(self, workflow_def: dict) -> Workflow:
        """Create a Workflow object from a workflow definition."""
        # try:
        workflow = Workflow()

        state_name = workflow_def["StartAt"]  # starting state
        while True:
            # add state to workflow
            state_def = workflow_def["States"][state_name]
            workflow.add_state(self._create_state(state_name, state_def))

            # go to next state
            if "Next" in state_def:
                state_name = state_def["Next"]
            else:
                break  # end of workflow

        return workflow

        # except Exception as e:
        #     logger.debug(e.args[0])
        #     raise StepFunctionError("Error creating workflow")

    def _create_state(self, name, state_def: dict) -> State:
        """Create a State object from a state definition."""
        if state_def["Type"] == "Task":
            function_name = state_def["Parameters"]["FunctionName"]
            task = Task(name, function_name, self.payloads[name])
            self.tasks.append(task)
            return task

        elif state_def["Type"] == "Parallel":
            parallel = Parallel(name)
            for branch_def in state_def["Branches"]:
                branch = self._create_workflow(branch_def)
                parallel.add_branch(branch)
            return parallel

        else:
            raise StepFunctionError(
                "State definition only support Task and Map type states."
            )
