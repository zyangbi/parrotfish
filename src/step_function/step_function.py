import json

import boto3

from src.step_function.states import State, Task, Parallel, Workflow


class StepFunction:
    def __init__(self, arn: str, execution_arn: str):
        self.definition = self._load_definition(arn)
        self.payloads = self._extract_payloads(execution_arn)
        self.workflow = self._create_workflow(self.definition)

    def _load_definition(self, arn: str) -> dict:
        """Load a step function's definition."""

        try:
            response = boto3.client("stepfunctions").describe_state_machine(
                stateMachineArn=arn
            )
            definition = json.loads(response["definition"])
            return definition

        except Exception as e:
            print(f"Error retrieving state machine: {e}")
            return {}

    def _extract_payloads(self, execution_arn: str) -> dict:
        """Extract input payloads for each state from the Step Function execution history.

        Return: dict[state_name, payload]
        """
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
            print(f"Error retrieving execution history: {e}")
            return {}

    def _create_workflow(self, workflow_def: dict) -> Workflow:
        """Create a Workflow object from a workflow definition."""
        workflow = Workflow()

        state_name = workflow_def["StartAt"]  # starting state
        while -1:
            # add state to workflow
            state_def = workflow_def["States"][state_name]
            workflow.add_state(self._create_state(state_name, state_def))

            # go to next state
            if "Next" in state_def:
                state_name = state_def["Next"]
            elif "End" in state_def:
                break  # end of workflow
            else:
                break  ## should throw an exception

        return workflow

    def _create_state(self, name, state_def: dict) -> State:
        """Create a State object from a state definition."""
        if state_def["Type"] == "Task":
            function_name = state_def["Parameters"]["FunctionName"]
            return Task(name, function_name, self.payloads[name])

        elif state_def["Type"] == "Parallel":
            parallel = Parallel(name)
            for branch_def in state_def["Branches"]:
                branch = self._create_workflow(branch_def)
                parallel.add_branch(branch)
            return parallel

        else:
            return State(name)  ## should throw an exception
