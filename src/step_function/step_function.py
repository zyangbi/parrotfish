import json
from concurrent.futures import ThreadPoolExecutor, as_completed

import boto3
import numpy as np
from jsonpath_ng import parse

from src.configuration import Configuration
from src.exception.step_function_error import StepFunctionError
from src.exploration.aws.aws_config_manager import AWSConfigManager
from src.logger import logger
from src.parrotfish import Parrotfish
from .states import State, Task, Parallel, Map, Workflow


class StepFunction:
    def __init__(self, config: any = None):
        if config is not None:
            self.config = config
            self.function_tasks_dict = {}
            self.aws_session = boto3.Session(region_name=config.region)
            self.definition = self._load_definition(config.arn)

    def optimize(self):
        for entry in self.config.payloads:
            self.function_tasks_dict = {}
            self.workflow = self._create_workflow(self.definition)
            self._set_workflow_payloads(self.workflow, entry["payload"])

            # optimize for cost using Parrotfish
            self._optimize_functions(self.function_tasks_dict)

            # optimize for execution time constraint
            self.optimize_whole_step_function()

    def _load_definition(self, arn: str) -> dict:
        """
        Loads a step function's definition from AWS Step Functions.

        Args:
            arn (str): The ARN of the Step Function.

        Returns:
            dict: The step function's definition.

        Raises:
            StepFunctionError: If an error occurred while loading the definition.
        """
        try:
            response = self.aws_session.client("stepfunctions").describe_state_machine(stateMachineArn=arn)
            definition = json.loads(response["definition"])
            return definition

        except Exception as e:
            logger.debug(f"Error loading definition: {e.args[0]}")
            raise e

    def _create_workflow(self, workflow_def: dict) -> Workflow:
        """
        Creates a Workflow object from a workflow definition.

        Args:
            workflow_def (dict): The definition of the workflow.

        Returns:
            Workflow: The created Workflow object.

        Raises:
            StepFunctionError: If an unsupported state type is encountered.
        """

        def _create_state(name, state_def: dict) -> State:
            """
            Creates a State object from a state definition.

            Args:
                name (str): The name of the state.
                state_def (dict): The definition of the state.

            Returns:
                State: The created State object.

            Raises:
                StepFunctionError: If an unsupported state type is encountered.
            """
            if state_def["Type"] == "Task":
                function_name = state_def["Parameters"]["FunctionName"]
                task = Task(name, function_name)

                if function_name not in self.function_tasks_dict:
                    self.function_tasks_dict[function_name] = []
                    config_manager = AWSConfigManager(function_name, self.aws_session)
                    config_manager.set_config(3008)  # set memory size to maximum
                self.function_tasks_dict[function_name].append(task)

                return task

            elif state_def["Type"] == "Parallel":
                parallel = Parallel(name)
                for branch_def in state_def["Branches"]:
                    branch = self._create_workflow(branch_def)
                    parallel.add_branch(branch)
                return parallel

            elif state_def["Type"] == "Map":
                map_state = Map(name)
                map_state.workflow_def = state_def["Iterator"]
                map_state.items_path = state_def["ItemsPath"]
                return map_state

            else:
                raise StepFunctionError("Only Support Task, Parallel, Map State Types.")

        workflow = Workflow()
        state_name = workflow_def["StartAt"]  # starting state
        while 1:
            # add state to workflow
            state_def = workflow_def["States"][state_name]
            workflow.add_state(_create_state(state_name, state_def))

            # go to next state
            if "Next" in state_def:
                state_name = state_def["Next"]
            elif "End" in state_def:
                break  # end of workflow
            else:
                break  ## should throw an exception

        return workflow

    def _set_workflow_payloads(self, workflow: Workflow, workflow_input: str) -> str:
        """
        Sets inputs for states in a workflow, chaining the output of each state to the input of the next.

        Args:
            workflow (Workflow): The workflow to initialize inputs for.
            workflow_input (str): The initial input for the workflow.

        Returns:
            str: The final output after initializing all workflow inputs.

        Raises:
            Exception: If an error occurs during the initialization of workflow inputs.
        """

        def _extract_items(input_data: str, items_path: str) -> list[str]:
            matches = parse(items_path).find(json.loads(input_data))[0]
            return [json.dumps(item_dict) for item_dict in matches.value]

        def _set_state_input(state: State, input: str) -> str:
            """
            Sets the input for a given state and returns its output.

            Args:
                state (State): The state to set the input for.
                input (str): The input data for the state.

            Returns:
                str: The output of the state after processing the input.

            Raises:
                Exception: If an error occurs during the setting of state input.
            """

            if isinstance(state, Task):
                try:
                    state.set_input(input)
                    output = state.get_output(self.aws_session)
                    return output
                except Exception as e:
                    logger.error(f"Error setting input of {state.name}: {e.args[0]}")
                    raise e

            elif isinstance(state, Parallel):
                outputs = []
                # Parallel execution of branches in a Parallel state
                with ThreadPoolExecutor(max_workers=10) as executor:
                    futures = {
                        executor.submit(self._set_workflow_payloads, branch, input): branch
                        for branch in state.branches
                    }
                    for future in as_completed(futures):
                        branch = futures[future]
                        try:
                            branch_output = future.result()
                            outputs.append(json.loads(branch_output))
                        except Exception as e:
                            logger.error(f"Error processing branch {branch}: {e.args[0]}")
                            raise e
                return json.dumps(outputs)

            elif isinstance(state, Map):
                inputs = _extract_items(input, state.items_path)
                state.iterations = [self._create_workflow(state.workflow_def) for _ in range(len(inputs))]

                outputs = []
                # Parallel execution of iterations in a Map state
                with ThreadPoolExecutor(max_workers=10) as executor:
                    futures = {
                        executor.submit(self._set_workflow_payloads, iteration, iteration_input): iteration_input
                        for iteration, iteration_input in zip(state.iterations, inputs)
                    }
                    for future in as_completed(futures):
                        iteration_input = futures[future]
                        try:
                            iteration_output = future.result()
                            outputs.append(json.loads(iteration_output))
                        except Exception as e:
                            logger.error(f"Error processing iteration with input {iteration_input}: {e.args[0]}")
                            raise e
                return json.dumps(outputs)

        logger.info("Start setting workflow inputs")
        payload = workflow_input
        for state in workflow.states:
            # The output of one function is the input of next function
            payload = _set_state_input(state, payload)
        logger.info("Finish setting workflow inputs\n")
        return payload

    def _optimize_functions(self, function_tasks_dict: dict):
        """
        Optimizes all Lambda functions using Parrotfish in parallel.
        """

        def _optimize_one_function(function_name: str, tasks: list[Task]) -> int:
            """
            Optimizes a single Lambda function using Parrotfish.

            Returns:
                The memory size that optimizes the cost of the function
            """

            try:
                config = {
                    "function_name": function_name,
                    "vendor": "AWS",
                    "region": self.config.region,
                    "payload": {},
                    "termination_threshold": self.config.termination_threshold,
                    "max_total_sample_count": self.config.max_total_sample_count,
                    "min_sample_per_config": self.config.min_sample_per_config,
                    "dynamic_sampling_params": self.config.dynamic_sampling_params,
                    "max_number_of_invocation_attempts": self.config.max_number_of_invocation_attempts,
                }
                parrotfish = Parrotfish(Configuration(config))
                collective_costs = np.zeros(len(parrotfish.explorer.memory_space))  # combined cost of all inputs

                # optimize each input of the function
                for task in tasks:
                    payload = {"payload": task.input, "weight": 1.0 / len(tasks)}
                    min_memory, param_function = parrotfish.optimize_one_payload(payload, collective_costs)
                    task.param_function = param_function
                    print(f"Optimized memory: {min_memory}MB, {task.name}. Input: {task.input}")

                # get the optimized memory size for the function
                memory_space = parrotfish.sampler.memory_space
                min_index = np.argmin(collective_costs[-len(memory_space):])
                min_memory = memory_space[min_index]
                for task in tasks:
                    # set optimized memory size for tasks
                    task.memory_size = min_memory
                    task.initial_memory_size = min_memory
                    task.max_memory_size = memory_space[-1]

            except Exception as e:
                logger.debug(f"Error optimizing function {function_name}: {e.args[0]}")
                raise e

        logger.info("Start optimizing all functions")

        # Run Parrotfish on all functions in parallel
        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = [executor.submit(_optimize_one_function, function, tasks)
                       for function, tasks in function_tasks_dict.items()]

            for future in as_completed(futures):
                future.result()

        logger.info("Finish optimizing all functions\n\n")

    def reset_memory_sizes(self):
        for function in self.function_tasks_dict:
            for task in self.function_tasks_dict[function]:
                task.memory_size = task.initial_memory_size

    def optimize_whole_step_function(self):
        workflow = self.workflow
        function_tasks_dict = self.function_tasks_dict
        memory_increment = self.config.memory_size_increment
        constraint_execution_time_threshold = self.config.constraint_execution_time_threshold

        if constraint_execution_time_threshold is None:
            logger.warning("No execution time threshold.")
            return

        critical_path_tasks, critical_path_time = workflow.get_critical_path()
        logger.info(
            f"Start optimizing step function for execution time, time: {critical_path_time}ms, threshold: {constraint_execution_time_threshold}ms, cost: {workflow.get_cost()}.")

        # Initialize cost increase dict
        cost_increases = {}
        for function in function_tasks_dict:
            cost_increases[function] = 0.0
            for task in function_tasks_dict[function]:
                original_cost = task.get_cost(task.memory_size)
                new_cost = task.get_cost(task.memory_size + memory_increment)
                cost_increases[function] += new_cost - original_cost

        # Update memory sizes until execution time threshold is reached
        while critical_path_time > constraint_execution_time_threshold:
            time_reductions = {}

            # Iterate over tasks on critical path and calculate time reductions for each function
            for task in critical_path_tasks:
                if task.memory_size + memory_increment > task.max_memory_size:
                    continue

                original_time = task.get_execution_time()
                new_time = task.get_execution_time(task.memory_size + memory_increment)

                if task.function_name not in time_reductions:
                    time_reductions[task.function_name] = 0.0
                time_reductions[task.function_name] += original_time - new_time

            # Find the function with the lowest cost to time reduction ratio
            best_function = None
            lowest_ratio = float('inf')
            for function_name in time_reductions:
                if time_reductions[function_name] > 0:
                    ratio = cost_increases[function_name] / time_reductions[function_name]
                    logger.debug(
                        f"ratio: {ratio}, {function_name}, {function_tasks_dict[function_name][0].memory_size}MB, {cost_increases[function_name]}, {time_reductions[function_name]}")

                    if ratio < lowest_ratio:
                        lowest_ratio = ratio
                        best_function = function_name

            # Increase memory size of best function, update cost increases
            if best_function:
                cost_increases[best_function] = 0.0
                for task in function_tasks_dict[best_function]:
                    task.increase_memory_size(memory_increment)
                    original_cost = task.get_cost()
                    new_cost = task.get_cost(task.memory_size + memory_increment)
                    cost_increases[best_function] += new_cost - original_cost
            else:
                raise StepFunctionError("Execution time threshold too low.")

            # Update critical path and time
            critical_path_tasks, critical_path_time = workflow.get_critical_path()
            logger.debug(
                f"Optimized function {best_function}, {task.memory_size}MB, time: {critical_path_time}ms, cost: {workflow.get_cost()}.\n")

        logger.info(
            f"Finish optimizing step function for execution time, time: {critical_path_time}ms, threshold: {constraint_execution_time_threshold}ms, cost: {workflow.get_cost()}.\n")

        print("Finish optimizing step function for execution time, optimized memory sizes:\n")
        for function in function_tasks_dict:
            print(f"{function}: {function_tasks_dict[function][0].memory_size}MB")
