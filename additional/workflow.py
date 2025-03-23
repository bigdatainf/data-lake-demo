# Example 5: Implementing a Simple Airflow-like Workflow
# workflow.py

import time
from datetime import datetime

class Task:
    def __init__(self, name, callable_func, dependencies=None):
        self.name = name
        self.callable_func = callable_func
        self.dependencies = dependencies or []
        self.status = "not_started"
        self.start_time = None
        self.end_time = None

    def execute(self):
        self.status = "running"
        self.start_time = datetime.now()
        try:
            print(f"Executing task: {self.name}")
            self.callable_func()
            self.status = "success"
        except Exception as e:
            self.status = "failed"
            print(f"Task {self.name} failed: {e}")
            raise
        finally:
            self.end_time = datetime.now()
            duration = (self.end_time - self.start_time).total_seconds()
            print(f"Task {self.name} {self.status} in {duration:.2f} seconds")

class Workflow:
    def __init__(self, name):
        self.name = name
        self.tasks = {}

    def add_task(self, task):
        self.tasks[task.name] = task

    def run(self):
        print(f"Starting workflow: {self.name}")
        workflow_start = datetime.now()

        # Track completed tasks
        completed_tasks = set()

        # Continue until all tasks are completed or any fails
        while len(completed_tasks) < len(self.tasks):
            for task_name, task in self.tasks.items():
                # Skip completed tasks
                if task_name in completed_tasks:
                    continue

                # Check if all dependencies are completed
                deps_satisfied = all(dep in completed_tasks for dep in task.dependencies)

                if deps_satisfied and task.status == "not_started":
                    try:
                        task.execute()
                        completed_tasks.add(task_name)
                    except Exception:
                        print(f"Workflow {self.name} failed on task {task_name}")
                        return False

            # Avoid tight loop
            time.sleep(0.1)

        workflow_end = datetime.now()
        duration = (workflow_end - workflow_start).total_seconds()
        print(f"Workflow {self.name} completed successfully in {duration:.2f} seconds")
        return True

# Example usage of the workflow
def run_data_lake_workflow():
    # Import our task functions
    from scripts.01_ingest_data import main as ingest_data
    from scripts.02_transform_data import main as transform_data
    from scripts.03_query_data import main as query_data

    # Create a workflow
    workflow = Workflow("daily_data_processing")

    # Add tasks with dependencies
    workflow.add_task(Task("ingest_data", ingest_data))
    workflow.add_task(Task("transform_data", transform_data, dependencies=["ingest_data"]))
    workflow.add_task(Task("query_data", query_data, dependencies=["transform_data"]))

    # Run the workflow
    workflow.run()