import logging
import asyncio
from typing import Dict, Any, List, Optional

import importlib

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
if not logger.hasHandlers():
    handler = logging.StreamHandler()
    formatter = logging.Formatter("[%(asctime)s] %(levelname)s %(name)s: %(message)s")
    handler.setFormatter(formatter)
    logger.addHandler(handler)

# Dynamically import agent modules
def _import_agent_class(module_path: str, class_name: str):
    try:
        module = importlib.import_module(module_path)
        agent_class = getattr(module, class_name)
        return agent_class
    except Exception as e:
        logger.error(f"Failed to import {class_name} from {module_path}: {e}")
        raise

# Import AttendanceClassificationAgent
AttendanceClassificationAgent = _import_agent_class(
    "code.employee_attendance_classification_agent_design.agent",
    "AttendanceClassificationAgent"
)

# Import EmployeeWorkAssignmentAgent
EmployeeWorkAssignmentAgent = _import_agent_class(
    "code.employee_work_assignment_agent_design.agent",
    "EmployeeWorkAssignmentAgent"
)

class OrchestrationEngine:
    """
    Orchestrates the workflow:
      1. Classifies employee attendance.
      2. Assigns work based on attendance and other business rules.
    """

    def __init__(self):
        self.attendance_agent = AttendanceClassificationAgent()
        self.assignment_agent = EmployeeWorkAssignmentAgent()

    async def execute(self, input_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Orchestrates the workflow.
        Args:
            input_data: dict with keys required by AttendanceClassificationAgent.
        Returns:
            dict: Final result from EmployeeWorkAssignmentAgent, plus intermediate results and errors.
        """
        orchestration_result = {
            "attendance_classification_result": None,
            "work_assignment_result": None,
            "errors": []
        }

        # Step 1: Attendance Classification
        try:
            logger.info("Step 1: Classifying attendance...")
            attendance_result = await self.attendance_agent.classify(input_data)
            orchestration_result["attendance_classification_result"] = attendance_result
        except Exception as e:
            logger.error(f"Error during attendance classification: {e}")
            orchestration_result["errors"].append({
                "step": "attendance_classification",
                "error": str(e)
            })
            # If attendance classification fails, propagate error and stop
            return orchestration_result

        # Check for success in attendance classification
        if not attendance_result.get("success", False):
            logger.warning("Attendance classification failed, skipping work assignment.")
            orchestration_result["errors"].append({
                "step": "attendance_classification",
                "error": attendance_result.get("error_code") or attendance_result.get("message") or "Unknown error"
            })
            return orchestration_result

        # Step 2: Work Assignment
        try:
            logger.info("Step 2: Assigning work based on attendance...")
            # Build employee_roster for assignment agent
            # Use masked employee_id and date from attendance_result
            employee_id = attendance_result.get("employee_id")
            date = attendance_result.get("date")
            attendance_status = attendance_result.get("attendance_status")
            # Compose employee_roster: at minimum, one employee with id and attendance status
            employee_roster = [{
                "employee_id": employee_id,
                "attendance_status": attendance_status,
                "date": date
            }]
            # Compose tasks: expect input_data to have 'tasks' key (list of task dicts)
            tasks = input_data.get("tasks")
            if not tasks or not isinstance(tasks, list):
                raise ValueError("Input data must include a non-empty 'tasks' list for work assignment.")

            # Optionally, allow extra fields in employee_roster if present in input_data
            # (e.g., name, skills, etc.)
            # If input_data has 'employee_roster', merge attendance_status into the matching employee
            if "employee_roster" in input_data and isinstance(input_data["employee_roster"], list):
                # Try to find and update the matching employee
                roster = []
                for emp in input_data["employee_roster"]:
                    emp_copy = dict(emp)
                    if emp_copy.get("employee_id") == employee_id:
                        emp_copy["attendance_status"] = attendance_status
                        emp_copy["date"] = date
                    roster.append(emp_copy)
                employee_roster = roster

            # Call assignment agent
            assignment_result = await self.assignment_agent.assign_tasks(
                tasks=tasks,
                employee_roster=employee_roster
            )
            orchestration_result["work_assignment_result"] = assignment_result
        except Exception as e:
            logger.error(f"Error during work assignment: {e}")
            orchestration_result["errors"].append({
                "step": "work_assignment",
                "error": str(e)
            })

        return orchestration_result

# Synchronous wrapper for environments that do not use asyncio directly
def run_orchestration(input_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Synchronous entrypoint for orchestration.
    """
    engine = OrchestrationEngine()
    try:
        loop = asyncio.get_event_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
    return loop.run_until_complete(engine.execute(input_data))

# For direct import and use:
#   from <this_module> import OrchestrationEngine, run_orchestration