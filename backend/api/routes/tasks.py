from fastapi import APIRouter
from .create_task import create_task
from .start_screening import start_screening
from .request_full_screening import request_full_screening
from .cancel_task import cancel_task
from .get_task import get_task
from .list_tasks import list_tasks

# Create the router with the tasks tag
router = APIRouter(tags=["tasks"])

# Register all the route functions
router.add_api_route("", create_task, methods=["POST"])
router.add_api_route("/{task_id}/screen", start_screening, methods=["POST"])
router.add_api_route("/{task_id}/request-full-screening", request_full_screening, methods=["POST"])
router.add_api_route("/{task_id}/cancel", cancel_task, methods=["POST"])
router.add_api_route("/{task_id}", get_task, methods=["GET"])
router.add_api_route("", list_tasks, methods=["GET"])