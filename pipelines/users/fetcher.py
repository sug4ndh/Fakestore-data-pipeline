"""Fetches users from FakeStoreAPI and validates the response with Pydantic."""

import httpx
from pydantic import ValidationError

from shared.schemas.users import UserModel
from shared.utils import get_logger

logger = get_logger(__name__)

API_URL = "https://fakestoreapi.com/users"


def fetch_users() -> list[UserModel]:
    """Fetch all users and validate with Pydantic.

    Raises:
        httpx.HTTPStatusError: on non-2xx response
        ValidationError: if the response shape doesn't match the schema
    """
    logger.info(f"Fetching users from {API_URL}")
    response = httpx.get(API_URL, timeout=30)
    response.raise_for_status()

    raw = response.json()
    logger.info(f"Received {len(raw)} users from API")

    try:
        users = [UserModel.model_validate(item) for item in raw]
    except ValidationError as exc:
        logger.error(f"Pydantic validation failed for users: {exc}")
        raise

    logger.info(f"Pydantic validation passed for {len(users)} users")
    return users
