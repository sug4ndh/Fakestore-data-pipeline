"""Fetches carts from FakeStoreAPI and validates the response with Pydantic."""

import httpx
from pydantic import ValidationError

from shared.schemas.carts import CartModel
from shared.utils import get_logger

logger = get_logger(__name__)

API_URL = "https://fakestoreapi.com/carts"


def fetch_carts() -> list[CartModel]:
    """Fetch all carts and validate with Pydantic.

    Raises:
        httpx.HTTPStatusError: on non-2xx response
        ValidationError: if the response shape doesn't match the schema
    """
    logger.info(f"Fetching carts from {API_URL}")
    response = httpx.get(API_URL, timeout=30)
    response.raise_for_status()

    raw = response.json()
    logger.info(f"Received {len(raw)} carts from API")

    try:
        carts = [CartModel.model_validate(item) for item in raw]
    except ValidationError as exc:
        logger.error(f"Pydantic validation failed for carts: {exc}")
        raise

    logger.info(f"Pydantic validation passed for {len(carts)} carts")
    return carts
