"""Fetches products from FakeStoreAPI and validates the response with Pydantic."""

import httpx
from pydantic import ValidationError

from shared.schemas.products import ProductModel
from shared.utils import get_logger

logger = get_logger(__name__)

API_URL = "https://fakestoreapi.com/products"


def fetch_products() -> list[ProductModel]:
    """Fetch all products and validate with Pydantic.

    Raises:
        httpx.HTTPStatusError: on non-2xx response
        ValidationError: if the response shape doesn't match the schema
    """
    logger.info(f"Fetching products from {API_URL}")
    response = httpx.get(API_URL, timeout=30)
    response.raise_for_status()

    raw = response.json()
    logger.info(f"Received {len(raw)} products from API")

    try:
        products = [ProductModel.model_validate(item) for item in raw]
    except ValidationError as exc:
        logger.error(f"Pydantic validation failed for products: {exc}")
        raise

    logger.info(f"Pydantic validation passed for {len(products)} products")
    return products
