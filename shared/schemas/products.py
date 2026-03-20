"""Pydantic models and Pandera schemas for the products endpoint."""

import pandera as pa
from pandera import Check, Column, DataFrameSchema
from pydantic import BaseModel, ConfigDict


# Pydantic models


class ProductRating(BaseModel):
    rate: float
    count: int


class ProductModel(BaseModel):
    model_config = ConfigDict(extra="allow")

    id: int
    title: str
    price: float
    description: str
    category: str
    image: str
    rating: ProductRating


# Pandera schemas

silver_products_schema = DataFrameSchema(
    columns={
        "id": Column(int, nullable=False),
        "title": Column(str, nullable=False),
        "price": Column(float, nullable=False),
        "description": Column(str, nullable=True),
        "category": Column(str, nullable=False),
        "image": Column(str, nullable=True),
        "rating_rate": Column(float, nullable=True),
        "rating_count": Column(int, nullable=True),
        "_ingested_at": Column(pa.DateTime, nullable=False),
    },
    strict=False,
)

gold_products_schema = DataFrameSchema(
    columns={
        "id": Column(int, nullable=False, checks=Check.greater_than(0)),
        "title": Column(
            str,
            nullable=False,
            checks=Check(lambda s: (s.str.strip().str.len() > 0).all(), element_wise=False),
        ),
        "price": Column(
            float,
            nullable=False,
            checks=Check.greater_than(0),
        ),
        "category": Column(str, nullable=False),
        "rating_rate": Column(
            float,
            nullable=True,
            checks=Check.in_range(0.0, 5.0),
        ),
        "rating_count": Column(
            int,
            nullable=True,
            checks=Check.greater_than_or_equal_to(0),
        ),
        "_ingested_at": Column(pa.DateTime, nullable=False),
    },
    strict=False,
)
