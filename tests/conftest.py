"""Shared fixtures for all pipeline tests."""

import pytest


@pytest.fixture
def sample_users_raw() -> list[dict]:
    """Raw API response for /users."""
    return [
        {
            "address": {
                "geolocation": {"lat": "-37.3159", "long": "81.1496"},
                "city": "kilcoole",
                "street": "new road",
                "number": 7682,
                "zipcode": "12926-3874",
            },
            "id": 1,
            "email": "john@gmail.com",
            "username": "johnd",
            "password": "m38rmF$",
            "name": {"firstname": "john", "lastname": "doe"},
            "phone": "1-570-236-7033",
            "__v": 0,
        },
        {
            "address": {
                "geolocation": {"lat": "-37.3159", "long": "81.1496"},
                "city": "kilcoole",
                "street": "Lovers Ln",
                "number": 7267,
                "zipcode": "12926-3874",
            },
            "id": 2,
            "email": "morrison@gmail.com",
            "username": "mor_2314",
            "password": "83r5^_",
            "name": {"firstname": "david", "lastname": "morrison"},
            "phone": "1-570-236-7033",
            "__v": 0,
        },
    ]


@pytest.fixture
def sample_products_raw() -> list[dict]:
    """Raw API response for /products."""
    return [
        {
            "id": 1,
            "title": "Fjallraven - Foldsack No. 1 Backpack, Fits 15 Laptops",
            "price": 109.95,
            "description": "Your perfect pack for everyday use.",
            "category": "men's clothing",
            "image": "https://fakestoreapi.com/img/81fPKd-2AYL._AC_SL1500_t.png",
            "rating": {"rate": 3.9, "count": 120},
        },
        {
            "id": 2,
            "title": "Mens Casual Premium Slim Fit T-Shirts",
            "price": 22.3,
            "description": "Slim-fitting style, contrast raglan long sleeve.",
            "category": "men's clothing",
            "image": "https://fakestoreapi.com/img/71-3HjGNDUL._AC_SY879.png",
            "rating": {"rate": 4.1, "count": 259},
        },
    ]


@pytest.fixture
def sample_carts_raw() -> list[dict]:
    """Raw API response for /carts."""
    return [
        {
            "id": 1,
            "userId": 1,
            "date": "2020-03-02T00:00:00.000Z",
            "products": [
                {"productId": 1, "quantity": 4},
                {"productId": 2, "quantity": 1},
            ],
            "__v": 0,
        },
        {
            "id": 2,
            "userId": 1,
            "date": "2020-01-02T00:00:00.000Z",
            "products": [
                {"productId": 2, "quantity": 4},
                {"productId": 1, "quantity": 10},
            ],
            "__v": 0,
        },
    ]


@pytest.fixture
def run_date() -> str:
    return "2026-03-19"


@pytest.fixture
def prev_run_date() -> str:
    return "2026-03-18"
