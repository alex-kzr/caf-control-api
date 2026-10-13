"""ASGI entrypoint; reads required configuration at process startup."""

from .app import create_app

app = create_app()
