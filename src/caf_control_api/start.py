"""Initialize the P0 schema, then serve the API."""

import uvicorn

from .migrate import main as migrate


def main() -> None:
    migrate()
    uvicorn.run("caf_control_api.server:app", host="0.0.0.0", port=8000)


if __name__ == "__main__":
    main()
