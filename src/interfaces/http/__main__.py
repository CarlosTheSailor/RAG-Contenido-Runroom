from __future__ import annotations

import uvicorn

from src.config import APIRuntimeSettings


def main() -> None:
    runtime = APIRuntimeSettings.from_env()
    uvicorn.run(
        "src.interfaces.http.app:create_app",
        host=runtime.host,
        port=runtime.port,
        factory=True,
    )


if __name__ == "__main__":
    main()
