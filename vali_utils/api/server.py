from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import uvicorn
from threading import Thread
import bittensor as bt
from typing import Optional
from .routes import router, get_validator
from .key_management.auth import APIKeyManager, verify_api_key
from .key_management.routes import key_router


class ValidatorAPI:
    def __init__(self, validator, port: int = 8000):
        self.validator = validator
        self.port = port
        self.key_manager = APIKeyManager(db_path="api_keys.db") # TODO
        self.app = self._create_app()
        self.server_thread: Optional[Thread] = None

    def _create_app(self) -> FastAPI:
        app = FastAPI(
            title="Data Universe Validator API",
            description="API for on-demand data queries from the Data Universe network",
            version="1.0.0"
        )

        # Add CORS middleware
        app.add_middleware(
            CORSMiddleware,
            allow_origins=["*"],
            allow_credentials=True,
            allow_methods=["*"],
            allow_headers=["*"],
        )

        # Set validator instance for dependency injection
        get_validator.api = self

        # Add authentication to all API routes
        @app.middleware("http")
        async def api_key_middleware(request, call_next):
            # Skip auth for docs and key management endpoints
            if request.url.path in ["/docs", "/redoc", "/openapi.json"] or request.url.path.startswith("/manage/"):
                return await call_next(request)

            try:
                api_key = request.headers.get("X-API-Key")
                await verify_api_key(api_key, self.key_manager)
            except Exception as e:
                from fastapi.responses import JSONResponse
                return JSONResponse(
                    status_code=getattr(e, "status_code", 500),
                    content={"detail": str(e)}
                )

            return await call_next(request)

        # Include API routes
        app.include_router(router, prefix="/api")
        # Include key management routes
        app.include_router(key_router)

        return app

    def start(self):
        """Start API server in background thread"""
        if self.server_thread and self.server_thread.is_alive():
            bt.logging.warning("API server already running")
            return

        def run_server():
            try:
                uvicorn.run(
                    self.app,
                    host="0.0.0.0",
                    port=self.port,
                    log_level="info"
                )
            except Exception as e:
                bt.logging.error(f"API server error: {str(e)}")

        self.server_thread = Thread(target=run_server, daemon=True)
        self.server_thread.start()
        bt.logging.success(f"API server started on port {self.port}")

    def stop(self):
        if self.server_thread and self.server_thread.is_alive():
            self.server_thread.join(timeout=5)
            bt.logging.info("API server stopped")