import time
from fastapi import FastAPI, Depends, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.openapi.docs import get_swagger_ui_html, get_redoc_html
import uvicorn
from threading import Thread
import bittensor as bt
from typing import Optional
from .routes import router, get_validator
from vali_utils.api.auth.key_routes import router as key_router
from vali_utils.api.auth.auth import APIKeyManager, key_manager, require_master_key
from vali_utils.api.utils import endpoint_error_handler


class ValidatorAPI:
    """API server for validator on-demand queries"""

    def __init__(self, validator, port: int = 8000):
        """
        Initialize API server

        Args:
            validator: Validator instance
            port: Port number to run API on
        """
        self.validator = validator
        self.port = port
        self.key_manager = key_manager
        self.app = self._create_app()
        self.server_thread: Optional[Thread] = None

    def _create_app(self) -> FastAPI:
        """Create and configure FastAPI application"""
        app = FastAPI(
            title="Data Universe Validator API",
            description="API for on-demand data queries from the Data Universe network",
            version="1.0.0",
            docs_url=None,    # Disable default docs routes
        )

        # Add CORS middleware
        app.add_middleware(
            CORSMiddleware,
            allow_origins=["*"],
            allow_credentials=True,
            allow_methods=["*"],
            allow_headers=["*"],
        )

        # Protected Swagger UI docs endpoint
        @app.get("/docs", include_in_schema=False)
        async def get_docs(_: bool = Depends(require_master_key)):
            return get_swagger_ui_html(
                openapi_url="/openapi.json",
                title="API Documentation"
            )

        # Protected ReDoc docs endpoint using default styling
        @app.get("/redoc", include_in_schema=False)
        async def get_redoc(_: bool = Depends(require_master_key)):
            return get_redoc_html(
                openapi_url="/openapi.json",
                title="API Documentation"
            )

        # Protected OpenAPI JSON schema endpoint
        @app.get("/openapi.json", include_in_schema=False)
        @endpoint_error_handler
        async def openapi_schema(_: bool = Depends(require_master_key)):
            try:
                if not app.openapi_schema:
                    from fastapi.openapi.utils import get_openapi
                    app.openapi_schema = get_openapi(
                        title=app.title,
                        version=app.version,
                        description=app.description,
                        routes=app.routes,
                    )
                    # Remove sensitive security information if needed
                    for path in app.openapi_schema.get("paths", {}).values():
                        for operation in path.values():
                            if "security" in operation:
                                del operation["security"]
                return app.openapi_schema
            except Exception as e:
                bt.logging.error(f"Failed to generate OpenAPI schema: {str(e)}")
                raise HTTPException(status_code=500, detail="Could not generate API documentation")

        # Rate limit headers middleware
        @app.middleware("http")
        async def add_rate_limit_headers(request: Request, call_next):
            response = await call_next(request)
            api_key = request.headers.get("X-API-Key")
            if api_key and self.key_manager.is_valid_key(api_key):
                _, headers = self.key_manager.check_rate_limit(api_key)
                for header_name, header_value in headers.items():
                    response.headers[header_name] = header_value
            return response

        # Set validator instance for dependency injection
        get_validator.api = self

        # Include API routes
        app.include_router(router, prefix="/api/v1")
        app.include_router(key_router, prefix="/api/v1/keys")

        return app

    def start(self):
        """Start API server with better error handling"""
        if self.server_thread and self.server_thread.is_alive():
            bt.logging.warning("API server already running")
            return

        def run_server():
            try:
                bt.logging.info(f"Starting API server on port {self.port}")
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
        """Stop API server"""
        bt.logging.info("Stopping API server")
        # The uvicorn server will stop when the thread is terminated
        self.server_thread = None  # Allow for garbage collection

    def restart(self):
        """Restart API server"""
        bt.logging.info("Restarting API server")
        self.stop()
        time.sleep(2)  # Give it a moment to fully stop
        self.start()
    def stop(self):
        """Stop API server"""
        if self.server_thread and self.server_thread.is_alive():
            self.server_thread.join(timeout=5)
            bt.logging.info("API server stopped")