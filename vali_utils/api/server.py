from fastapi import FastAPI, Depends, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.openapi.docs import get_swagger_ui_html
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
        """Initialize API server

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
            docs_url=None,    # Disable default docs
            redoc_url=None    # Disable default redoc
        )

        # Add CORS middleware
        app.add_middleware(
            CORSMiddleware,
            allow_origins=["*"],
            allow_credentials=True,
            allow_methods=["*"],
            allow_headers=["*"],
        )

        # Protected documentation routes
        @app.get("/docs", include_in_schema=False)
        async def get_docs(_: bool = Depends(require_master_key)):
            return get_swagger_ui_html(
                openapi_url="/openapi.json",
                title="API Documentation",
                swagger_js_url="https://cdn.jsdelivr.net/npm/swagger-ui-dist@5.9.0/swagger-ui-bundle.js",
                swagger_css_url="https://cdn.jsdelivr.net/npm/swagger-ui-dist@5.9.0/swagger-ui.css",
            )

        @app.get("/openapi.json", include_in_schema=False)
        @endpoint_error_handler
        async def get_openapi(_: bool = Depends(require_master_key)):
            try:
                if not app.openapi_schema:
                    app.openapi_schema = get_openapi(
                        title=app.title,
                        version=app.version,
                        description=app.description,
                        routes=app.routes,
                        tags=app.openapi_tags,
                        servers=app.servers,
                        terms_of_service=app.terms_of_service,
                        contact=app.contact,
                        license_info=app.license_info
                    )
                    # Strip any sensitive information
                    for path in app.openapi_schema["paths"].values():
                        for operation in path.values():
                            if "security" in operation:
                                del operation["security"]
                return app.openapi_schema
            except Exception as e:
                bt.logging.error(f"Failed to generate OpenAPI schema: {str(e)}")
                raise HTTPException(status_code=500, detail="Could not generate API documentation")

        # Add rate limit headers middleware
        @app.middleware("http")
        async def add_rate_limit_headers(request, call_next):
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
        """Start API server in background thread"""
        if self.server_thread and self.server_thread.is_alive():
            bt.logging.warning("API server already running")
            return

        def run_server():
            """Run uvicorn server"""
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
        """Stop API server"""
        if self.server_thread and self.server_thread.is_alive():
            self.server_thread.join(timeout=5)
            bt.logging.info("API server stopped")