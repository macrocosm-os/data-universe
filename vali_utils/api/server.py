from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import uvicorn
from threading import Thread
import bittensor as bt
from typing import Optional
from .routes import router, get_validator


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
        self.app = self._create_app()
        self.server_thread: Optional[Thread] = None

    def _create_app(self) -> FastAPI:
        """Create and configure FastAPI application"""
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

        # Include API routes
        app.include_router(router, prefix="/api/v1")

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