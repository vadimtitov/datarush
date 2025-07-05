"""Tests for logging module."""

import logging
import os
import tempfile
import time
from unittest.mock import patch

import pytest

from datarush.config import DatarushConfig, LoggingConfig, set_datarush_config
from datarush.utils.logging import DataflowLogger, OperationLogger, setup_logging


class TestSetupLogging:
    """Test setup_logging function."""

    def test_setup_logging_defaults(self):
        """Test setup_logging with default parameters."""
        # Clear existing handlers
        root_logger = logging.getLogger()
        original_handlers = root_logger.handlers.copy()

        try:
            setup_logging()

            # Check that root logger has handlers
            assert len(root_logger.handlers) > 0

            # Check that console handler is present
            console_handlers = [
                h for h in root_logger.handlers if isinstance(h, logging.StreamHandler)
            ]
            assert len(console_handlers) > 0

            # Check log level
            assert root_logger.level == logging.INFO

        finally:
            # Restore original handlers
            root_logger.handlers = original_handlers

    def test_setup_logging_with_file(self):
        """Test setup_logging with log file."""
        with tempfile.NamedTemporaryFile(mode="w", delete=False) as tmp_file:
            log_file = tmp_file.name

        try:
            # Clear existing handlers
            root_logger = logging.getLogger()
            original_handlers = root_logger.handlers.copy()

            try:
                setup_logging(log_file=log_file)

                # Check that file handler is present
                file_handlers = [
                    h for h in root_logger.handlers if isinstance(h, logging.FileHandler)
                ]
                assert len(file_handlers) > 0

                # Check that file exists and is writable
                assert os.path.exists(log_file)

            finally:
                # Restore original handlers
                root_logger.handlers = original_handlers

        finally:
            # Clean up
            if os.path.exists(log_file):
                os.unlink(log_file)

    def test_setup_logging_custom_level(self):
        """Test setup_logging with custom log level."""
        # Clear existing handlers
        root_logger = logging.getLogger()
        original_handlers = root_logger.handlers.copy()

        try:
            setup_logging(level="DEBUG")

            # Check log level
            assert root_logger.level == logging.DEBUG

        finally:
            # Restore original handlers
            root_logger.handlers = original_handlers

    def test_setup_logging_custom_format(self):
        """Test setup_logging with custom format."""
        custom_format = "%(levelname)s - %(message)s"

        # Clear existing handlers
        root_logger = logging.getLogger()
        original_handlers = root_logger.handlers.copy()

        try:
            setup_logging(format_string=custom_format)

            # Check that at least one handler has the custom format
            has_custom_format = False
            for handler in root_logger.handlers:
                if hasattr(handler, "formatter") and handler.formatter:
                    if custom_format in str(handler.formatter._fmt):
                        has_custom_format = True
                        break

            assert has_custom_format

        finally:
            # Restore original handlers
            root_logger.handlers = original_handlers

    def test_setup_logging_with_config(self):
        """Test setup_logging with config integration."""
        # Set up config with logging settings
        config = DatarushConfig()
        set_datarush_config(config)

        # Clear existing handlers
        root_logger = logging.getLogger()
        original_handlers = root_logger.handlers.copy()

        try:
            setup_logging()

            # Should work without errors
            assert len(root_logger.handlers) > 0

        finally:
            # Restore original handlers
            root_logger.handlers = original_handlers

    def test_setup_logging_without_config(self):
        """Test setup_logging when config is not available."""
        # Clear existing handlers
        root_logger = logging.getLogger()
        original_handlers = root_logger.handlers.copy()

        try:
            # Should work without config
            setup_logging()
            assert len(root_logger.handlers) > 0

        finally:
            # Restore original handlers
            root_logger.handlers = original_handlers

    def test_setup_logging_clears_existing_handlers(self):
        """Test that setup_logging clears existing handlers."""
        # Add some handlers first
        root_logger = logging.getLogger()
        original_handlers = root_logger.handlers.copy()

        # Add a test handler
        test_handler = logging.StreamHandler()
        root_logger.addHandler(test_handler)
        initial_handler_count = len(root_logger.handlers)

        try:
            setup_logging()

            # Should have cleared old handlers and added new ones
            assert len(root_logger.handlers) > 0
            assert test_handler not in root_logger.handlers

        finally:
            # Restore original handlers
            root_logger.handlers = original_handlers


class TestOperationLogger:
    """Test OperationLogger context manager."""

    def test_operation_logger_success(self, caplog):
        """Test OperationLogger with successful operation."""
        logger = logging.getLogger("test_operation")
        logger.setLevel(logging.INFO)

        with OperationLogger("test_op", "Test Operation", logger):
            time.sleep(0.01)  # Small delay to ensure timing

        # Check that start and completion messages were logged
        assert len(caplog.records) == 2

        # Check start message
        start_record = caplog.records[0]
        assert "Starting operation: Test Operation (test_op)" in start_record.message
        assert start_record.levelname == "INFO"

        # Check completion message
        completion_record = caplog.records[1]
        assert "Completed operation: Test Operation (test_op)" in completion_record.message
        assert "in " in completion_record.message  # Should include duration
        assert completion_record.levelname == "INFO"

    def test_operation_logger_failure(self, caplog):
        """Test OperationLogger with failed operation."""
        logger = logging.getLogger("test_operation_failure")
        logger.setLevel(logging.INFO)

        with pytest.raises(ValueError):
            with OperationLogger("test_op", "Test Operation", logger):
                time.sleep(0.01)
                raise ValueError("Test error")

        # Check that start and failure messages were logged
        assert len(caplog.records) == 2

        # Check start message
        start_record = caplog.records[0]
        assert "Starting operation: Test Operation (test_op)" in start_record.message
        assert start_record.levelname == "INFO"

        # Check failure message
        failure_record = caplog.records[1]
        assert "Failed operation: Test Operation (test_op)" in failure_record.message
        assert "ValueError: Test error" in failure_record.message
        assert "after " in failure_record.message  # Should include duration
        assert failure_record.levelname == "ERROR"

    def test_operation_logger_timing(self, caplog):
        """Test that OperationLogger measures timing correctly."""
        logger = logging.getLogger("test_timing")
        logger.setLevel(logging.INFO)

        with OperationLogger("test_op", "Test Operation", logger):
            time.sleep(0.1)  # 100ms delay

        # Check that duration is logged
        completion_record = caplog.records[1]
        assert "in " in completion_record.message

        # Extract duration from log message
        duration_str = completion_record.message.split("in ")[1].split("s")[0]
        duration = float(duration_str)

        # Should be at least 0.1 seconds
        assert duration >= 0.1

    def test_operation_logger_attributes(self):
        """Test OperationLogger attributes."""
        logger = logging.getLogger("test_attributes")

        op_logger = OperationLogger("test_op", "Test Operation", logger)

        assert op_logger.operation_name == "test_op"
        assert op_logger.operation_title == "Test Operation"
        assert op_logger.logger == logger
        assert op_logger.start_time is None


class TestDataflowLogger:
    """Test DataflowLogger context manager."""

    def test_dataflow_logger_success(self, caplog):
        """Test DataflowLogger with successful template execution."""
        logger = logging.getLogger("test_dataflow")
        logger.setLevel(logging.INFO)

        with DataflowLogger("test_template", "v1.0", logger):
            time.sleep(0.01)  # Small delay to ensure timing

        # Check that start and completion messages were logged
        assert len(caplog.records) == 2

        # Check start message
        start_record = caplog.records[0]
        assert "Starting template execution: test_template v1.0" in start_record.message
        assert start_record.levelname == "INFO"

        # Check completion message
        completion_record = caplog.records[1]
        assert "Completed template execution: test_template v1.0" in completion_record.message
        assert "in " in completion_record.message  # Should include duration
        assert completion_record.levelname == "INFO"

    def test_dataflow_logger_failure(self, caplog):
        """Test DataflowLogger with failed template execution."""
        logger = logging.getLogger("test_dataflow_failure")
        logger.setLevel(logging.INFO)

        with pytest.raises(RuntimeError):
            with DataflowLogger("test_template", "v1.0", logger):
                time.sleep(0.01)
                raise RuntimeError("Template execution failed")

        # Check that start and failure messages were logged
        assert len(caplog.records) == 2

        # Check start message
        start_record = caplog.records[0]
        assert "Starting template execution: test_template v1.0" in start_record.message
        assert start_record.levelname == "INFO"

        # Check failure message
        failure_record = caplog.records[1]
        assert "Failed template execution: test_template v1.0" in failure_record.message
        assert "RuntimeError: Template execution failed" in failure_record.message
        assert "after " in failure_record.message  # Should include duration
        assert failure_record.levelname == "ERROR"

    def test_dataflow_logger_timing(self, caplog):
        """Test that DataflowLogger measures timing correctly."""
        logger = logging.getLogger("test_dataflow_timing")
        logger.setLevel(logging.INFO)

        with DataflowLogger("test_template", "v2.0", logger):
            time.sleep(0.1)  # 100ms delay

        # Check that duration is logged
        completion_record = caplog.records[1]
        assert "in " in completion_record.message

        # Extract duration from log message
        duration_str = completion_record.message.split("in ")[1].split("s")[0]
        duration = float(duration_str)

        # Should be at least 0.1 seconds
        assert duration >= 0.1

    def test_dataflow_logger_attributes(self):
        """Test DataflowLogger attributes."""
        logger = logging.getLogger("test_dataflow_attributes")

        df_logger = DataflowLogger("test_template", "v1.0", logger)

        assert df_logger.template_name == "test_template"
        assert df_logger.template_version == "v1.0"
        assert df_logger.logger == logger
        assert df_logger.start_time is None

    def test_dataflow_logger_without_version(self, caplog):
        """Test DataflowLogger without version prefix."""
        logger = logging.getLogger("test_no_version")
        logger.setLevel(logging.INFO)

        with DataflowLogger("test_template", "1.0", logger):
            time.sleep(0.01)

        # Check that version is logged without "v" prefix
        start_record = caplog.records[0]
        assert "Starting template execution: test_template 1.0" in start_record.message


class TestLoggingIntegration:
    """Test logging integration with config."""

    @patch.dict(
        os.environ,
        {
            "DATARUSH_LOG_LEVEL": "DEBUG",
            "DATARUSH_LOG_FILE": "/tmp/test.log",
            "DATARUSH_LOG_FORMAT": "%(levelname)s - %(message)s",
        },
    )
    def test_logging_with_environment_variables(self):
        """Test logging setup with environment variables."""
        # Clear existing handlers
        root_logger = logging.getLogger()
        original_handlers = root_logger.handlers.copy()

        try:
            setup_logging()

            # Should use environment variable settings
            assert root_logger.level == logging.DEBUG

            # Check that file handler is present
            file_handlers = [h for h in root_logger.handlers if isinstance(h, logging.FileHandler)]
            assert len(file_handlers) > 0

        finally:
            # Restore original handlers
            root_logger.handlers = original_handlers

    def test_logging_config_integration(self):
        """Test logging with config integration."""
        # Set up config
        config = DatarushConfig()
        set_datarush_config(config)

        # Clear existing handlers
        root_logger = logging.getLogger()
        original_handlers = root_logger.handlers.copy()

        try:
            setup_logging()

            # Should work with config
            assert len(root_logger.handlers) > 0

        finally:
            # Restore original handlers
            root_logger.handlers = original_handlers


class TestLoggingEdgeCases:
    """Test edge cases and error conditions."""

    def test_operation_logger_no_exception_info(self, caplog):
        """Test OperationLogger with None exception info."""
        logger = logging.getLogger("test_no_exc")
        logger.setLevel(logging.INFO)

        op_logger = OperationLogger("test_op", "Test Operation", logger)
        op_logger.start_time = time.time()

        # Simulate exit without exception
        op_logger.__exit__(None, None, None)

        # Should log completion
        assert len(caplog.records) == 1
        assert "Completed operation" in caplog.records[0].message

    def test_dataflow_logger_no_exception_info(self, caplog):
        """Test DataflowLogger with None exception info."""
        logger = logging.getLogger("test_no_exc_df")
        logger.setLevel(logging.INFO)

        df_logger = DataflowLogger("test_template", "v1.0", logger)
        df_logger.start_time = time.time()

        # Simulate exit without exception
        df_logger.__exit__(None, None, None)

        # Should log completion
        assert len(caplog.records) == 1
        assert "Completed template execution" in caplog.records[0].message

    def test_logging_with_invalid_level(self):
        """Test setup_logging with invalid log level."""
        # Clear existing handlers
        root_logger = logging.getLogger()
        original_handlers = root_logger.handlers.copy()

        try:
            # Should handle invalid level gracefully
            setup_logging(level="INVALID_LEVEL")

            # Should fall back to default level
            assert root_logger.level == logging.INFO

        finally:
            # Restore original handlers
            root_logger.handlers = original_handlers
