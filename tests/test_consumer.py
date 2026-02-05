"""
Unit tests for FlowODM consumer loops.

Tests for sync and async consumer loops, including Windows compatibility.
"""

from __future__ import annotations

import asyncio
import signal
from unittest.mock import MagicMock, patch

import pytest

from flowodm.consumer import AsyncConsumerLoop, ConsumerLoop


@pytest.mark.unit
class TestConsumerLoop:
    """Unit tests for synchronous ConsumerLoop."""

    def test_consumer_loop_initialization(self):
        """Test ConsumerLoop can be initialized with required parameters."""
        mock_model = MagicMock()
        mock_handler = MagicMock()

        loop = ConsumerLoop(model=mock_model, handler=mock_handler)

        assert loop.model is mock_model
        assert loop.handler is mock_handler
        assert loop._running is False
        assert loop.max_retries == 0
        assert loop.retry_delay == 1.0
        assert loop.poll_timeout == 1.0
        assert loop.commit_strategy == "before_processing"

    def test_consumer_loop_stop(self):
        """Test that stop() sets _running to False."""
        mock_model = MagicMock()
        mock_handler = MagicMock()

        loop = ConsumerLoop(model=mock_model, handler=mock_handler)
        loop._running = True

        loop.stop()

        assert loop._running is False

    def test_consumer_loop_signal_handler(self):
        """Test that signal handler calls stop()."""
        mock_model = MagicMock()
        mock_handler = MagicMock()

        loop = ConsumerLoop(model=mock_model, handler=mock_handler)
        loop._running = True

        loop._signal_handler(signal.SIGINT, None)

        assert loop._running is False


@pytest.mark.unit
class TestConsumerLoopWindowsCompatibility:
    """Tests for Windows compatibility in ConsumerLoop."""

    def test_consumer_loop_handles_missing_sigterm(self):
        """Test that ConsumerLoop handles platforms without SIGTERM (Windows).

        On Windows, signal.SIGTERM doesn't exist, so we should only set up
        SIGINT handler.
        """
        mock_model = MagicMock()
        mock_handler = MagicMock()

        loop = ConsumerLoop(model=mock_model, handler=mock_handler)

        # Track signal.signal calls
        signal_calls = []
        original_signal = signal.signal

        def track_signal(signum, handler):
            signal_calls.append((signum, handler))
            return original_signal(signum, signal.SIG_DFL)

        # Mock signal module to simulate Windows (no SIGTERM attribute)
        with patch("signal.signal", side_effect=track_signal):
            with patch("signal.SIGTERM", create=True) as mock_sigterm:
                # Simulate Windows where SIGTERM doesn't exist
                del mock_sigterm
                # Make hasattr return False for SIGTERM
                original_hasattr = hasattr

                def custom_hasattr(obj, name):
                    if obj.__name__ == "signal" and name == "SIGTERM":
                        return False
                    return original_hasattr(obj, name)

                with patch("builtins.hasattr", side_effect=custom_hasattr):
                    loop._setup_signal_handlers()

        # Verify only SIGINT was set up
        assert len(signal_calls) == 1
        assert signal_calls[0][0] == signal.SIGINT

    def test_consumer_loop_uses_both_signals_when_available(self):
        """Test that ConsumerLoop uses both SIGTERM and SIGINT on Unix-like systems."""
        mock_model = MagicMock()
        mock_handler = MagicMock()

        loop = ConsumerLoop(model=mock_model, handler=mock_handler)

        # Track signal.signal calls
        signal_calls = []
        original_signal = signal.signal

        def track_signal(signum, handler):
            signal_calls.append((signum, handler))
            return original_signal(signum, signal.SIG_DFL)

        with patch("signal.signal", side_effect=track_signal):
            loop._setup_signal_handlers()

        # Verify both SIGINT and SIGTERM were set up (if SIGTERM exists)
        if hasattr(signal, "SIGTERM"):
            assert len(signal_calls) == 2
            signal_nums = [call[0] for call in signal_calls]
            assert signal.SIGINT in signal_nums
            assert signal.SIGTERM in signal_nums
        else:
            # On Windows, only SIGINT
            assert len(signal_calls) == 1
            assert signal_calls[0][0] == signal.SIGINT


@pytest.mark.unit
class TestAsyncConsumerLoop:
    """Unit tests for asynchronous AsyncConsumerLoop."""

    def test_async_consumer_loop_initialization(self):
        """Test AsyncConsumerLoop can be initialized with required parameters."""
        mock_model = MagicMock()
        mock_handler = MagicMock()

        loop = AsyncConsumerLoop(model=mock_model, handler=mock_handler)

        assert loop.model is mock_model
        assert loop.handler is mock_handler
        assert loop._running is False
        assert loop.max_concurrent == 10
        assert loop.max_retries == 0
        assert loop.retry_delay == 1.0
        assert loop.poll_timeout == 1.0
        assert loop.commit_strategy == "before_processing"

    def test_async_consumer_loop_stop(self):
        """Test that stop() sets _running to False."""
        mock_model = MagicMock()
        mock_handler = MagicMock()

        loop = AsyncConsumerLoop(model=mock_model, handler=mock_handler)
        loop._running = True

        loop.stop()

        assert loop._running is False


@pytest.mark.unit
class TestAsyncConsumerLoopWindowsCompatibility:
    """Tests for Windows compatibility in AsyncConsumerLoop."""

    async def test_async_consumer_loop_handles_signal_handler_not_implemented(self):
        """Test that AsyncConsumerLoop handles NotImplementedError on Windows.

        On Windows, ProactorEventLoop doesn't support add_signal_handler(),
        so we need to fall back to signal.signal().
        """
        mock_model = MagicMock()
        mock_model._get_topic.return_value = "test-topic"

        async def mock_handler(_event):
            pass

        # Create a mock consumer that returns None (no messages)
        mock_consumer = MagicMock()
        mock_consumer.poll.return_value = None

        # Mock get_async_consumer to return our mock as a coroutine
        async def get_mock_consumer(*args, **kwargs):
            return mock_consumer

        mock_model.get_async_consumer = get_mock_consumer

        loop = AsyncConsumerLoop(model=mock_model, handler=mock_handler)

        # Create a mock event loop that raises NotImplementedError for add_signal_handler
        mock_event_loop = MagicMock()
        mock_event_loop.add_signal_handler.side_effect = NotImplementedError

        # Track if signal.signal was called as fallback
        signal_calls = []
        original_signal = signal.signal

        def track_signal(signum, handler):
            signal_calls.append((signum, handler))
            return original_signal(signum, signal.SIG_DFL)

        with patch("asyncio.get_running_loop", return_value=mock_event_loop):
            with patch("signal.signal", side_effect=track_signal):
                # Make the loop stop immediately after starting
                async def stop_after_start():
                    await asyncio.sleep(0.01)
                    loop.stop()

                # Run both the consumer loop and the stop task
                await asyncio.gather(loop.run(), stop_after_start(), return_exceptions=True)

        # Verify signal.signal was called as fallback for SIGINT
        assert len(signal_calls) == 1
        assert signal_calls[0][0] == signal.SIGINT

    async def test_async_consumer_loop_uses_native_signal_handlers_when_supported(self):
        """Test that AsyncConsumerLoop uses native signal handlers when supported."""
        mock_model = MagicMock()
        mock_model._get_topic.return_value = "test-topic"

        async def mock_handler(_event):
            pass

        # Create a mock consumer that returns None (no messages)
        mock_consumer = MagicMock()
        mock_consumer.poll.return_value = None

        # Mock get_async_consumer to return our mock as a coroutine
        async def get_mock_consumer(*args, **kwargs):
            return mock_consumer

        mock_model.get_async_consumer = get_mock_consumer

        loop = AsyncConsumerLoop(model=mock_model, handler=mock_handler)

        # Create a mock event loop that supports add_signal_handler
        mock_event_loop = MagicMock()
        signal_handler_calls = []

        def track_add_signal_handler(sig, handler):
            signal_handler_calls.append((sig, handler))

        mock_event_loop.add_signal_handler.side_effect = track_add_signal_handler

        with patch("asyncio.get_running_loop", return_value=mock_event_loop):
            # Make the loop stop immediately after starting
            async def stop_after_start():
                await asyncio.sleep(0.01)
                loop.stop()

            # Run both the consumer loop and the stop task
            await asyncio.gather(loop.run(), stop_after_start(), return_exceptions=True)

        # Verify add_signal_handler was called for both SIGTERM and SIGINT
        assert len(signal_handler_calls) == 2
        signal_nums = [call[0] for call in signal_handler_calls]
        assert signal.SIGTERM in signal_nums
        assert signal.SIGINT in signal_nums

    async def test_windows_signal_fallback_calls_stop(self):
        """Test that the Windows signal fallback correctly calls stop()."""
        mock_model = MagicMock()

        async def mock_handler(_event):
            pass

        loop = AsyncConsumerLoop(model=mock_model, handler=mock_handler)
        loop._running = True

        # Simulate what the Windows fallback handler does
        def fallback_handler(signum, frame):
            loop.stop()

        fallback_handler(signal.SIGINT, None)

        assert loop._running is False


@pytest.mark.unit
class TestCommitStrategies:
    """Unit tests for commit strategy functionality."""

    def test_invalid_commit_strategy_raises_error(self):
        """Test that invalid commit_strategy raises ValueError."""
        mock_model = MagicMock()
        mock_handler = MagicMock()

        with pytest.raises(ValueError) as exc_info:
            ConsumerLoop(model=mock_model, handler=mock_handler, commit_strategy="invalid_strategy")

        assert "Invalid commit_strategy" in str(exc_info.value)
        assert "invalid_strategy" in str(exc_info.value)

    def test_before_processing_commits_early(self):
        """Test that before_processing commits before handler execution."""
        mock_model = MagicMock()
        mock_model._deserialize_avro.return_value = {"test": "data"}

        handler_called = False
        commit_called_before_handler = None

        def track_handler(_data):
            nonlocal handler_called
            handler_called = True

        mock_consumer = MagicMock()

        def track_commit(_msg):
            nonlocal commit_called_before_handler
            commit_called_before_handler = not handler_called

        mock_consumer.commit.side_effect = track_commit

        loop = ConsumerLoop(
            model=mock_model, handler=track_handler, commit_strategy="before_processing"
        )
        loop._consumer = mock_consumer

        # Create a mock message
        mock_msg = MagicMock()
        mock_msg.value.return_value = b"test_data"

        # Process the message
        loop._process_message(mock_msg)

        # Verify commit was called before handler
        assert commit_called_before_handler is True
        assert handler_called is True
        mock_consumer.commit.assert_called_once_with(mock_msg)

    def test_after_processing_commits_late(self):
        """Test that after_processing commits after handler execution."""
        mock_model = MagicMock()
        mock_model._deserialize_avro.return_value = {"test": "data"}

        handler_called = False
        commit_called_after_handler = None

        def track_handler(_data):
            nonlocal handler_called
            handler_called = True

        mock_consumer = MagicMock()

        def track_commit(_msg):
            nonlocal commit_called_after_handler
            commit_called_after_handler = handler_called

        mock_consumer.commit.side_effect = track_commit

        loop = ConsumerLoop(
            model=mock_model, handler=track_handler, commit_strategy="after_processing"
        )
        loop._consumer = mock_consumer

        # Create a mock message
        mock_msg = MagicMock()
        mock_msg.value.return_value = b"test_data"

        # Process the message
        loop._process_message(mock_msg)

        # Verify commit was called after handler
        assert commit_called_after_handler is True
        assert handler_called is True
        mock_consumer.commit.assert_called_once_with(mock_msg)

    def test_before_processing_skips_on_commit_failure(self):
        """Test that message is skipped if early commit fails."""
        mock_model = MagicMock()
        mock_handler = MagicMock()
        mock_consumer = MagicMock()

        # Make commit fail on all attempts
        mock_consumer.commit.side_effect = Exception("Commit failed")

        loop = ConsumerLoop(
            model=mock_model, handler=mock_handler, commit_strategy="before_processing"
        )
        loop._consumer = mock_consumer

        # Create a mock message
        mock_msg = MagicMock()
        mock_msg.value.return_value = b"test_data"

        with patch("flowodm.consumer.logger") as mock_logger:
            # Process the message
            loop._process_message(mock_msg)

            # Verify handler was NOT called (message skipped)
            mock_handler.assert_not_called()

            # Verify error was logged
            error_logs = [call_args[0][0] for call_args in mock_logger.error.call_args_list]
            assert any("Skipping message" in log for log in error_logs)

    def test_commit_retry_logic(self):
        """Test that commit failures are retried with backoff."""
        mock_model = MagicMock()
        mock_handler = MagicMock()
        mock_consumer = MagicMock()

        # Make commit fail twice, then succeed
        commit_attempt_count = 0

        def commit_with_retries(_msg):
            nonlocal commit_attempt_count
            commit_attempt_count += 1
            if commit_attempt_count < 3:
                raise Exception("Transient failure")

        mock_consumer.commit.side_effect = commit_with_retries

        loop = ConsumerLoop(
            model=mock_model, handler=mock_handler, commit_strategy="before_processing"
        )
        loop._consumer = mock_consumer

        mock_msg = MagicMock()

        with patch("time.sleep"):  # Speed up test by mocking sleep
            result = loop._commit_offset(mock_msg, max_attempts=3)

        # Verify commit was retried and eventually succeeded
        assert result is True
        assert commit_attempt_count == 3

    def test_after_processing_commits_on_handler_failure(self):
        """Test that after_processing commits even when handler fails after retries."""
        mock_model = MagicMock()
        mock_model._deserialize_avro.return_value = {"test": "data"}

        def failing_handler(_data):
            raise Exception("Handler failed")

        mock_consumer = MagicMock()

        loop = ConsumerLoop(
            model=mock_model,
            handler=failing_handler,
            commit_strategy="after_processing",
            max_retries=0,  # No retries for faster test
        )
        loop._consumer = mock_consumer

        mock_msg = MagicMock()
        mock_msg.value.return_value = b"test_data"

        with patch("flowodm.consumer.logger"):
            loop._process_message(mock_msg)

        # Verify commit was called even though handler failed
        mock_consumer.commit.assert_called_once_with(mock_msg)

    def test_before_processing_does_not_commit_on_failure(self):
        """Test that before_processing does not commit again on handler failure."""
        mock_model = MagicMock()
        mock_model._deserialize_avro.return_value = {"test": "data"}

        def failing_handler(_data):
            raise Exception("Handler failed")

        mock_consumer = MagicMock()

        loop = ConsumerLoop(
            model=mock_model,
            handler=failing_handler,
            commit_strategy="before_processing",
            max_retries=0,  # No retries for faster test
        )
        loop._consumer = mock_consumer

        mock_msg = MagicMock()
        mock_msg.value.return_value = b"test_data"

        with patch("flowodm.consumer.logger"):
            loop._process_message(mock_msg)

        # Verify commit was called only once (early commit, not on failure)
        mock_consumer.commit.assert_called_once_with(mock_msg)


@pytest.mark.unit
class TestAsyncCommitStrategies:
    """Unit tests for async commit strategy functionality."""

    def test_async_invalid_commit_strategy_raises_error(self):
        """Test that invalid commit_strategy raises ValueError."""
        mock_model = MagicMock()
        mock_handler = MagicMock()

        with pytest.raises(ValueError) as exc_info:
            AsyncConsumerLoop(
                model=mock_model, handler=mock_handler, commit_strategy="invalid_strategy"
            )

        assert "Invalid commit_strategy" in str(exc_info.value)

    async def test_async_before_processing_commits_early(self):
        """Test that before_processing commits before handler execution."""
        mock_model = MagicMock()
        mock_model._deserialize_avro.return_value = {"test": "data"}

        handler_called = False
        commit_called_before_handler = None

        async def track_handler(_data):
            nonlocal handler_called
            handler_called = True

        mock_consumer = MagicMock()

        def track_commit(_msg):
            nonlocal commit_called_before_handler
            commit_called_before_handler = not handler_called

        mock_consumer.commit.side_effect = track_commit

        loop = AsyncConsumerLoop(
            model=mock_model, handler=track_handler, commit_strategy="before_processing"
        )
        loop._consumer = mock_consumer

        # Create a mock message
        mock_msg = MagicMock()
        mock_msg.value.return_value = b"test_data"

        # Process the message
        await loop._process_message(mock_msg)

        # Verify commit was called before handler
        assert commit_called_before_handler is True
        assert handler_called is True
        mock_consumer.commit.assert_called_once_with(mock_msg)

    async def test_async_after_processing_commits_late(self):
        """Test that after_processing commits after handler execution."""
        mock_model = MagicMock()
        mock_model._deserialize_avro.return_value = {"test": "data"}

        handler_called = False
        commit_called_after_handler = None

        async def track_handler(_data):
            nonlocal handler_called
            handler_called = True

        mock_consumer = MagicMock()

        def track_commit(_msg):
            nonlocal commit_called_after_handler
            commit_called_after_handler = handler_called

        mock_consumer.commit.side_effect = track_commit

        loop = AsyncConsumerLoop(
            model=mock_model, handler=track_handler, commit_strategy="after_processing"
        )
        loop._consumer = mock_consumer

        # Create a mock message
        mock_msg = MagicMock()
        mock_msg.value.return_value = b"test_data"

        # Process the message
        await loop._process_message(mock_msg)

        # Verify commit was called after handler
        assert commit_called_after_handler is True
        assert handler_called is True
        mock_consumer.commit.assert_called_once_with(mock_msg)

    async def test_async_commit_retry_logic(self):
        """Test that async commit failures are retried with backoff."""
        mock_model = MagicMock()
        mock_handler = MagicMock()
        mock_consumer = MagicMock()

        # Make commit fail twice, then succeed
        commit_attempt_count = 0

        def commit_with_retries(_msg):
            nonlocal commit_attempt_count
            commit_attempt_count += 1
            if commit_attempt_count < 3:
                raise Exception("Transient failure")

        mock_consumer.commit.side_effect = commit_with_retries

        loop = AsyncConsumerLoop(
            model=mock_model, handler=mock_handler, commit_strategy="before_processing"
        )
        loop._consumer = mock_consumer

        mock_msg = MagicMock()

        with patch("asyncio.sleep"):  # Speed up test by mocking sleep
            result = await loop._commit_offset(mock_msg, max_attempts=3)

        # Verify commit was retried and eventually succeeded
        assert result is True
        assert commit_attempt_count == 3


@pytest.mark.unit
class TestRetryLogSuppression:
    """Tests for retry log suppression when max_retries=0."""

    def test_no_retry_logs_when_max_retries_zero(self):
        """Test that no retry logs are emitted when max_retries=0."""
        mock_model = MagicMock()
        mock_model._deserialize_avro.return_value = {"test": "data"}

        def failing_handler(_data):
            raise Exception("Handler failed")

        mock_consumer = MagicMock()

        loop = ConsumerLoop(
            model=mock_model,
            handler=failing_handler,
            commit_strategy="after_processing",
            max_retries=0,
        )
        loop._consumer = mock_consumer

        mock_msg = MagicMock()
        mock_msg.value.return_value = b"test_data"

        with patch("flowodm.consumer.logger") as mock_logger:
            loop._process_message(mock_msg)

            # Verify no warning logs about retry attempts
            warning_logs = [call_args[0][0] for call_args in mock_logger.warning.call_args_list]
            assert not any("attempt" in log for log in warning_logs)

            # Verify no error logs about max retries exceeded
            error_logs = [call_args[0][0] for call_args in mock_logger.error.call_args_list]
            assert not any("Max retries exceeded" in log for log in error_logs)

    def test_retry_logs_when_max_retries_positive(self):
        """Test that retry logs are emitted when max_retries > 0."""
        mock_model = MagicMock()
        mock_model._deserialize_avro.return_value = {"test": "data"}

        def failing_handler(_data):
            raise Exception("Handler failed")

        mock_consumer = MagicMock()

        loop = ConsumerLoop(
            model=mock_model,
            handler=failing_handler,
            commit_strategy="after_processing",
            max_retries=1,
        )
        loop._consumer = mock_consumer

        mock_msg = MagicMock()
        mock_msg.value.return_value = b"test_data"

        with patch("flowodm.consumer.logger") as mock_logger:
            with patch("time.sleep"):  # Speed up test
                loop._process_message(mock_msg)

            # Verify warning logs about retry attempts are present
            warning_logs = [call_args[0][0] for call_args in mock_logger.warning.call_args_list]
            assert any("attempt" in log for log in warning_logs)

            # Verify error log about max retries exceeded is present
            error_logs = [call_args[0][0] for call_args in mock_logger.error.call_args_list]
            assert any("Max retries exceeded" in log for log in error_logs)

    async def test_async_no_retry_logs_when_max_retries_zero(self):
        """Test that no retry logs are emitted in async loop when max_retries=0."""
        mock_model = MagicMock()
        mock_model._deserialize_avro.return_value = {"test": "data"}

        async def failing_handler(_data):
            raise Exception("Handler failed")

        mock_consumer = MagicMock()

        loop = AsyncConsumerLoop(
            model=mock_model,
            handler=failing_handler,
            commit_strategy="after_processing",
            max_retries=0,
        )
        loop._consumer = mock_consumer

        mock_msg = MagicMock()
        mock_msg.value.return_value = b"test_data"

        with patch("flowodm.consumer.logger") as mock_logger:
            await loop._process_message(mock_msg)

            # Verify no warning logs about retry attempts
            warning_logs = [call_args[0][0] for call_args in mock_logger.warning.call_args_list]
            assert not any("attempt" in log for log in warning_logs)

            # Verify no error logs about max retries exceeded
            error_logs = [call_args[0][0] for call_args in mock_logger.error.call_args_list]
            assert not any("Max retries exceeded" in log for log in error_logs)


@pytest.mark.unit
class TestErrorHandlerDeserializedMessage:
    """Tests for error handler receiving deserialized message parameter."""

    def test_sync_error_handler_receives_none_on_deserialization_failure(self):
        """Test that sync error handler receives None when deserialization fails."""
        mock_model = MagicMock()
        mock_model._deserialize_avro.side_effect = Exception("Deserialization failed")

        mock_handler = MagicMock()

        # Track error handler calls
        error_handler_calls = []

        def track_error_handler(error, raw_msg, deserialized):
            error_handler_calls.append((error, raw_msg, deserialized))

        mock_consumer = MagicMock()

        loop = ConsumerLoop(
            model=mock_model,
            handler=mock_handler,
            error_handler=track_error_handler,
            commit_strategy="after_processing",
            max_retries=0,  # No retries for faster test
        )
        loop._consumer = mock_consumer

        mock_msg = MagicMock()
        mock_msg.value.return_value = b"invalid_data"

        with patch("flowodm.consumer.logger"):
            loop._process_message(mock_msg)

        # Verify error handler was called with None for deserialized
        assert len(error_handler_calls) == 1
        error, raw_msg, deserialized = error_handler_calls[0]
        assert str(error) == "Deserialization failed"
        assert raw_msg is mock_msg
        assert deserialized is None

        # Verify the actual handler was never called
        mock_handler.assert_not_called()

    def test_sync_error_handler_receives_instance_on_handler_failure(self):
        """Test that sync error handler receives deserialized instance when handler fails."""
        mock_model = MagicMock()
        mock_instance = MagicMock()
        mock_model._deserialize_avro.return_value = mock_instance

        def failing_handler(_data):
            raise Exception("Handler failed")

        # Track error handler calls
        error_handler_calls = []

        def track_error_handler(error, raw_msg, deserialized):
            error_handler_calls.append((error, raw_msg, deserialized))

        mock_consumer = MagicMock()

        loop = ConsumerLoop(
            model=mock_model,
            handler=failing_handler,
            error_handler=track_error_handler,
            commit_strategy="after_processing",
            max_retries=0,  # No retries for faster test
        )
        loop._consumer = mock_consumer

        mock_msg = MagicMock()
        mock_msg.value.return_value = b"valid_data"

        with patch("flowodm.consumer.logger"):
            loop._process_message(mock_msg)

        # Verify error handler was called with the deserialized instance
        assert len(error_handler_calls) == 1
        error, raw_msg, deserialized = error_handler_calls[0]
        assert str(error) == "Handler failed"
        assert raw_msg is mock_msg
        assert deserialized is mock_instance

    async def test_async_error_handler_receives_none_on_deserialization_failure(self):
        """Test that async error handler receives None when deserialization fails."""
        mock_model = MagicMock()
        mock_model._deserialize_avro.side_effect = Exception("Deserialization failed")

        async def mock_handler(_data):
            pass

        # Track error handler calls
        error_handler_calls = []

        async def track_error_handler(error, raw_msg, deserialized):
            error_handler_calls.append((error, raw_msg, deserialized))

        mock_consumer = MagicMock()

        loop = AsyncConsumerLoop(
            model=mock_model,
            handler=mock_handler,
            error_handler=track_error_handler,
            commit_strategy="after_processing",
            max_retries=0,  # No retries for faster test
        )
        loop._consumer = mock_consumer

        mock_msg = MagicMock()
        mock_msg.value.return_value = b"invalid_data"

        with patch("flowodm.consumer.logger"):
            await loop._process_message(mock_msg)

        # Verify error handler was called with None for deserialized
        assert len(error_handler_calls) == 1
        error, raw_msg, deserialized = error_handler_calls[0]
        assert str(error) == "Deserialization failed"
        assert raw_msg is mock_msg
        assert deserialized is None

    async def test_async_error_handler_receives_instance_on_handler_failure(self):
        """Test that async error handler receives deserialized instance when handler fails."""
        mock_model = MagicMock()
        mock_instance = MagicMock()
        mock_model._deserialize_avro.return_value = mock_instance

        async def failing_handler(_data):
            raise Exception("Handler failed")

        # Track error handler calls
        error_handler_calls = []

        async def track_error_handler(error, raw_msg, deserialized):
            error_handler_calls.append((error, raw_msg, deserialized))

        mock_consumer = MagicMock()

        loop = AsyncConsumerLoop(
            model=mock_model,
            handler=failing_handler,
            error_handler=track_error_handler,
            commit_strategy="after_processing",
            max_retries=0,  # No retries for faster test
        )
        loop._consumer = mock_consumer

        mock_msg = MagicMock()
        mock_msg.value.return_value = b"valid_data"

        with patch("flowodm.consumer.logger"):
            await loop._process_message(mock_msg)

        # Verify error handler was called with the deserialized instance
        assert len(error_handler_calls) == 1
        error, raw_msg, deserialized = error_handler_calls[0]
        assert str(error) == "Handler failed"
        assert raw_msg is mock_msg
        assert deserialized is mock_instance
