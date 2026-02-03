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
        assert loop.max_retries == 3
        assert loop.retry_delay == 1.0
        assert loop.poll_timeout == 1.0
        assert loop.commit_strategy == "per_message"

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
        assert loop.max_retries == 3
        assert loop.retry_delay == 1.0
        assert loop.poll_timeout == 1.0
        assert loop.commit_strategy == "per_message"

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
