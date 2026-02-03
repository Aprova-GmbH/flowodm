"""
Consumer loop patterns for building Kafka microservices.

Provides both synchronous and asynchronous consumer loops with:
- Graceful shutdown handling
- Error handling and retry logic
- Lifecycle hooks (on_startup, on_shutdown)
- Configurable commit strategies
"""

from __future__ import annotations

import asyncio
import logging
import signal
from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING, Any

from flowodm.exceptions import ConsumerError
from flowodm.settings import BaseSettings, LongRunningSettings

if TYPE_CHECKING:
    from flowodm.model import FlowBaseModel

logger = logging.getLogger(__name__)


class ConsumerLoop:
    """
    Synchronous consumer loop for processing Kafka messages.

    Provides a main loop for microservices that consume and process messages.
    Handles graceful shutdown on SIGTERM/SIGINT signals.

    Example:
        >>> def process_order(order: OrderEvent) -> None:
        ...     print(f"Processing order {order.order_id}")
        ...
        >>> loop = ConsumerLoop(
        ...     model=OrderEvent,
        ...     handler=process_order,
        ...     settings=LongRunningSettings(),
        ... )
        >>> loop.run()  # Blocking
    """

    def __init__(
        self,
        model: type[FlowBaseModel],
        handler: Callable[[Any], None],
        settings: BaseSettings | None = None,
        group_id: str | None = None,
        error_handler: Callable[[Exception, Any], None] | None = None,
        on_startup: Callable[[], None] | None = None,
        on_shutdown: Callable[[], None] | None = None,
        commit_strategy: str = "per_message",
        max_retries: int = 3,
        retry_delay: float = 1.0,
        poll_timeout: float = 1.0,
    ):
        """
        Initialize consumer loop.

        Args:
            model: FlowBaseModel subclass to consume
            handler: Function to process each message
            settings: Kafka settings profile (defaults to LongRunningSettings)
            group_id: Consumer group ID (uses model's Settings if not specified)
            error_handler: Optional function to handle processing errors
            on_startup: Optional function called before loop starts
            on_shutdown: Optional function called after loop stops
            commit_strategy: "per_message", "per_batch", or "manual"
            max_retries: Maximum retry attempts for failed messages
            retry_delay: Delay between retries in seconds
            poll_timeout: Kafka poll timeout in seconds
        """
        self.model = model
        self.handler = handler
        self.settings = settings or LongRunningSettings()
        self.group_id = group_id
        self.error_handler = error_handler
        self.on_startup = on_startup
        self.on_shutdown = on_shutdown
        self.commit_strategy = commit_strategy
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.poll_timeout = poll_timeout

        self._running = False
        self._consumer: Any = None

    def _setup_signal_handlers(self) -> None:
        """Setup signal handlers for graceful shutdown."""
        signal.signal(signal.SIGTERM, self._signal_handler)
        signal.signal(signal.SIGINT, self._signal_handler)

    def _signal_handler(self, signum: int, frame: Any) -> None:
        """Handle shutdown signals."""
        logger.info(f"Received signal {signum}, initiating graceful shutdown...")
        self.stop()

    def stop(self) -> None:
        """Signal the loop to stop gracefully."""
        self._running = False

    def run(self) -> None:
        """
        Start the consumer loop (blocking).

        Processes messages until stop() is called or a signal is received.
        """
        self._setup_signal_handlers()

        # Call startup hook
        if self.on_startup:
            logger.info("Running startup hook...")
            self.on_startup()

        # Get consumer
        self._consumer = self.model.get_consumer(self.group_id, self.settings)
        self._running = True

        logger.info(
            f"Starting consumer loop for {self.model.__name__} "
            f"on topic {self.model._get_topic()}"
        )

        try:
            while self._running:
                msg = self._consumer.poll(self.poll_timeout)

                if msg is None:
                    continue

                if msg.error():
                    logger.warning(f"Consumer error: {msg.error()}")
                    continue

                self._process_message(msg)

        except Exception as e:
            logger.error(f"Consumer loop error: {e}")
            raise ConsumerError(f"Consumer loop failed: {e}") from e

        finally:
            # Call shutdown hook
            if self.on_shutdown:
                logger.info("Running shutdown hook...")
                self.on_shutdown()

            # Close consumer
            if self._consumer:
                logger.info("Closing consumer...")
                self._consumer.close()

            logger.info("Consumer loop stopped")

    def _process_message(self, msg: Any) -> None:
        """Process a single message with retry logic."""
        retries = 0

        while retries <= self.max_retries:
            try:
                # Deserialize message
                value = msg.value()
                if value is None:
                    return
                instance = self.model._deserialize_avro(value)

                # Call handler
                self.handler(instance)

                # Commit based on strategy
                if self.commit_strategy == "per_message":
                    self._consumer.commit(msg)

                return

            except Exception as e:
                retries += 1
                logger.warning(
                    f"Error processing message (attempt {retries}/{self.max_retries + 1}): {e}"
                )

                if retries > self.max_retries:
                    logger.error(f"Max retries exceeded for message: {e}")

                    if self.error_handler:
                        try:
                            self.error_handler(e, msg)
                        except Exception as handler_error:
                            logger.error(f"Error handler failed: {handler_error}")

                    # Commit failed message to not block
                    if self.commit_strategy == "per_message":
                        self._consumer.commit(msg)

                    return

                # Wait before retry
                import time

                time.sleep(self.retry_delay)


class AsyncConsumerLoop:
    """
    Asynchronous consumer loop for processing Kafka messages.

    Supports concurrent message processing with configurable parallelism.

    Example:
        >>> async def process_order(order: OrderEvent) -> None:
        ...     await external_api.submit(order)
        ...
        >>> loop = AsyncConsumerLoop(
        ...     model=OrderEvent,
        ...     handler=process_order,
        ...     max_concurrent=20,
        ... )
        >>> await loop.run()
    """

    def __init__(
        self,
        model: type[FlowBaseModel],
        handler: Callable[[Any], Awaitable[None]],
        settings: BaseSettings | None = None,
        group_id: str | None = None,
        error_handler: Callable[[Exception, Any], Awaitable[None]] | None = None,
        on_startup: Callable[[], Awaitable[None]] | None = None,
        on_shutdown: Callable[[], Awaitable[None]] | None = None,
        max_concurrent: int = 10,
        commit_strategy: str = "per_message",
        max_retries: int = 3,
        retry_delay: float = 1.0,
        poll_timeout: float = 1.0,
    ):
        """
        Initialize async consumer loop.

        Args:
            model: FlowBaseModel subclass to consume
            handler: Async function to process each message
            settings: Kafka settings profile
            group_id: Consumer group ID
            error_handler: Optional async function to handle errors
            on_startup: Optional async function called before loop starts
            on_shutdown: Optional async function called after loop stops
            max_concurrent: Maximum concurrent message processing tasks
            commit_strategy: "per_message", "per_batch", or "manual"
            max_retries: Maximum retry attempts
            retry_delay: Delay between retries
            poll_timeout: Kafka poll timeout
        """
        self.model = model
        self.handler = handler
        self.settings = settings or LongRunningSettings()
        self.group_id = group_id
        self.error_handler = error_handler
        self.on_startup = on_startup
        self.on_shutdown = on_shutdown
        self.max_concurrent = max_concurrent
        self.commit_strategy = commit_strategy
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.poll_timeout = poll_timeout

        self._running = False
        self._consumer: Any = None
        self._semaphore: asyncio.Semaphore | None = None

    def stop(self) -> None:
        """Signal the loop to stop gracefully."""
        self._running = False

    async def run(self) -> None:
        """
        Start the async consumer loop.

        Processes messages until stop() is called.
        """
        # Setup signal handlers for asyncio
        loop = asyncio.get_running_loop()
        try:
            for sig in (signal.SIGTERM, signal.SIGINT):
                loop.add_signal_handler(sig, self.stop)
        except NotImplementedError:
            # Windows doesn't support add_signal_handler on ProactorEventLoop
            # Fall back to signal.signal() for SIGINT (Ctrl+C)
            signal.signal(signal.SIGINT, lambda signum, frame: self.stop())

        # Call startup hook
        if self.on_startup:
            logger.info("Running async startup hook...")
            await self.on_startup()

        # Get consumer
        self._consumer = await self.model.get_async_consumer(self.group_id, self.settings)
        self._semaphore = asyncio.Semaphore(self.max_concurrent)
        self._running = True

        logger.info(
            f"Starting async consumer loop for {self.model.__name__} "
            f"on topic {self.model._get_topic()}"
        )

        pending_tasks: set[asyncio.Task[None]] = set()

        try:
            while self._running:
                # Poll for message
                if hasattr(self._consumer, "poll_async"):
                    msg = await self._consumer.poll_async(self.poll_timeout)
                else:
                    # Run sync poll in thread executor to avoid blocking event loop
                    msg = await asyncio.to_thread(self._consumer.poll, self.poll_timeout)

                if msg is None:
                    # Clean up completed tasks
                    done = {t for t in pending_tasks if t.done()}
                    pending_tasks -= done
                    # Yield to allow other tasks to run
                    await asyncio.sleep(0)
                    continue

                if msg.error():
                    logger.warning(f"Consumer error: {msg.error()}")
                    continue

                # Process message with concurrency limit
                await self._semaphore.acquire()
                task = asyncio.create_task(self._process_message_with_semaphore(msg))
                pending_tasks.add(task)

            # Wait for pending tasks on shutdown
            if pending_tasks:
                logger.info(f"Waiting for {len(pending_tasks)} pending tasks...")
                await asyncio.gather(*pending_tasks, return_exceptions=True)

        except Exception as e:
            logger.error(f"Async consumer loop error: {e}")
            raise ConsumerError(f"Async consumer loop failed: {e}") from e

        finally:
            # Call shutdown hook
            if self.on_shutdown:
                logger.info("Running async shutdown hook...")
                await self.on_shutdown()

            # Close consumer
            if self._consumer:
                logger.info("Closing async consumer...")
                try:
                    self._consumer.close()
                except Exception:
                    pass

            logger.info("Async consumer loop stopped")

    async def _process_message_with_semaphore(self, msg: Any) -> None:
        """Process message and release semaphore when done."""
        try:
            await self._process_message(msg)
        finally:
            if self._semaphore:
                self._semaphore.release()

    async def _process_message(self, msg: Any) -> None:
        """Process a single message with retry logic."""
        retries = 0

        while retries <= self.max_retries:
            try:
                # Deserialize message
                value = msg.value()
                if value is None:
                    return
                instance = self.model._deserialize_avro(value)

                # Call handler
                await self.handler(instance)

                # Commit based on strategy
                if self.commit_strategy == "per_message":
                    self._consumer.commit(msg)

                return

            except Exception as e:
                retries += 1
                logger.warning(
                    f"Error processing message (attempt {retries}/{self.max_retries + 1}): {e}"
                )

                if retries > self.max_retries:
                    logger.error(f"Max retries exceeded for message: {e}")

                    if self.error_handler:
                        try:
                            await self.error_handler(e, msg)
                        except Exception as handler_error:
                            logger.error(f"Async error handler failed: {handler_error}")

                    # Commit failed message
                    if self.commit_strategy == "per_message":
                        self._consumer.commit(msg)

                    return

                # Wait before retry
                await asyncio.sleep(self.retry_delay)


def consumer_loop(
    model: type[FlowBaseModel],
    settings: BaseSettings | None = None,
    **kwargs: Any,
) -> Callable[[Callable[[Any], None]], ConsumerLoop]:
    """
    Decorator to create a consumer loop from a handler function.

    Example:
        >>> @consumer_loop(model=OrderEvent, settings=LongRunningSettings())
        ... def handle_order(order: OrderEvent) -> None:
        ...     process_order(order)
        ...
        >>> handle_order.run()  # Start the loop
    """

    def decorator(handler: Callable[[Any], None]) -> ConsumerLoop:
        loop = ConsumerLoop(model=model, handler=handler, settings=settings, **kwargs)
        return loop

    return decorator


def async_consumer_loop(
    model: type[FlowBaseModel],
    settings: BaseSettings | None = None,
    **kwargs: Any,
) -> Callable[[Callable[[Any], Awaitable[None]]], AsyncConsumerLoop]:
    """
    Decorator to create an async consumer loop from a handler function.

    Example:
        >>> @async_consumer_loop(model=OrderEvent, max_concurrent=20)
        ... async def handle_order(order: OrderEvent) -> None:
        ...     await process_order(order)
        ...
        >>> await handle_order.run()  # Start the loop
    """

    def decorator(handler: Callable[[Any], Awaitable[None]]) -> AsyncConsumerLoop:
        loop = AsyncConsumerLoop(model=model, handler=handler, settings=settings, **kwargs)
        return loop

    return decorator
