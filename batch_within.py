#!/usr/bin/env python3
"""
Race-Free Reactive/Async Stream Combinator: batchWithin(K, D).

Guarantees:
  1. Anti-Starvation Deadline: Timers are anchored to the arrival of the FIRST
     element in a window and are NEVER reset on subsequent items.
  2. Non-Empty Invariant: Emitted chunks strictly satisfy 1 <= len(chunk) <= K.
     Zero empty chunks [] under any race conditions.
  3. Full Backpressure Propagation: Bounded ready-chunk queue pauses fast upstream
     producers when downstream consumers are slow.
  4. Exact Element Preservation: Zero dropped or duplicated elements; strict FIFO.
"""

from __future__ import annotations

import asyncio
import collections
from typing import (
    AsyncIterator,
    Deque,
    Generic,
    List,
    Optional,
    TypeVar,
)

T = TypeVar("T")


class BatchWithinState:
    IDLE = "IDLE"
    BUFFERING = "BUFFERING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"


class BatchWithinStream(Generic[T]):
    """
    Deterministic, race-free state machine for batchWithin(max_items=K, window_duration=D).
    """

    def __init__(
        self,
        max_items: int,
        window_duration: float,
        max_ready_chunks: int = 16,
    ):
        if max_items <= 0:
            raise ValueError("max_items (K) must be positive")
        if window_duration <= 0:
            raise ValueError("window_duration (D) must be positive")

        self.k = max_items
        self.d = window_duration
        self.max_ready_chunks = max_ready_chunks

        self._lock = asyncio.Lock()
        self._buffer: List[T] = []
        self._ready_chunks: Deque[List[T]] = collections.deque()
        self._pending_pulls: Deque[asyncio.Future[List[T]]] = collections.deque()
        self._backpressure_event = asyncio.Event()
        self._backpressure_event.set()  # Initially unblocked

        self._timer_task: Optional[asyncio.Task] = None
        self._timer_epoch: int = 0
        self._state = BatchWithinState.IDLE
        self._error: Optional[Exception] = None

    async def _timer_worker(self, epoch: int, duration: float):
        """
        Anchored timer task for the current window epoch.
        """
        try:
            await asyncio.sleep(duration)
        except asyncio.CancelledError:
            return

        async with self._lock:
            # Stale timer check
            if epoch != self._timer_epoch or self._state in (
                BatchWithinState.COMPLETED,
                BatchWithinState.FAILED,
            ):
                return

            if self._buffer:
                self._flush_buffer_locked()

    def _flush_buffer_locked(self):
        """
        Flushes current buffer into a ready chunk and delivers to downstream.
        MUST be called while holding self._lock.
        """
        if not self._buffer:
            return

        chunk = self._buffer
        self._buffer = []
        self._timer_epoch += 1

        if self._timer_task and not self._timer_task.done():
            self._timer_task.cancel()
            self._timer_task = None

        if self._pending_pulls:
            waiter = self._pending_pulls.popleft()
            if not waiter.done():
                waiter.set_result(chunk)
        else:
            self._ready_chunks.append(chunk)
            if len(self._ready_chunks) >= self.max_ready_chunks:
                self._backpressure_event.clear()

        if self._state != BatchWithinState.COMPLETED:
            self._state = (
                BatchWithinState.BUFFERING if self._buffer else BatchWithinState.IDLE
            )

    async def push(self, item: T):
        """
        Push an item from upstream into the combinator.
        Respects downstream pull backpressure.
        """
        while True:
            await self._backpressure_event.wait()
            async with self._lock:
                if self._state == BatchWithinState.COMPLETED:
                    raise RuntimeError("Cannot push into a completed stream")
                if self._state == BatchWithinState.FAILED:
                    raise (self._error or RuntimeError("Stream failed"))

                if len(self._ready_chunks) < self.max_ready_chunks:
                    break
                # Backpressure triggered while waiting for lock; loop again

        async with self._lock:
            if not self._buffer:
                # First item in a new window: anchor the deadline!
                self._state = BatchWithinState.BUFFERING
                self._timer_epoch += 1
                self._timer_task = asyncio.create_task(
                    self._timer_worker(self._timer_epoch, self.d)
                )

            self._buffer.append(item)

            if len(self._buffer) >= self.k:
                # Reached K: emit immediately and cancel running timer
                self._flush_buffer_locked()

    async def pull(self) -> List[T]:
        """
        Pull a chunk from the stream.
        Raises StopAsyncIteration on normal stream completion.
        """
        async with self._lock:
            if self._ready_chunks:
                chunk = self._ready_chunks.popleft()
                if len(self._ready_chunks) < self.max_ready_chunks:
                    self._backpressure_event.set()
                return chunk

            if self._state == BatchWithinState.COMPLETED:
                if self._buffer:
                    self._flush_buffer_locked()
                    return self._ready_chunks.popleft()
                raise StopAsyncIteration

            if self._state == BatchWithinState.FAILED:
                raise (self._error or RuntimeError("Stream failed"))

            loop = asyncio.get_running_loop()
            fut: asyncio.Future[List[T]] = loop.create_future()
            self._pending_pulls.append(fut)

        try:
            return await fut
        finally:
            async with self._lock:
                if len(self._ready_chunks) < self.max_ready_chunks:
                    self._backpressure_event.set()

    async def complete(self):
        """
        Signal upstream stream completion.
        Flushes any remaining buffered items.
        """
        async with self._lock:
            if self._state in (BatchWithinState.COMPLETED, BatchWithinState.FAILED):
                return

            self._state = BatchWithinState.COMPLETED
            if self._buffer:
                self._flush_buffer_locked()

            if self._timer_task and not self._timer_task.done():
                self._timer_task.cancel()
                self._timer_task = None

            # Wake up any waiting downstream pulls with StopAsyncIteration
            while self._pending_pulls:
                waiter = self._pending_pulls.popleft()
                if not waiter.done():
                    waiter.set_exception(StopAsyncIteration())

    async def fail(self, error: Exception):
        """
        Signal stream failure.
        """
        async with self._lock:
            self._state = BatchWithinState.FAILED
            self._error = error
            self._buffer.clear()
            self._ready_chunks.clear()

            if self._timer_task and not self._timer_task.done():
                self._timer_task.cancel()
                self._timer_task = None

            while self._pending_pulls:
                waiter = self._pending_pulls.popleft()
                if not waiter.done():
                    waiter.set_exception(error)


async def batch_within(
    upstream: AsyncIterator[T],
    max_items: int,
    window_duration: float,
    max_ready_chunks: int = 16,
) -> AsyncIterator[List[T]]:
    """
    High-level async generator wrapping any async iterator with batchWithin(K, D).
    """
    stream: BatchWithinStream[T] = BatchWithinStream(
        max_items=max_items,
        window_duration=window_duration,
        max_ready_chunks=max_ready_chunks,
    )

    async def _producer():
        try:
            async for item in upstream:
                await stream.push(item)
            await stream.complete()
        except Exception as e:
            await stream.fail(e)

    producer_task = asyncio.create_task(_producer())
    try:
        while True:
            try:
                chunk = await stream.pull()
                yield chunk
            except StopAsyncIteration:
                break
    finally:
        if not producer_task.done():
            producer_task.cancel()
            await asyncio.gather(producer_task, return_exceptions=True)
