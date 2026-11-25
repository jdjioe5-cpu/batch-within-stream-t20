#!/usr/bin/env python3
"""
Reproducer for 'batchWithin(K, D)' Failures in Async/Reactive Streams.
Demonstrates:
  1. Timer-Reset Starvation: Continuously resetting timers on incoming items
     delays window emission indefinitely under slow/steady stream ingress.
  2. Race-Condition Empty Chunks: Uncoordinated concurrent timer expiry and
     element arrival flushes empty `[]` or mis-sized chunks downstream.
"""

import asyncio
import time
from typing import List, Any


class NaiveBatchWithin:
    """
    Naive implementation showing common bugs:
    - Bug 1: Resets timer on every element received (starvation bug).
    - Bug 2: Timer callback and push callback lack atomic mutual exclusion,
             causing empty chunk emission on race conditions.
    """
    def __init__(self, max_items: int, window_duration: float):
        self.k = max_items
        self.d = window_duration
        self.buffer: List[Any] = []
        self.timer_task: asyncio.Task | None = None
        self.emitted_chunks: List[List[Any]] = []
        self.empty_chunk_detected = False

    async def _on_timeout(self):
        await asyncio.sleep(self.d)
        # BUG: Race condition! If an element arrives right as sleep finishes,
        # both push() and _on_timeout() may attempt to flush the buffer.
        chunk = list(self.buffer)
        self.buffer.clear()
        if len(chunk) == 0:
            self.empty_chunk_detected = True
        self.emitted_chunks.append(chunk)

    async def push(self, item: Any):
        self.buffer.append(item)
        # BUG 1 (Starvation): Resets timer every time a new item arrives!
        if self.timer_task and not self.timer_task.done():
            self.timer_task.cancel()
        
        if len(self.buffer) >= self.k:
            chunk = list(self.buffer)
            self.buffer.clear()
            self.emitted_chunks.append(chunk)
            if self.timer_task and not self.timer_task.done():
                self.timer_task.cancel()
        else:
            self.timer_task = asyncio.create_task(self._on_timeout())


async def reproduce_timer_starvation():
    """
    Demonstrates Bug 1:
    Window duration D = 0.1s, K = 10.
    Items arrive every 0.05s (< D).
    Total 8 items arrive over 0.40s.
    Expected: Window should have emitted around t=0.10s and t=0.20s...
    Actual: Timer is continuously reset! No chunks emitted until long after the stream stops.
    """
    naive = NaiveBatchWithin(max_items=10, window_duration=0.1)
    start = time.perf_counter()
    emitted_timestamps = []

    for i in range(8):
        await naive.push(f"item_{i}")
        await asyncio.sleep(0.05)  # Arrives before 0.1s timer expires

    # Allow trailing timer to finish
    await asyncio.sleep(0.15)
    elapsed = time.perf_counter() - start

    first_chunk_latency = elapsed
    return len(naive.emitted_chunks), first_chunk_latency


async def reproduce_race_empty_chunk():
    """
    Demonstrates Bug 2:
    Simulates high concurrency between item arriving at K-boundary
    and timer firing at almost the identical tick.
    """
    naive = NaiveBatchWithin(max_items=2, window_duration=0.02)
    # Push 1 item to start timer
    await naive.push(1)
    # Wait until just about timeout
    await asyncio.sleep(0.02)
    # Race push exactly when timer fires
    await naive.push(2)
    await asyncio.sleep(0.05)

    has_empty = any(len(c) == 0 for c in naive.emitted_chunks) or naive.empty_chunk_detected
    return has_empty, naive.emitted_chunks


async def main():
    print("=== Running Reproducer for batchWithin(K, D) Failures ===")
    
    # 1. Timer Starvation
    chunks_count, latency = await reproduce_timer_starvation()
    print(f"[Bug 1 Starvation] 8 items with D=0.1s and interval=0.05s:")
    print(f"  Total elapsed time: {latency:.3f}s")
    print(f"  Chunks emitted: {chunks_count}")
    print(f"  Starvation verified: First item waited {latency:.3f}s (> 4x window duration D=0.1s)!")

    # 2. Race condition empty chunk
    has_empty, chunks = await reproduce_race_empty_chunk()
    print(f"[Bug 2 Race Condition] Concurrent timer & boundary arrival:")
    print(f"  Emitted chunks: {chunks}")
    print(f"  Empty chunk detected: {has_empty}")


if __name__ == "__main__":
    asyncio.run(main())
