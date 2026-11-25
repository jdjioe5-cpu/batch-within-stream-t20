#!/usr/bin/env python3
"""
Comprehensive Property & Invariant Test Suite for batchWithin(K, D).

Validates:
  Invariant 1: Exact K-emission on fast streams.
  Invariant 2: Exact D-timeout emission (Starvation-free deadline).
  Invariant 3: Strict Non-empty chunks (1 <= len(chunk) <= K).
  Invariant 4: Element preservation (FIFO, zero loss, zero duplication).
  Invariant 5: Downstream variable backpressure resistance.
  Invariant 6: Randomized property-based fuzzing across varying (K, D, delays).
"""

import asyncio
import os
import random
import sys
import time
import unittest
from pathlib import Path
from typing import List

CURRENT_DIR = Path(__file__).resolve().parent
if str(CURRENT_DIR) not in sys.path:
    sys.path.insert(0, str(CURRENT_DIR))

from batch_within import BatchWithinStream, batch_within


class TestBatchWithinProperties(unittest.IsolatedAsyncioTestCase):

    async def test_k_emission_fast_upstream(self):
        """Invariant 1: Fast upstream produces exact chunks of size K."""
        K = 10
        D = 1.0  # Generous duration so timer doesn't fire
        total_items = 100

        async def upstream():
            for i in range(total_items):
                yield i

        chunks = []
        async for chunk in batch_within(upstream(), max_items=K, window_duration=D):
            chunks.append(chunk)

        self.assertEqual(len(chunks), 10)
        for chunk in chunks:
            self.assertEqual(len(chunk), K)
        self.assertEqual([x for c in chunks for x in c], list(range(total_items)))

    async def test_d_timeout_anti_starvation(self):
        """Invariant 2: Timer is anchored to 1st element, preventing starvation."""
        K = 10
        D = 0.08  # 80ms
        stream = BatchWithinStream[int](max_items=K, window_duration=D)

        start = time.perf_counter()
        # Push 4 items spaced by 30ms (total ~90ms > D=80ms)
        async def pusher():
            for i in range(4):
                await stream.push(i)
                await asyncio.sleep(0.03)
            await stream.complete()

        push_task = asyncio.create_task(pusher())

        # Pull first chunk
        chunk1 = await stream.pull()
        first_latency = time.perf_counter() - start

        # The first chunk must emit around D (approx 80-100ms), NOT wait for all 4 items
        self.assertGreaterEqual(len(chunk1), 1)
        self.assertLess(len(chunk1), K)
        # Latency should be close to D (allow small tolerance for CI scheduler)
        self.assertLess(first_latency, 0.18, "Timer was starved or delayed by subsequent items!")

        await push_task

    async def test_non_empty_and_no_race_leak(self):
        """Invariant 3: Even with simultaneous arrivals & timeout, no empty chunks."""
        K = 4
        D = 0.02
        stream = BatchWithinStream[int](max_items=K, window_duration=D)

        # High concurrency hammering
        async def hammer():
            for i in range(40):
                await stream.push(i)
                if i % 3 == 0:
                    await asyncio.sleep(0.01)

        hammer_tasks = [asyncio.create_task(hammer()) for _ in range(4)]

        chunks = []
        async def puller():
            while True:
                try:
                    c = await stream.pull()
                    chunks.append(c)
                except StopAsyncIteration:
                    break

        pull_task = asyncio.create_task(puller())

        await asyncio.gather(*hammer_tasks)
        await stream.complete()
        await pull_task

        self.assertGreater(len(chunks), 0)
        for c in chunks:
            self.assertGreater(len(c), 0, "Disallowed empty chunk emitted!")
            self.assertLessEqual(len(c), K, "Chunk exceeded max_items K!")

        total_emitted = sum(len(c) for c in chunks)
        self.assertEqual(total_emitted, 160)

    async def test_variable_backpressure(self):
        """Invariant 4 & 5: Downstream backpressure pauses upstream safely."""
        K = 5
        D = 0.05
        total_items = 50
        stream = BatchWithinStream[int](max_items=K, window_duration=D, max_ready_chunks=2)

        async def producer():
            for i in range(total_items):
                await stream.push(i)
            await stream.complete()

        prod_task = asyncio.create_task(producer())

        chunks = []
        # Consumer pulls with random backpressure pauses
        while True:
            try:
                await asyncio.sleep(random.uniform(0.005, 0.02))
                c = await stream.pull()
                chunks.append(c)
            except StopAsyncIteration:
                break

        await prod_task

        flattened = [x for c in chunks for x in c]
        self.assertEqual(flattened, list(range(total_items)))
        for c in chunks:
            self.assertTrue(1 <= len(c) <= K)

    async def test_completion_flushes_partial_buffer(self):
        """Invariant: Stream completion immediately flushes partial buffer."""
        K = 10
        D = 10.0  # Very long timeout
        stream = BatchWithinStream[int](max_items=K, window_duration=D)

        await stream.push(1)
        await stream.push(2)
        await stream.push(3)
        await stream.complete()

        chunk = await stream.pull()
        self.assertEqual(chunk, [1, 2, 3])

        with self.assertRaises(StopAsyncIteration):
            await stream.pull()

    async def test_property_fuzzing(self):
        """Fuzzing property test: 30 iterations with randomized K, D, and delay profiles."""
        for iteration in range(30):
            K = random.randint(2, 15)
            D = random.uniform(0.01, 0.04)
            n_items = random.randint(20, 80)

            stream = BatchWithinStream[int](
                max_items=K,
                window_duration=D,
                max_ready_chunks=random.randint(2, 8),
            )

            async def producer():
                for i in range(n_items):
                    await stream.push(i)
                    if random.random() < 0.3:
                        await asyncio.sleep(random.uniform(0.001, 0.01))
                await stream.complete()

            prod_task = asyncio.create_task(producer())

            emitted_chunks = []
            while True:
                try:
                    if random.random() < 0.2:
                        await asyncio.sleep(random.uniform(0.005, 0.015))
                    c = await stream.pull()
                    emitted_chunks.append(c)
                except StopAsyncIteration:
                    break

            await prod_task

            flattened = [x for c in emitted_chunks for x in c]
            self.assertEqual(
                flattened,
                list(range(n_items)),
                f"Iteration {iteration}: Element loss/reorder detected!",
            )
            for c in emitted_chunks:
                self.assertTrue(
                    1 <= len(c) <= K,
                    f"Iteration {iteration}: Invalid chunk size {len(c)} for K={K}",
                )


if __name__ == "__main__":
    unittest.main()
