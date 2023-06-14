from resotodatalink.batch_stream import BatchStream
import pytest


@pytest.mark.asyncio
async def test_batch_stream() -> None:
    async def test_batch(total: int, batch_size: int, modulo: int, watermark: int) -> None:
        count = 0
        stream = BatchStream(iter(range(total)), lambda x: x % modulo, batch_size, watermark)
        async for key, elems in stream:
            assert key <= modulo
            assert len(elems) <= batch_size
            count += len(elems)
        assert count == total  # all elements are returned
        assert stream.count == 0  # no elements are left in the stream

    await test_batch(101, 10, 10, 100)  # 101 elements with batches of 10. watermark is never reached
    await test_batch(101, 1, 10, 100)  # 101 elements with batches of 1. watermark is reached
    await test_batch(101, 10, 2, 10)  # 101 elements with batches of 10 with a modulo of 2. watermark is reached
