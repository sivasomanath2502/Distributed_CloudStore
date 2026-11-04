import unittest

from shared.chunking import (
    CHUNK_SIZE,
    split_object,
    reconstruct_object
)


class TestChunking(unittest.TestCase):

    def test_small_object(self):

        original = b"Hello Distributed Storage"

        chunks = split_object(original)

        self.assertEqual(len(chunks), 1)
        self.assertEqual(chunks[0]["size"], len(original))

        reconstructed = reconstruct_object(chunks)

        self.assertEqual(original, reconstructed)

    def test_exactly_one_chunk(self):

        original = b"A" * CHUNK_SIZE

        chunks = split_object(original)

        self.assertEqual(len(chunks), 1)
        self.assertEqual(chunks[0]["size"], CHUNK_SIZE)

        self.assertEqual(
            reconstruct_object(chunks),
            original
        )

    def test_multiple_chunks(self):

        original = b"A" * (2 * CHUNK_SIZE + 512)

        chunks = split_object(original)

        self.assertEqual(len(chunks), 3)

        self.assertEqual(chunks[0]["size"], CHUNK_SIZE)
        self.assertEqual(chunks[1]["size"], CHUNK_SIZE)
        self.assertEqual(chunks[2]["size"], 512)

        reconstructed = reconstruct_object(chunks)

        self.assertEqual(original, reconstructed)

    def test_chunk_order(self):

        original = b"A" * CHUNK_SIZE + b"B" * CHUNK_SIZE

        chunks = split_object(original)

        chunks.reverse()

        reconstructed = reconstruct_object(chunks)

        self.assertEqual(original, reconstructed)

    def test_unique_chunk_ids(self):

        original = b"A" * (3 * CHUNK_SIZE)

        chunks = split_object(original)

        chunk_ids = [
            chunk["chunk_id"] for chunk in chunks
        ]

        self.assertEqual(
            len(chunk_ids),
            len(set(chunk_ids))
        )

    def test_corrupted_chunk(self):

        original = b"A" * CHUNK_SIZE

        chunks = split_object(original)

        corrupted_content = bytearray(
            chunks[0]["content"]
        )

        corrupted_content[0] = ord("B")

        chunks[0]["content"] = bytes(corrupted_content)

        with self.assertRaises(ValueError):
            reconstruct_object(chunks)

    def test_empty_object(self):

        original = b""

        chunks = split_object(original)

        self.assertEqual(len(chunks), 0)

        self.assertEqual(
            reconstruct_object(chunks),
            original
        )


if __name__ == "__main__":
    unittest.main()