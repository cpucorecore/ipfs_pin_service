#!/usr/bin/env python3
"""
Generate a random file for IPFS testing.

Defaults to using the OS CSPRNG (/dev/urandom or platform equivalent) for speed and
cryptographic-quality randomness. For maximum entropy from a blocking TRNG, use
--source random to read from /dev/random (may be very slow and block).
"""

from __future__ import annotations

import argparse
import hashlib
import math
import os
import sys
from typing import Callable, Optional


def _ensure_parent_dir(path: str) -> None:
    directory = os.path.dirname(os.path.abspath(path))
    if directory and not os.path.exists(directory):
        os.makedirs(directory, exist_ok=True)


def _read_from_device(device_path: str) -> Callable[[int], bytes]:
    f = open(device_path, "rb", buffering=0)

    def reader(num_bytes: int) -> bytes:
        return f.read(num_bytes)

    # Attach file handle for later cleanup
    setattr(reader, "_fh", f)
    return reader


def _read_from_os_urandom() -> Callable[[int], bytes]:
    def reader(num_bytes: int) -> bytes:
        return os.urandom(num_bytes)

    return reader


def _read_from_secrets() -> Callable[[int], bytes]:
    # Secrets may be backed by platform CSPRNG (good quality randomness)
    import secrets

    def reader(num_bytes: int) -> bytes:
        return secrets.token_bytes(num_bytes)

    return reader


def generate_random_file(
    output_path: str,
    size_mb: int,
    source: str = "urandom",
    chunk_mb: int = 8,
    fsync: bool = True,
) -> str:
    """Generate a random file.

    Args:
        output_path: Destination file path.
        size_mb: Desired size in megabytes (MiB = 1024*1024 bytes per MB here).
        source: 'urandom' (default), 'random' (blocking TRNG), or 'secrets'.
        chunk_mb: Chunk size in MB for streaming write.
        fsync: If True, fsync the file to disk before returning.

    Returns:
        Hex string of the SHA-256 digest of the generated content.
    """
    if size_mb <= 0:
        raise ValueError("size_mb must be positive")
    if chunk_mb <= 0:
        raise ValueError("chunk_mb must be positive")

    total_bytes = int(size_mb) * 1024 * 1024
    chunk_bytes = int(chunk_mb) * 1024 * 1024

    if source == "random":
        # Maximum entropy from kernel TRNG (blocking, slow for large sizes)
        reader = _read_from_device("/dev/random")
    elif source == "urandom":
        # Fast, cryptographically secure
        # Prefer direct device read on Unix to avoid per-call overhead
        if os.path.exists("/dev/urandom"):
            reader = _read_from_device("/dev/urandom")
        else:
            reader = _read_from_os_urandom()
    elif source == "secrets":
        reader = _read_from_secrets()
    else:
        raise ValueError("source must be one of: urandom, random, secrets")

    _ensure_parent_dir(output_path)

    sha256 = hashlib.sha256()
    bytes_remaining = total_bytes

    with open(output_path, "wb") as out_f:
        try:
            while bytes_remaining > 0:
                to_read = chunk_bytes if bytes_remaining >= chunk_bytes else bytes_remaining
                data = reader(to_read)
                if not data:
                    raise RuntimeError("Random source returned no data; possibly interrupted")
                out_f.write(data)
                sha256.update(data)
                bytes_remaining -= len(data)

            if fsync:
                out_f.flush()
                os.fsync(out_f.fileno())
        finally:
            # Close device handle if present
            fh = getattr(reader, "_fh", None)
            if fh is not None:
                fh.close()

    return sha256.hexdigest()


def parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate a random file for IPFS testing")
    parser.add_argument("--output", "-o", required=True, help="Output file path")
    parser.add_argument(
        "--size-mb",
        "-s",
        type=int,
        required=True,
        help="Size to generate in MB (MiB)",
    )
    parser.add_argument(
        "--source",
        choices=["urandom", "random", "secrets"],
        default="urandom",
        help=(
            "Randomness source: urandom (fast, secure, default), "
            "random (blocking TRNG, slow), secrets (CSPRNG)"
        ),
    )
    parser.add_argument(
        "--chunk-mb",
        type=int,
        default=8,
        help="Chunk size in MB for streaming writes (default: 8)",
    )
    parser.add_argument(
        "--no-fsync",
        action="store_true",
        help="Do not fsync the file before exiting",
    )
    return parser.parse_args(argv)


def main(argv: Optional[list[str]] = None) -> int:
    args = parse_args(argv)
    try:
        digest = generate_random_file(
            output_path=args.output,
            size_mb=args.size_mb,
            source=args.source,
            chunk_mb=args.chunk_mb,
            fsync=not args.no_fsync,
        )
    except Exception as exc:  # noqa: BLE001
        print(f"Error: {exc}", file=sys.stderr)
        return 1

    print(
        f"Generated: {args.output}\n"
        f"Size: {args.size_mb} MB\n"
        f"Source: {args.source}\n"
        f"SHA-256: {digest}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


