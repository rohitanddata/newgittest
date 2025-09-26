# ====== Phase 1: File Comparison (Ultra Optimized) ======
import hashlib
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional, Tuple, List
from functools import partial

FILE_PATH_OLD = "Files/naptantest/Naptan_V1"
FILE_PATH_NEW = "Files/naptantest/Naptan_V3"
HASH_ALGO = "md5"

FILES_IDENTICAL = False
OLD_HASH = None
NEW_HASH = None

def quick_file_check(file1: str, file2: str) -> Tuple[bool, bool]:
    """
    Quick file comparison using metadata before expensive hashing
    Returns: (files_exist, should_compare_content)
    """
    try:
        stat1 = os.stat(file1)
        stat2 = os.stat(file2)

        # If file sizes differ significantly, definitely different
        size_diff = abs(stat1.st_size - stat2.st_size)
        if size_diff > 1024:  # Allow small differences (headers, etc.)
            return True, False

        # If modification times are identical and sizes match, likely same
        if stat1.st_mtime == stat2.st_mtime and stat1.st_size == stat2.st_size:
            return True, False  # Skip detailed comparison

        return True, True

    except (OSError, IOError):
        return False, False

def detect_file_type_fast(file_path: str) -> str:
    """Fast file type detection using magic bytes"""
    try:
        with open(file_path, 'rb') as f:
            # Read first 64 bytes for magic number detection
            header = f.read(64)

            # Check common binary signatures
            binary_signatures = [
                b'\x00\x00',  # Null bytes
                b'\xFF\xD8\xFF',  # JPEG
                b'\x89PNG\r\n\x1a\n',  # PNG
                b'PK\x03\x04',  # ZIP
                b'\x00\x01\x00\x00',  # Some binary formats
            ]

            for sig in binary_signatures:
                if header.startswith(sig):
                    return 'binary'

            return 'text'

    except Exception:
        return 'unknown'

def hash_binary_file_parallel(file_path: str, algo: str = "md5", chunk_size: int = 65536, max_workers: int = 4) -> str:
    """
    Ultra-fast binary file hashing using parallel chunk processing
    """
    def hash_chunk(chunk_data: bytes, hash_obj: hashlib.md5) -> bytes:
        """Hash a single chunk and return the hash object state"""
        chunk_hash = hash_obj.copy()
        chunk_hash.update(chunk_data)
        return chunk_hash.digest()

    try:
        file_size = os.path.getsize(file_path)
        hash_obj = hashlib.md5() if algo.lower() == "md5" else hashlib.sha256()

        if file_size <= chunk_size:
            # Small file, hash directly
            with open(file_path, 'rb') as f:
                hash_obj.update(f.read())
            return hash_obj.hexdigest()

        # Large file - parallel processing
        chunks = []
        with open(file_path, 'rb') as f:
            while True:
                chunk = f.read(chunk_size)
                if not chunk:
                    break
                chunks.append(chunk)

        # Process chunks in parallel
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            chunk_hashes = list(executor.map(
                partial(hash_chunk, hash_obj=hash_obj),
                chunks
            ))

        # Combine all chunk hashes for final result
        combined_hash = hash_obj.copy()
        for chunk_hash in sorted(chunk_hashes):  # Sort for consistent ordering
            combined_hash.update(chunk_hash)

        return combined_hash.hexdigest()

    except Exception as e:
        print(f"Binary hashing failed: {e}, using fallback")
        return hash_binary_file_simple(file_path, algo, chunk_size)

def hash_binary_file_simple(file_path: str, algo: str = "md5", chunk_size: int = 8192) -> str:
    """Simple chunked binary file hashing"""
    hash_obj = hashlib.md5() if algo.lower() == "md5" else hashlib.sha256()

    with open(file_path, 'rb') as f:
        while True:
            chunk = f.read(chunk_size)
            if not chunk:
                break
            hash_obj.update(chunk)

    return hash_obj.hexdigest()

def hash_text_file_optimized(file_path: str, algo: str = "md5") -> str:
    """
    Optimized text file hashing using streaming approach
    """
    def hash_lines_streaming(iterator):
        hash_obj = hashlib.md5() if algo.lower() == "md5" else hashlib.sha256()
        for line in iterator:
            if line is not None:
                hash_obj.update(line.encode('utf-8', errors='ignore'))
                hash_obj.update(b'\n')
        yield hash_obj.hexdigest()

    try:
        rdd = spark.sparkContext.textFile(file_path, minPartitions=4)
        hash_rdd = rdd.mapPartitions(hash_lines_streaming)
        return hash_rdd.collect()[0]

    except Exception as e:
        print(f"Optimized text hashing failed: {e}, using simple approach")
        return hash_text_file_simple(file_path, algo)

def hash_text_file_simple(file_path: str, algo: str = "md5") -> str:
    """Simple text file hashing with line-by-line processing"""
    hash_obj = hashlib.md5() if algo.lower() == "md5" else hashlib.sha256()

    with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
        for line in f:
            hash_obj.update(line.encode('utf-8'))
            hash_obj.update(b'\n')

    return hash_obj.hexdigest()

def calculate_ultra_fast_hash(file_path: str, algo: str = "md5") -> str:
    """
    Ultra-fast file hashing with intelligent type detection and parallel processing
    """
    start_time = time.time()

    try:
        file_type = detect_file_type_fast(file_path)
        print(f"  Detected file type: {file_type} ({time.time() - start_time".3f"}s)")

        if file_type == 'binary':
            # Use parallel processing for binary files
            return hash_binary_file_parallel(file_path, algo)
        else:
            # Use optimized streaming for text files
            return hash_text_file_optimized(file_path, algo)

    except Exception as e:
        print(f"  Ultra-fast hashing failed: {e}, using simple method")
        file_type = detect_file_type_fast(file_path)
        if file_type == 'binary':
            return hash_binary_file_simple(file_path, algo)
        else:
            return hash_text_file_simple(file_path, algo)

def compare_files_ultra_fast(file1: str, file2: str, algo: str = "md5") -> Tuple[bool, Optional[str], Optional[str]]:
    """
    Ultra-fast file comparison with pre-checks and parallel processing
    """
    print("🚀 Starting ultra-fast file comparison...")

    # Quick metadata check
    files_exist, should_compare = quick_file_check(file1, file2)
    if not files_exist:
        print("❌ One or both files don't exist")
        return False, None, None

    if not should_compare:
        print("⚡ Files appear identical based on metadata - skipping detailed comparison")
        # Read one file to get hash for consistency
        hash_val = calculate_ultra_fast_hash(file1, algo)
        return True, hash_val, hash_val

    # Full comparison needed
    start_time = time.time()

    try:
        with ThreadPoolExecutor(max_workers=2) as executor:
            future1 = executor.submit(calculate_ultra_fast_hash, file1, algo)
            future2 = executor.submit(calculate_ultra_fast_hash, file2, algo)

            hash1 = future1.result()
            hash2 = future2.result()

        comparison_time = time.time() - start_time
        print(f"  Hash comparison completed in {comparison_time:.3f".3f"

        are_identical = hash1 == hash2
        return are_identical, hash1, hash2

    except Exception as e:
        print(f"❌ Error in ultra-fast comparison: {e}")
        return False, None, None

# Main execution
print("🔬 Starting comparison...")

FILES_IDENTICAL, OLD_HASH, NEW_HASH = compare_files_ultra_fast(FILE_PATH_OLD, FILE_PATH_NEW, HASH_ALGO)

if FILES_IDENTICAL:
    print("✅ Files are IDENTICAL, parsing skipped.")
else:
    print("⚡ Files are DIFFERENT, parsing will proceed.")

if OLD_HASH and NEW_HASH:
    print(f"📊 Hash comparison complete - Old: {OLD_HASH[:16]}... New: {NEW_HASH[:16]}...")

# Performance summary
print("
=== Performance Summary ===")
print(f"File 1 hash: {OLD_HASH}")
print(f"File 2 hash: {NEW_HASH}")
print(f"Identical: {FILES_IDENTICAL}")