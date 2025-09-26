# ====== Phase 1: File Comparison (Performance Optimized) ======
import hashlib
from pyspark.sql import functions as F
from pyspark import RDD
import os
from typing import Optional, Tuple

FILE_PATH_OLD = "Files/naptantest/Naptan_V1"
FILE_PATH_NEW = "Files/naptantest/Naptan_V3"
HASH_ALGO = "md5"

FILES_IDENTICAL = False
OLD_HASH = None
NEW_HASH = None

def detect_file_type(file_path: str) -> str:
    """Detect if file is binary or text based on content sample"""
    try:
        # Try to read first few bytes to detect encoding
        with open(file_path, 'rb') as f:
            sample = f.read(1024)
            # Check for null bytes (indicates binary)
            if b'\x00' in sample:
                return 'binary'
            else:
                return 'text'
    except Exception:
        return 'unknown'

def calculate_file_hash_optimized(file_path: str, algo: str = "md5", chunk_size: int = 8192) -> str:
    """
    Optimized file hashing using streaming/chunked approach
    - Avoids loading entire file into memory
    - Uses RDD for distributed processing when possible
    - Handles both binary and text files efficiently
    """
    try:
        file_type = detect_file_type(file_path)

        if file_type == 'binary':
            # For binary files, use direct file reading with chunks
            hash_obj = hashlib.md5() if algo.lower() == "md5" else hashlib.sha256()

            with open(file_path, 'rb') as f:
                while True:
                    chunk = f.read(chunk_size)
                    if not chunk:
                        break
                    hash_obj.update(chunk)

            return hash_obj.hexdigest()

        else:
            # For text files, use Spark's text reading but avoid collect()
            # Read file as RDD and process line by line
            def hash_text_lines(iterator):
                hash_obj = hashlib.md5() if algo.lower() == "md5" else hashlib.sha256()
                for line in iterator:
                    hash_obj.update(line.encode('utf-8'))
                    hash_obj.update(b'\n')  # Include newline in hash
                yield hash_obj.hexdigest()

            # Use RDD for better memory management
            rdd = spark.sparkContext.textFile(file_path)

            # Get hash using mapPartitions to process in parallel
            hash_rdd = rdd.mapPartitions(hash_text_lines)
            combined_hash = hash_rdd.collect()[0]

            return combined_hash

    except Exception as e:
        print(f"Warning: Falling back to original method for {file_path}: {str(e)}")
        # Fallback to original method if optimized approach fails
        return calculate_hash_fallback(file_path, algo)

def calculate_hash_fallback(file_path: str, algo: str = "md5") -> str:
    """Fallback method - original approach for compatibility"""
    try:
        df = spark.read.format("binaryFile").load(file_path).select("content")
        row = df.collect()[0]
        content = row["content"]
        if isinstance(content, memoryview):
            data = content.tobytes()
        elif isinstance(content, bytearray):
            data = bytes(content)
        else:
            data = content
    except Exception:
        txt_rows = spark.read.text(file_path).collect()
        all_text = "\n".join([r["value"] for r in txt_rows])
        data = all_text.encode("utf-8")

    h = hashlib.md5() if algo.lower() == "md5" else hashlib.sha256()
    h.update(data)
    return h.hexdigest()

def compare_files_efficient(file1: str, file2: str, algo: str = "md5") -> Tuple[bool, Optional[str], Optional[str]]:
    """
    Efficiently compare two files by hash
    Returns: (are_identical, hash1, hash2)
    """
    try:
        # Calculate hashes in parallel if possible
        print(f"Calculating hash for {file1}...")
        hash1 = calculate_file_hash_optimized(file1, algo)

        print(f"Calculating hash for {file2}...")
        hash2 = calculate_file_hash_optimized(file2, algo)

        are_identical = hash1 == hash2
        return are_identical, hash1, hash2

    except Exception as e:
        print(f"Error in file comparison: {str(e)}")
        return False, None, None

# Main comparison logic
print("🔍 Starting optimized file comparison...")

FILES_IDENTICAL, OLD_HASH, NEW_HASH = compare_files_efficient(FILE_PATH_OLD, FILE_PATH_NEW, HASH_ALGO)

if FILES_IDENTICAL:
    print("✅ Files are IDENTICAL, parsing skipped.")
else:
    print("⚡ Files are DIFFERENT, parsing will proceed.")

# Additional performance metrics
if OLD_HASH and NEW_HASH:
    print(f"📊 Hash comparison complete - Old: {OLD_HASH[:16]}... New: {NEW_HASH[:16]}...")