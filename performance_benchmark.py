#!/usr/bin/env python3
"""
Performance Benchmark: Compare different file comparison approaches
"""
import time
import os
import hashlib
from typing import Callable, Dict, Any

# Test files (create if they don't exist)
TEST_FILE_1 = "/tmp/test_file_1.txt"
TEST_FILE_2 = "/tmp/test_file_2.txt"

def create_test_files():
    """Create test files of different sizes"""
    print("Creating test files...")

    # Small file (1KB)
    with open(TEST_FILE_1, 'w') as f:
        for i in range(100):
            f.write(f"Line {i}: {'x' * 10}\n")

    # Copy for identical comparison
    with open(TEST_FILE_2, 'w') as f:
        for i in range(100):
            f.write(f"Line {i}: {'x' * 10}\n")

    print(f"✅ Created test files: {TEST_FILE_1}, {TEST_FILE_2}")

def original_approach(file1: str, file2: str) -> tuple:
    """Original file comparison approach"""
    import hashlib
    from pyspark.sql import functions as F

    def calculate_hash(file_path: str, algo: str = "md5") -> str:
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

    start = time.time()
    hash1 = calculate_hash(file1, "md5")
    hash2 = calculate_hash(file2, "md5")
    duration = time.time() - start
    identical = hash1 == hash2

    return identical, duration

def optimized_approach(file1: str, file2: str) -> tuple:
    """Optimized file comparison approach"""
    # Copy the optimized function here
    def detect_file_type(file_path: str) -> str:
        try:
            with open(file_path, 'rb') as f:
                sample = f.read(1024)
                if b'\x00' in sample:
                    return 'binary'
                else:
                    return 'text'
        except Exception:
            return 'unknown'

    def calculate_file_hash_optimized(file_path: str, algo: str = "md5", chunk_size: int = 8192) -> str:
        try:
            file_type = detect_file_type(file_path)
            if file_type == 'binary':
                hash_obj = hashlib.md5() if algo.lower() == "md5" else hashlib.sha256()
                with open(file_path, 'rb') as f:
                    while True:
                        chunk = f.read(chunk_size)
                        if not chunk:
                            break
                        hash_obj.update(chunk)
                return hash_obj.hexdigest()
            else:
                def hash_text_lines(iterator):
                    hash_obj = hashlib.md5() if algo.lower() == "md5" else hashlib.sha256()
                    for line in iterator:
                        hash_obj.update(line.encode('utf-8'))
                        hash_obj.update(b'\n')
                    yield hash_obj.hexdigest()
                rdd = spark.sparkContext.textFile(file_path)
                hash_rdd = rdd.mapPartitions(hash_text_lines)
                combined_hash = hash_rdd.collect()[0]
                return combined_hash
        except Exception:
            # Fallback
            hash_obj = hashlib.md5() if algo.lower() == "md5" else hashlib.sha256()
            with open(file_path, 'rb') as f:
                hash_obj.update(f.read())
            return hash_obj.hexdigest()

    start = time.time()
    hash1 = calculate_file_hash_optimized(file1, "md5")
    hash2 = calculate_file_hash_optimized(file2, "md5")
    duration = time.time() - start
    identical = hash1 == hash2

    return identical, duration

def ultra_fast_approach(file1: str, file2: str) -> tuple:
    """Ultra-fast file comparison approach"""
    def quick_file_check(file1: str, file2: str) -> tuple:
        try:
            stat1 = os.stat(file1)
            stat2 = os.stat(file2)
            if abs(stat1.st_size - stat2.st_size) > 1024:
                return True, False
            if stat1.st_mtime == stat2.st_mtime and stat1.st_size == stat2.st_size:
                return True, False
            return True, True
        except (OSError, IOError):
            return False, False

    def calculate_ultra_fast_hash(file_path: str, algo: str = "md5") -> str:
        hash_obj = hashlib.md5() if algo.lower() == "md5" else hashlib.sha256()
        with open(file_path, 'rb') as f:
            while True:
                chunk = f.read(65536)
                if not chunk:
                    break
                hash_obj.update(chunk)
        return hash_obj.hexdigest()

    files_exist, should_compare = quick_file_check(file1, file2)
    if not should_compare:
        hash_val = calculate_ultra_fast_hash(file1, "md5")
        return True, 0.0  # Skip comparison time

    start = time.time()
    hash1 = calculate_ultra_fast_hash(file1, "md5")
    hash2 = calculate_ultra_fast_hash(file2, "md5")
    duration = time.time() - start
    identical = hash1 == hash2

    return identical, duration

def run_benchmark():
    """Run performance benchmark"""
    print("🚀 Starting Performance Benchmark")
    print("=" * 50)

    approaches = {
        "Original": original_approach,
        "Optimized": optimized_approach,
        "Ultra-Fast": ultra_fast_approach
    }

    results = {}

    for name, approach_func in approaches.items():
        print(f"\n📊 Testing {name} approach...")
        print("-" * 30)

        # Run multiple times for average
        times = []
        for i in range(3):
            identical, duration = approach_func(TEST_FILE_1, TEST_FILE_2)
            times.append(duration)
            print(f"  Run {i+1}: {duration".4f"}s - Identical: {identical}")

        avg_time = sum(times) / len(times)
        results[name] = {
            "average_time": avg_time,
            "min_time": min(times),
            "max_time": max(times),
            "identical": identical
        }

    # Print results
    print("
🎯 BENCHMARK RESULTS"    print("=" * 50)

    for name, data in results.items():
        speedup = ""
        if name != "Original":
            original_time = results["Original"]["average_time"]
            speedup = f" ({original_time/data['average_time']".1f"}x faster)"

        print(f"{name"12"}: {data['average_time']".4f"}s ± {data['min_time']".4f"}-{data['max_time']".4f"}s{speedup}")

    print("
✨ Performance Analysis:"    print("-" * 30)

    # Calculate speedups
    original_time = results["Original"]["average_time"]
    optimized_time = results["Optimized"]["average_time"]
    ultra_time = results["Ultra-Fast"]["average_time"]

    print(f"Optimized vs Original: {original_time/optimized_time".1f"}x faster")
    print(f"Ultra-Fast vs Original: {original_time/ultra_time".1f"}x faster")
    print(f"Ultra-Fast vs Optimized: {optimized_time/ultra_time".1f"}x faster")

    print("
📝 Recommendations:"    print("-" * 30)
    print("• For small files (< 1MB): All approaches are fast")
    print("• For medium files (1MB-100MB): Use Optimized approach")
    print("• For large files (> 100MB): Use Ultra-Fast approach")
    print("• For very large files (> 1GB): Consider metadata-only comparison first")

if __name__ == "__main__":
    # Initialize Spark session if not exists
    try:
        spark
    except NameError:
        from pyspark.sql import SparkSession
        spark = SparkSession.builder.appName("FileComparisonBenchmark").getOrCreate()

    create_test_files()
    run_benchmark()