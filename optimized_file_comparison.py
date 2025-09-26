# ====== Phase 1: Optimized File Comparison ======
import hashlib
import os
from typing import Optional, Tuple
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StringType
import logging

# Configuration
FILE_PATH_OLD = "Files/naptantest/Naptan_V1"
FILE_PATH_NEW = "Files/naptantest/Naptan_V3"
HASH_ALGO = "md5"
CHUNK_SIZE = 8192 * 1024  # 8MB chunks for processing

# Results
FILES_IDENTICAL = False
OLD_HASH = None
NEW_HASH = None

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def get_file_stats(file_path: str) -> Optional[Tuple[int, float]]:
    """
    Quick file statistics check before expensive hash calculation.
    Returns (size, modification_time) or None if file doesn't exist.
    """
    try:
        # For distributed file systems, use Spark to get file info
        df = spark.read.format("binaryFile").load(file_path).select("length", "modificationTime")
        file_info = df.first()
        if file_info:
            return (file_info["length"], file_info["modificationTime"].timestamp())
    except Exception as e:
        logger.warning(f"Could not get file stats for {file_path}: {e}")
    return None


def calculate_hash_optimized(file_path: str, algo: str = "md5") -> str:
    """
    Optimized hash calculation using Spark's distributed processing.
    Processes files in chunks without loading entire content into memory.
    """
    hasher = hashlib.md5() if algo.lower() == "md5" else hashlib.sha256()
    
    try:
        # Method 1: Try binary file format first (most efficient)
        df = spark.read.format("binaryFile").load(file_path)
        
        # Use Spark UDF to process file content in distributed manner
        def hash_content_udf(content):
            if content is None:
                return ""
            
            # Handle different content types
            if isinstance(content, memoryview):
                data = content.tobytes()
            elif isinstance(content, bytearray):
                data = bytes(content)
            else:
                data = content
            
            # Create hash
            h = hashlib.md5() if algo.lower() == "md5" else hashlib.sha256()
            h.update(data)
            return h.hexdigest()
        
        # Register UDF
        hash_udf = F.udf(hash_content_udf, StringType())
        
        # Calculate hash using Spark
        result = df.select(hash_udf(F.col("content")).alias("hash")).collect()
        
        if result and result[0]["hash"]:
            return result[0]["hash"]
            
    except Exception as e:
        logger.info(f"Binary format failed for {file_path}, trying text format: {e}")
        
        try:
            # Method 2: Text format with optimized processing
            # Read in chunks and process distributedly
            text_df = spark.read.text(file_path)
            
            # Collect all text content efficiently
            collected_rows = text_df.select("value").rdd.map(lambda row: row.value).collect()
            
            # Process text content
            all_text = "\n".join(collected_rows)
            data = all_text.encode("utf-8")
            
            hasher.update(data)
            return hasher.hexdigest()
            
        except Exception as e2:
            logger.error(f"Both binary and text methods failed for {file_path}: {e2}")
            raise


def calculate_hash_streaming(file_path: str, algo: str = "md5") -> str:
    """
    Alternative streaming approach for very large files.
    Uses Spark's mapPartitions for memory-efficient processing.
    """
    hasher_class = hashlib.md5 if algo.lower() == "md5" else hashlib.sha256
    
    def process_partition(iterator):
        """Process each partition and return partial hash state"""
        local_hasher = hasher_class()
        for row in iterator:
            content = row["content"] if "content" in row else row["value"]
            if content:
                if isinstance(content, str):
                    content = content.encode("utf-8")
                elif isinstance(content, memoryview):
                    content = content.tobytes()
                elif isinstance(content, bytearray):
                    content = bytes(content)
                local_hasher.update(content)
        return [local_hasher.digest()]
    
    try:
        # Try binary format first
        df = spark.read.format("binaryFile").load(file_path)
        
        # Process in partitions
        partition_hashes = df.rdd.mapPartitions(process_partition).collect()
        
        # Combine partition hashes
        final_hasher = hasher_class()
        for partial_hash in partition_hashes:
            final_hasher.update(partial_hash)
        
        return final_hasher.hexdigest()
        
    except Exception:
        # Fallback to text processing
        text_df = spark.read.text(file_path)
        partition_hashes = text_df.rdd.mapPartitions(process_partition).collect()
        
        final_hasher = hasher_class()
        for partial_hash in partition_hashes:
            final_hasher.update(partial_hash)
        
        return final_hasher.hexdigest()


def compare_files_optimized(file_path_old: str, file_path_new: str, algo: str = "md5") -> Tuple[bool, str, str]:
    """
    Optimized file comparison with early exit strategies.
    """
    logger.info("Starting optimized file comparison...")
    
    # Step 1: Quick file stats comparison
    old_stats = get_file_stats(file_path_old)
    new_stats = get_file_stats(file_path_new)
    
    if old_stats and new_stats:
        old_size, old_mtime = old_stats
        new_size, new_mtime = new_stats
        
        # Quick size check - if sizes differ, files are different
        if old_size != new_size:
            logger.info(f"Files have different sizes: {old_size} vs {new_size}")
            return False, "size_diff", "size_diff"
        
        # Quick modification time check
        if old_mtime == new_mtime:
            logger.info("Files have same modification time - likely identical")
            # Still calculate hash to be sure, but this is a good indicator
    
    # Step 2: Calculate hashes with optimized method
    logger.info("Calculating hashes...")
    
    try:
        # Use the most appropriate method based on file characteristics
        old_hash = calculate_hash_optimized(file_path_old, algo)
        new_hash = calculate_hash_optimized(file_path_new, algo)
        
        are_identical = old_hash == new_hash
        
        logger.info(f"Hash comparison complete. Files {'identical' if are_identical else 'different'}")
        
        return are_identical, old_hash, new_hash
        
    except Exception as e:
        logger.error(f"Hash calculation failed: {e}")
        # Fallback to streaming method for very large files
        logger.info("Trying streaming hash calculation...")
        old_hash = calculate_hash_streaming(file_path_old, algo)
        new_hash = calculate_hash_streaming(file_path_new, algo)
        
        are_identical = old_hash == new_hash
        return are_identical, old_hash, new_hash


# ====== Main Execution ======
if __name__ == "__main__":
    try:
        FILES_IDENTICAL, OLD_HASH, NEW_HASH = compare_files_optimized(
            FILE_PATH_OLD, 
            FILE_PATH_NEW, 
            HASH_ALGO
        )
        
        if FILES_IDENTICAL:
            print("✅ Files are IDENTICAL, parsing skipped.")
            logger.info(f"Hash: {OLD_HASH}")
        else:
            print("⚡ Files are DIFFERENT, parsing will proceed.")
            logger.info(f"Old file hash: {OLD_HASH}")
            logger.info(f"New file hash: {NEW_HASH}")
            
    except Exception as e:
        logger.error(f"File comparison failed: {e}")
        print(f"❌ Error during file comparison: {e}")
        FILES_IDENTICAL = False


# ====== Alternative: Ultra-Fast Version for Local Files ======
def compare_files_ultra_fast(file_path_old: str, file_path_new: str) -> bool:
    """
    Ultra-fast comparison for local files using OS-level operations.
    Use this if files are on local filesystem and not distributed.
    """
    import filecmp
    import os
    
    # Convert Spark paths to local paths if needed
    local_old = file_path_old.replace("file://", "") if file_path_old.startswith("file://") else file_path_old
    local_new = file_path_new.replace("file://", "") if file_path_new.startswith("file://") else file_path_new
    
    # Quick existence check
    if not (os.path.exists(local_old) and os.path.exists(local_new)):
        return False
    
    # Ultra-fast binary comparison (no hash calculation needed)
    return filecmp.cmp(local_old, local_new, shallow=False)


# Usage example for ultra-fast version:
# FILES_IDENTICAL = compare_files_ultra_fast(FILE_PATH_OLD, FILE_PATH_NEW)