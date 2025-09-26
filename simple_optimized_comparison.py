# ====== Phase 1: Optimized File Comparison (Drop-in Replacement) ======
import hashlib
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

FILE_PATH_OLD = "Files/naptantest/Naptan_V1"
FILE_PATH_NEW = "Files/naptantest/Naptan_V3"
HASH_ALGO = "md5"

FILES_IDENTICAL = False
OLD_HASH = None
NEW_HASH = None


def quick_file_check(file_path_old: str, file_path_new: str) -> bool:
    """
    Quick check using file metadata before expensive hash calculation.
    Returns True if files are likely identical, False if definitely different.
    """
    try:
        old_df = spark.read.format("binaryFile").load(file_path_old).select("length", "modificationTime")
        new_df = spark.read.format("binaryFile").load(file_path_new).select("length", "modificationTime")
        
        old_info = old_df.first()
        new_info = new_df.first()
        
        if old_info and new_info:
            # If sizes are different, files are definitely different
            if old_info["length"] != new_info["length"]:
                return False
            # If modification times are same, likely identical (but still need to verify)
            if old_info["modificationTime"] == new_info["modificationTime"]:
                return True
    except:
        pass  # Fall back to hash comparison
    
    return True  # Proceed with hash calculation


def calculate_hash(file_path: str, algo: str = "md5") -> str:
    """
    Optimized hash calculation that avoids loading entire file into driver memory.
    """
    hasher_type = hashlib.md5 if algo.lower() == "md5" else hashlib.sha256
    
    try:
        # Method 1: Use UDF to process binary content in Spark workers
        def hash_binary_content(content):
            if content is None:
                return ""
            
            # Handle different content types
            if isinstance(content, memoryview):
                data = content.tobytes()
            elif isinstance(content, bytearray):
                data = bytes(content)
            else:
                data = content
            
            # Calculate hash
            h = hasher_type()
            h.update(data)
            return h.hexdigest()
        
        # Register UDF and apply to content
        hash_udf = F.udf(hash_binary_content, StringType())
        df = spark.read.format("binaryFile").load(file_path)
        result = df.select(hash_udf(F.col("content")).alias("hash")).first()
        
        if result and result["hash"]:
            return result["hash"]
            
    except Exception:
        # Method 2: Fallback to text processing with reduced memory usage
        try:
            # Read text and process more efficiently
            text_df = spark.read.text(file_path)
            
            # Use mapPartitions to process data in chunks
            def hash_partition(iterator):
                h = hasher_type()
                for row in iterator:
                    if row["value"]:
                        h.update(row["value"].encode("utf-8"))
                        h.update(b"\n")  # Add newline separator
                return [h.hexdigest()]
            
            # Process partitions and combine results
            partition_hashes = text_df.rdd.mapPartitions(hash_partition).collect()
            
            # If only one partition, return its hash
            if len(partition_hashes) == 1:
                return partition_hashes[0]
            
            # Combine multiple partition hashes
            final_hasher = hasher_type()
            for ph in partition_hashes:
                final_hasher.update(ph.encode("utf-8"))
            return final_hasher.hexdigest()
            
        except Exception:
            # Method 3: Original approach as last resort (but optimized)
            txt_rows = spark.read.text(file_path).select("value").rdd.map(lambda r: r.value).collect()
            all_text = "\n".join(txt_rows)
            data = all_text.encode("utf-8")
            
            h = hasher_type()
            h.update(data)
            return h.hexdigest()


# ====== Main Comparison Logic ======

# Quick pre-check to avoid unnecessary hash calculations
if not quick_file_check(FILE_PATH_OLD, FILE_PATH_NEW):
    FILES_IDENTICAL = False
    OLD_HASH = "different_sizes"
    NEW_HASH = "different_sizes"
    print("⚡ Files are DIFFERENT (different sizes), parsing will proceed.")
else:
    # Perform hash comparison
    OLD_HASH = calculate_hash(FILE_PATH_OLD, algo=HASH_ALGO)
    NEW_HASH = calculate_hash(FILE_PATH_NEW, algo=HASH_ALGO)
    
    if OLD_HASH == NEW_HASH:
        FILES_IDENTICAL = True
        print("✅ Files are IDENTICAL, parsing skipped.")
    else:
        FILES_IDENTICAL = False
        print("⚡ Files are DIFFERENT, parsing will proceed.")