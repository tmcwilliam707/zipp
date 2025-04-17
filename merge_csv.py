import os
import pandas as pd
import csv
import chardet
import re
from pathlib import Path
import logging
import time
import psutil
import atexit
import multiprocessing
import gc

# Fix for semaphore warning: Disable multiprocessing parallelism
os.environ["OMP_NUM_THREADS"] = "1"  # Disable OpenMP parallelism
os.environ["MKL_NUM_THREADS"] = "1"  # Disable Intel MKL parallelism

# Explicitly clean up multiprocessing resources at shutdown
def cleanup_resources():
    multiprocessing.resource_tracker._tracker._fd = None  # Clear file descriptor
    multiprocessing.resource_tracker._tracker = None  # Clear tracker instance
atexit.register(cleanup_resources)

# Set up logging
logging.basicConfig(
    filename='csv_merge.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def detect_delimiter(file_path, sample_size=8192):
    """Detect the delimiter used in a CSV file."""
    try:
        with open(file_path, 'rb') as f:
            sample = f.read(sample_size).decode(errors='ignore')
            sniffer = csv.Sniffer()
            delimiter = sniffer.sniff(sample).delimiter
            return delimiter
    except Exception as e:
        logging.warning(f"Delimiter detection failed for {file_path}: {e}")
        return ','  # Default to comma

def detect_encoding(file_path):
    """Detect the file encoding."""
    try:
        with open(file_path, 'rb') as f:
            result = chardet.detect(f.read(8192))
            return result['encoding'] or 'utf-8'
    except Exception as e:
        logging.warning(f"Encoding detection failed for {file_path}: {e}")
        return 'utf-8'

def clean_row(row, line_number, file_name):
    """Clean a single row, logging issues."""
    cleaned = [str(cell).strip() if cell is not None else '' for cell in row]
    if len(cleaned) == 0:
        logging.warning(f"Empty row at line {line_number} in {file_name}")
    return cleaned

def manual_parse_csv(file_path, encoding, delimiter, chunk_size=5000):
    """Manually parse a CSV file, yielding chunks frequently."""
    rows = []
    max_columns = 0
    file_name = Path(file_path).name
    line_number = 0
    
    try:
        with open(file_path, 'r', encoding=encoding, errors='ignore') as f:
            file_size = os.path.getsize(file_path)
            processed = 0
            for line in f:
                line_number += 1
                processed += len(line.encode(encoding))
                if processed % 1000000 == 0:  # Update every ~1MB
                    logging.info(f"Processed {processed / file_size * 100:.1f}% of {file_name}")
                if not line.strip() or line.strip().startswith('#'):
                    continue
                try:
                    fields = list(csv.reader([line], delimiter=delimiter, quoting=csv.QUOTE_MINIMAL))[0]
                except Exception:
                    fields = line.split(delimiter)
                cleaned_fields = clean_row(fields, line_number, file_name)
                if cleaned_fields:
                    rows.append(cleaned_fields)
                    max_columns = max(max_columns, len(cleaned_fields))
                if len(rows) >= chunk_size:
                    yield from process_chunk(rows, max_columns, file_name)
                    rows = []  # Clear rows to free memory
            if rows:
                yield from process_chunk(rows, max_columns, file_name)
    except Exception as e:
        logging.error(f"Manual parsing failed for {file_path}: {e}")
        return
    finally:
        logging.info(f"Closed file handle for {file_name}")

def process_chunk(rows, max_columns, file_name):
    """Process a chunk of rows into a DataFrame."""
    if rows:
        # Pad or truncate rows to match max_columns
        for i, row in enumerate(rows):
            if len(row) < max_columns:
                rows[i] = row + [''] * (max_columns - len(row))
            elif len(row) > max_columns:
                logging.warning(f"Truncating row with {len(row)} fields to {max_columns} in {file_name}")
                rows[i] = row[:max_columns]
        columns = [f'col_{i}' for i in range(max_columns)]
        yield pd.DataFrame(rows, columns=columns)

def read_malformed_csv(file_path, chunk_size=5000):
    """Read a CSV, prioritizing manual parsing to preserve all data."""
    start_time = time.time()
    process = psutil.Process()
    encoding = detect_encoding(file_path)
    delimiter = detect_delimiter(file_path)
    file_name = Path(file_path).name
    
    logging.info(f"Open file descriptors: {len(process.open_files())}")
    
    # Try pandas first, using C engine and chunking
    base_kwargs = {
        'encoding': encoding,
        'delimiter': delimiter,
        'dtype': str,
        'keep_default_na': False,
        'na_values': [''],
        'quoting': csv.QUOTE_MINIMAL,
        'on_bad_lines': 'skip',  # Skip bad lines instead of erroring
        'engine': 'c',  # Use C engine to avoid Python engine's multiprocessing
    }

    try:
        chunks = pd.read_csv(file_path, chunksize=chunk_size, **base_kwargs)
        dfs = []
        for chunk in chunks:
            chunk.columns = [re.sub(r'[^\w\s]', '_', str(col)).strip() for col in chunk.columns]
            dfs.append(chunk)
        if dfs:
            df = pd.concat(dfs, ignore_index=True)
            logging.info(
                f"Successfully read {file_name} with pandas in {time.time() - start_time:.2f}s, "
                f"Memory: {process.memory_info().rss / 1024**2:.2f}MB"
            )
            return df
    except Exception as e:
        logging.info(f"Pandas failed for {file_name}: {e}. Falling back to manual parsing")
        # Fall back to manual parsing
        chunks = manual_parse_csv(file_path, encoding, delimiter, chunk_size=chunk_size)
        dfs = []
        for chunk in chunks:
            if chunk is not None and not chunk.empty:
                dfs.append(chunk)
        if dfs:
            df = pd.concat(dfs, ignore_index=True)
            df.columns = [re.sub(r'[^\w\s]', '_', str(col)).strip() for col in df.columns]
            logging.info(
                f"Successfully read {file_name} with manual parsing in {time.time() - start_time:.2f}s, "
                f"Memory: {process.memory_info().rss / 1024**2:.2f}MB"
            )
            return df
        logging.error(f"Failed to read {file_name}")
        return None

def main():
    # Define paths
    csv_dir = Path("/Users/taylormcwilliam/Downloads/698426317559824384/files/unzip_files")
    output_file = csv_dir / "merged.csv"

    # Ensure directory exists
    if not csv_dir.exists():
        logging.error(f"Directory {csv_dir} does not exist")
        print(f"Directory {csv_dir} does not exist")
        return

    all_columns = set()
    first_file = True
    process = psutil.Process()

    for file_path in csv_dir.glob("*.csv"):
        file_size = file_path.stat().st_size
        if file_size > 1_000_000_000:  # Warn for files > 1GB
            logging.warning(f"Large file detected: {file_path.name} ({file_size / 1024**2:.2f}MB)")
        print(f"Processing: {file_path.name}")
        logging.info(f"Processing: {file_path.name}")
        logging.info(f"Memory usage: {process.memory_info().rss / 1024**2:.2f}MB")
        logging.info(f"Open file descriptors: {len(process.open_files())}")

        df = read_malformed_csv(file_path, chunk_size=5000)
        if df is not None and not df.empty:
            all_columns.update(df.columns)
        else:
            print(f"Skipped {file_path.name} due to unrecoverable errors")
            logging.warning(f"Skipped {file_path.name} due to unrecoverable errors")
            continue

        # Align columns efficiently
        all_columns_sorted = sorted(list(all_columns))
        missing_cols = [col for col in all_columns_sorted if col not in df.columns]
        if missing_cols:
            # Create a DataFrame with missing columns filled with empty strings
            missing_df = pd.DataFrame(
                {col: '' for col in missing_cols},
                index=df.index
            )
            # Concatenate original and missing columns
            df = pd.concat([df, missing_df], axis=1)
        # Reorder columns to match all_columns_sorted
        df = df[all_columns_sorted]

        # Write to output file
        mode = 'w' if first_file else 'a'
        header = first_file
        df.to_csv(output_file, mode=mode, header=header, index=False, quoting=csv.QUOTE_MINIMAL)
        first_file = False
        logging.info(f"Appended {file_path.name} to {output_file}")

        # Free memory
        del df
        gc.collect()

    if first_file:
        print("No valid CSV files found to merge")
        logging.warning("No valid CSV files found to merge")
    else:
        print(f"Merged CSV saved to: {output_file}")
        logging.info(f"Merged CSV saved to: {output_file}")

if __name__ == "__main__":
    main()