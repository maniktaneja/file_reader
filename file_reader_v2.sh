#!/bin/bash

# Script: file_reader_dd.sh
# Purpose: Read files from a list using dd and write to /dev/null
# Usage: ./file_reader_dd.sh [file_list] [options]

# Function to display usage
show_usage() {
    echo "Usage: $0 FILE_LIST [OPTIONS]"
    echo ""
    echo "Reads files from a list using dd and writes to /dev/null"
    echo ""
    echo "Arguments:"
    echo "  FILE_LIST         File containing list of file paths (one per line)"
    echo ""
    echo "Options:"
    echo "  -h, --help        Show this help message"
    echo "  -b, --block-size SIZE  DD block size (default: 1M)"
    echo "  -j, --jobs NUM    Number of parallel jobs (default: 1)"
    echo "  -s, --skip-errors Continue processing even if files can't be read"
    echo ""
    echo "Block Size Examples:"
    echo "  512, 1K, 4K, 1M, 8M, 64M, 1G"
    echo ""
    echo "Examples:"
    echo "  $0 file_list.txt                    # Basic file reading"
    echo "  $0 files.txt -b 8M -j 4             # Use 8M blocks with 4 parallel jobs"
    echo "  $0 files.txt -s -j 8                # Skip errors with 8 parallel jobs"
}

# Cleanup function
cleanup() {
    echo ""
    echo "Processing interrupted."
    # Kill any running background jobs
    for pid in "${pids[@]}"; do
        kill "$pid" 2>/dev/null
    done
    # Clean up temporary files
    rm -f "$COUNTER_FILE" "$LOCK_FILE"
    exit 0
}

# Set up signal handlers
trap cleanup SIGINT SIGTERM

# Default values
BLOCK_SIZE="1M"
PARALLEL_JOBS=1
SKIP_ERRORS=false
processed_files=0
failed_files=0

# Create lock file for atomic counter updates
LOCK_FILE="/tmp/file_reader_dd_lock_$"
COUNTER_FILE="/tmp/file_reader_dd_counter_$"
echo "0" > "$COUNTER_FILE"

# Parse command line arguments
if [[ $# -eq 0 ]]; then
    show_usage
    exit 1
fi

FILE_LIST="$1"
shift

while [[ $# -gt 0 ]]; do
    case $1 in
        -h|--help)
            show_usage
            exit 0
            ;;
        -b|--block-size)
            if [[ -z "$2" ]]; then
                echo "Error: -b/--block-size requires a value"
                exit 1
            fi
            BLOCK_SIZE="$2"
            shift 2
            ;;
        -j|--jobs)
            if [[ -z "$2" ]] || ! [[ "$2" =~ ^[1-9][0-9]*$ ]]; then
                echo "Error: -j/--jobs requires a positive integer"
                exit 1
            fi
            PARALLEL_JOBS="$2"
            shift 2
            ;;
        -s|--skip-errors)
            SKIP_ERRORS=true
            shift
            ;;
        -*)
            echo "Error: Unknown option $1"
            echo "Use -h or --help for usage information"
            exit 1
            ;;
        *)
            echo "Error: Unexpected argument $1"
            exit 1
            ;;
    esac
done

# Validate input file
if [[ ! -f "$FILE_LIST" ]]; then
    echo "Error: File list '$FILE_LIST' does not exist" >&2
    exit 1
fi

if [[ ! -r "$FILE_LIST" ]]; then
    echo "Error: Cannot read file list '$FILE_LIST'" >&2
    exit 1
fi

# Count total files
total_files=$(grep -c "^[^#[:space:]]" "$FILE_LIST" 2>/dev/null || echo "0")

# Show startup information
echo "File Reader DD - Starting processing"
echo "Input file: $FILE_LIST"
echo "Total files to process: $total_files"
echo "Block size: $BLOCK_SIZE"
echo "Parallel jobs: $PARALLEL_JOBS"
echo "================================="
echo ""

# Function to atomically increment counter
increment_counter() {
    local counter_type="$1"  # "processed" or "failed"
    (
        flock -x 200
        current_processed=$(sed -n '1p' "$COUNTER_FILE")
        current_failed=$(sed -n '2p' "$COUNTER_FILE")
        
        if [[ "$counter_type" == "processed" ]]; then
            current_processed=$((current_processed + 1))
        else
            current_failed=$((current_failed + 1))
        fi
        
        # Write both counters back to file
        echo "$current_processed" > "$COUNTER_FILE"
        echo "$current_failed" >> "$COUNTER_FILE"
        echo "$current_processed"
    ) 200>"$LOCK_FILE"
}

# Function to get current counter
get_counter() {
    local counter_type="$1"  # "processed" or "failed"
    (
        flock -s 200
        if [[ "$counter_type" == "processed" ]]; then
            sed -n '1p' "$COUNTER_FILE"
        else
            sed -n '2p' "$COUNTER_FILE"
        fi
    ) 200>"$LOCK_FILE"
}

# Function to process a single file
process_file() {
    local file_path="$1"
    local total_files="$2"
    
    # Check if file exists and is readable
    if [[ ! -f "$file_path" ]]; then
        local current_count=$(increment_counter "failed")
        echo "[$current_count/$total_files] ERROR: File not found: $file_path"
        return 1
    fi
    
    if [[ ! -r "$file_path" ]]; then
        local current_count=$(increment_counter "failed")
        echo "[$current_count/$total_files] ERROR: File not readable: $file_path"
        return 1
    fi
    
    # Increment processed counter and get current count
    local current_count=$(increment_counter "processed")
    
    # Show the dd command being executed
    echo "[$current_count/$total_files] dd if=\"$file_path\" of=/dev/null bs=$BLOCK_SIZE"
    
    # Execute dd command
    if ! dd if="$file_path" of=/dev/null bs="$BLOCK_SIZE" 2>/dev/null; then
        increment_counter "failed" > /dev/null
        echo "[$current_count/$total_files] ERROR: DD failed for $file_path"
        return 1
    fi
    
    return 0
}

# Process files with parallelism
pids=()
job_count=0

while IFS= read -r file_path || [[ -n "$file_path" ]]; do
    # Skip empty lines and comments
    [[ -z "$file_path" || "$file_path" =~ ^[[:space:]]*# ]] && continue
    
    # Wait for available slot if max jobs reached
    while [[ ${#pids[@]} -ge $PARALLEL_JOBS ]]; do
        for i in "${!pids[@]}"; do
            if ! kill -0 "${pids[i]}" 2>/dev/null; then
                wait "${pids[i]}"
                exit_code=$?
                unset "pids[i]"
                if [[ $exit_code -ne 0 && "$SKIP_ERRORS" != true ]]; then
                    echo "Stopping due to error (use -s to skip errors)"
                    # Kill remaining jobs
                    for pid in "${pids[@]}"; do
                        kill "$pid" 2>/dev/null
                    done
                    exit 1
                fi
            fi
        done
        pids=("${pids[@]}")  # Reindex array
        sleep 0.1
    done
    
    # Start processing file in background
    process_file "$file_path" "$total_files" &
    pids+=($!)
    ((job_count++))
    
done < "$FILE_LIST"

# Wait for all remaining jobs to complete
for pid in "${pids[@]}"; do
    wait "$pid"
    exit_code=$?
    if [[ $exit_code -ne 0 && "$SKIP_ERRORS" != true ]]; then
        echo "Stopping due to error (use -s to skip errors)"
        exit 1
    fi
done

# Get final counts
final_processed=$(get_counter "processed")
final_failed=$(get_counter "failed")

# Clean up temporary files
rm -f "$COUNTER_FILE" "$LOCK_FILE"

# Display final results
echo ""
echo "================================="
echo "Processing completed"
echo "Files processed: $final_processed / $total_files"
echo "Failed files: $final_failed"

exit 0
