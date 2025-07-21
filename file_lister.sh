#!/bin/bash

NFS_PATH="$1"
OUTPUT_FILE="$2"

echo "Starting file discovery on: $NFS_PATH"
echo "Files will be written to $OUTPUT_FILE and displayed on screen"
echo "=================================================="

{
    cd "$NFS_PATH" && rsync -r --list-only . | grep "^-" | while read -r line; do
        filename=$(echo "$line" | awk '{for(i=5;i<=NF;i++) printf "%s%s", $i, (i<NF?" ":""); print ""}')
        echo "$NFS_PATH/$filename"
    done
} | tee "$OUTPUT_FILE"

echo "=================================================="
echo "Discovery completed. Total files: $(wc -l < "$OUTPUT_FILE")"
