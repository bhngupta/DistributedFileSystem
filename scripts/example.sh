#!/bin/bash 
# Data Grid Simulation

API_BASE="http://localhost:8000"
UPLOADED_FILES=()

echo "Data Grid Simulation Starting..."
echo "Using API: $API_BASE"

# Verify API is accessible
if curl -s "$API_BASE/health" > /dev/null; then
    echo "API is accessible"
else
    echo "ERROR: Cannot connect to API at $API_BASE"
    echo "Please make sure the controller is running"
    exit 1
fi

# data upload
upload_data() {
    local category=$1
    local run_number=$2
    local event_count=$3
    
    local filename="${category}_run${run_number}_events${event_count}.dat"
    local content="# Generalized ${category} Data
# Run: ${run_number}
# Events: ${event_count}
# Timestamp: $(date)
# Data Quality: GOOD
$(for i in $(seq 1 $event_count); do echo "Event_${i}: $(openssl rand -hex 16)"; done)"
    
    echo "$content" > "/tmp/$filename"
    
    local response=$(curl -s -X POST "$API_BASE/files/upload" \
         -F "uploaded_file=@/tmp/$filename")
    
    local file_id=$(echo "$response" | jq -r '.file_id')
    if [ "$file_id" != "null" ] && [ "$file_id" != "" ]; then
        UPLOADED_FILES+=("$file_id")
        echo "Uploaded ${category} data: $filename (ID: $file_id, Events: $event_count)"
    else
        echo "Failed to upload $filename"
    fi
    
    rm "/tmp/$filename"
}

# downloading data
download_analysis_data() {
    
    if [ ${#UPLOADED_FILES[@]} -gt 0 ]; then
        local random_index=$((RANDOM % ${#UPLOADED_FILES[@]}))
        local file_id="${UPLOADED_FILES[$random_index]}"
        curl -s "$API_BASE/files/$file_id" > /dev/null
        echo "🔍 Analysis job downloaded data: $file_id"
    else
    
        local api_response=$(curl -s "$API_BASE/files")
        local file_count=$(echo "$api_response" | jq '.files | length')
        
        if [ "$file_count" -gt 0 ]; then
            # Get a random file from the list
            local random_index=$((RANDOM % file_count))
            
    
            local file_id=$(echo "$api_response" | jq -r ".files[$random_index].file_id")
            
            if [ "$file_id" != "null" ] && [ ! -z "$file_id" ]; then
                curl -s "$API_BASE/files/$file_id" > /dev/null
                echo "🔍 Analysis job downloaded data: $file_id"
    
                UPLOADED_FILES+=("$file_id")
            else
                echo "Could not extract valid file ID from API response"
            fi
        else
            echo "No data available for analysis (no files found)"
        fi
    fi
}

# Monte Carlo simulation upload
upload_mc_simulation() {
    local process=$1
    local nevents=$2
    
    local filename="mc_${process}_${nevents}k_events.root"
    local content="# Monte Carlo Simulation Data
# Process: ${process}
# Generated Events: ${nevents}000
# Generator: Generic Generator
# Timestamp: $(date)
$(for i in $(seq 1 $((nevents*10))); do echo "MC_Event_${i}: $(openssl rand -hex 8)"; done)"
    
    echo "$content" > "/tmp/$filename"
    
    local response=$(curl -s -X POST "$API_BASE/files/upload" \
         -F "uploaded_file=@/tmp/$filename")
    
    local file_id=$(echo "$response" | jq -r '.file_id')
    if [ "$file_id" != "null" ] && [ "$file_id" != "" ]; then
        UPLOADED_FILES+=("$file_id")
        echo "Monte Carlo uploaded: $filename (ID: $file_id, Process: $process)"
    fi
    
    rm "/tmp/$filename"
}

# grid status
check_grid_status() {
    local total_files=$(curl -s "$API_BASE/files" | jq '.files | length')
    echo "Grid Status: $total_files datasets available"
}

CATEGORIES=("CategoryA" "CategoryB" "CategoryC" "CategoryD")
MC_PROCESSES=("Process1" "Process2" "Process3" "Process4" "Process5")

echo ""
echo "Starting multi-category data simulation..."

# Simulate 8 hours of data generation
for hour in {1..8}; do
    echo ""
    echo "Hour $hour of data generation ==="
    
    for category in "${CATEGORIES[@]}"; do
        # Random number of runs per hour (1-3)
        runs=$((RANDOM % 3 + 1))
        
        for run in $(seq 1 $runs); do
            run_number=$((1000 + RANDOM % 9000))
            event_count=$((RANDOM % 1000 + 100))
            upload_data "$category" "$run_number" "$event_count" &
        done
    done
    
    # Monte Carlo production
    mc_count=$((RANDOM % 3 + 1))
    for i in $(seq 1 $mc_count); do
        process=${MC_PROCESSES[$((RANDOM % ${#MC_PROCESSES[@]}))]}
        nevents=$((RANDOM % 50 + 10))
        upload_mc_simulation "$process" "$nevents" &
    done
    
    # Wait for uploads to complete before trying to download
    wait
    
    # Now do the downloads
    analysis_jobs=$((RANDOM % 5 + 2))
    for i in $(seq 1 $analysis_jobs); do
        download_analysis_data &
    done
    
    # Wait for downloads to complete
    wait
    
    check_grid_status
    
    sleep 3

done

echo ""
echo "Data simulation completed!"
echo "Check monitoring dashboard!"
echo "Total files uploaded: ${#UPLOADED_FILES[@]}"

# Cleanup
rm -f temp-general.json 2>/dev/null

echo ""
echo "Grid Summary:"
curl -s "$API_BASE/files" | jq '.files | length' | xargs echo " Total datasets:"
echo "Ready for analysis!"
