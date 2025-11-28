#!/bin/bash
# System Health Check - Pure Bash Implementation
# Replaces optimize-go

set -euo pipefail

# Get memory info in GB
get_memory_info() {
    local total_bytes used_gb total_gb

    # Total memory
    total_bytes=$(sysctl -n hw.memsize 2> /dev/null || echo "0")
    total_gb=$(awk "BEGIN {printf \"%.2f\", $total_bytes / (1024*1024*1024)}" 2> /dev/null || echo "0")
    [[ -z "$total_gb" || "$total_gb" == "" ]] && total_gb="0"

    # Used memory from vm_stat
    local vm_output active wired compressed page_size
    vm_output=$(vm_stat 2> /dev/null || echo "")
    page_size=4096

    active=$(echo "$vm_output" | awk '/Pages active:/ {print $NF}' | tr -d '.' 2> /dev/null || echo "0")
    wired=$(echo "$vm_output" | awk '/Pages wired down:/ {print $NF}' | tr -d '.' 2> /dev/null || echo "0")
    compressed=$(echo "$vm_output" | awk '/Pages occupied by compressor:/ {print $NF}' | tr -d '.' 2> /dev/null || echo "0")

    active=${active:-0}
    wired=${wired:-0}
    compressed=${compressed:-0}

    local used_bytes=$(((active + wired + compressed) * page_size))
    used_gb=$(awk "BEGIN {printf \"%.2f\", $used_bytes / (1024*1024*1024)}" 2> /dev/null || echo "0")
    [[ -z "$used_gb" || "$used_gb" == "" ]] && used_gb="0"

    echo "$used_gb $total_gb"
}

# Get disk info
get_disk_info() {
    local home="${HOME:-/}"
    local df_output total_gb used_gb used_percent

    df_output=$(df -k "$home" 2> /dev/null | tail -1)

    local total_kb used_kb
    total_kb=$(echo "$df_output" | awk '{print $2}' 2> /dev/null || echo "0")
    used_kb=$(echo "$df_output" | awk '{print $3}' 2> /dev/null || echo "0")

    total_kb=${total_kb:-0}
    used_kb=${used_kb:-0}
    [[ "$total_kb" == "0" ]] && total_kb=1 # Avoid division by zero

    total_gb=$(awk "BEGIN {printf \"%.2f\", $total_kb / (1024*1024)}" 2> /dev/null || echo "0")
    used_gb=$(awk "BEGIN {printf \"%.2f\", $used_kb / (1024*1024)}" 2> /dev/null || echo "0")
    used_percent=$(awk "BEGIN {printf \"%.1f\", ($used_kb / $total_kb) * 100}" 2> /dev/null || echo "0")

    [[ -z "$total_gb" || "$total_gb" == "" ]] && total_gb="0"
    [[ -z "$used_gb" || "$used_gb" == "" ]] && used_gb="0"
    [[ -z "$used_percent" || "$used_percent" == "" ]] && used_percent="0"

    echo "$used_gb $total_gb $used_percent"
}

# Get uptime in days
get_uptime_days() {
    local boot_output boot_time uptime_days

    boot_output=$(sysctl -n kern.boottime 2> /dev/null || echo "")
    boot_time=$(echo "$boot_output" | sed -n 's/.*sec = \([0-9]*\).*/\1/p' 2> /dev/null || echo "")

    if [[ -n "$boot_time" && "$boot_time" =~ ^[0-9]+$ ]]; then
        local now=$(date +%s 2> /dev/null || echo "0")
        local uptime_sec=$((now - boot_time))
        uptime_days=$(awk "BEGIN {printf \"%.1f\", $uptime_sec / 86400}" 2> /dev/null || echo "0")
    else
        uptime_days="0"
    fi

    [[ -z "$uptime_days" || "$uptime_days" == "" ]] && uptime_days="0"
    echo "$uptime_days"
}

# Get directory size in KB
dir_size_kb() {
    local path="$1"
    [[ ! -e "$path" ]] && echo "0" && return
    du -sk "$path" 2> /dev/null | awk '{print $1}' || echo "0"
}

# Format size from KB
format_size_kb() {
    local kb="$1"
    [[ "$kb" -le 0 ]] && echo "0B" && return

    local mb gb
    mb=$(awk "BEGIN {printf \"%.1f\", $kb / 1024}")
    gb=$(awk "BEGIN {printf \"%.2f\", $mb / 1024}")

    if awk "BEGIN {exit !($gb >= 1)}"; then
        echo "${gb}GB"
    elif awk "BEGIN {exit !($mb >= 1)}"; then
        printf "%.0fMB\n" "$mb"
    else
        echo "${kb}KB"
    fi
}

# Check cache size
check_cache_refresh() {
    local cache_dir="$HOME/Library/Caches"
    local size_kb=$(dir_size_kb "$cache_dir")
    local desc="Refresh Finder previews, Quick Look, and Safari caches"

    if [[ $size_kb -gt 0 ]]; then
        local size_str=$(format_size_kb "$size_kb")
        desc="Refresh ${size_str} of Finder/Safari caches"
    fi

    echo "cache_refresh|User Cache Refresh|${desc}|true"
}

# Check Mail downloads
check_mail_downloads() {
    local dirs=(
        "$HOME/Library/Mail Downloads"
        "$HOME/Library/Containers/com.apple.mail/Data/Library/Mail Downloads"
    )

    local total_kb=0
    for dir in "${dirs[@]}"; do
        total_kb=$((total_kb + $(dir_size_kb "$dir")))
    done

    if [[ $total_kb -gt 0 ]]; then
        local size_str=$(format_size_kb "$total_kb")
        echo "mail_downloads|Mail Downloads|Recover ${size_str} of Mail attachments|true"
    fi
}

# Check saved state
check_saved_state() {
    local state_dir="$HOME/Library/Saved Application State"
    local size_kb=$(dir_size_kb "$state_dir")

    if [[ $size_kb -gt 0 ]]; then
        local size_str=$(format_size_kb "$size_kb")
        echo "saved_state_cleanup|Saved State|Clear ${size_str} of stale saved states|true"
    fi
}

# Check swap files
check_swap_cleanup() {
    local total_kb=0
    local file

    for file in /private/var/vm/swapfile*; do
        [[ -f "$file" ]] && total_kb=$((total_kb + $(get_file_size "$file") / 1024))
    done

    if [[ $total_kb -gt 0 ]]; then
        local size_str=$(format_size_kb "$total_kb")
        echo "swap_cleanup|Memory & Swap|Purge swap (${size_str}) & inactive memory|false"
    fi
}

# Check local snapshots
check_local_snapshots() {
    command -v tmutil > /dev/null 2>&1 || return

    local snapshots
    snapshots=$(tmutil listlocalsnapshots / 2> /dev/null || echo "")

    local count
    count=$(echo "$snapshots" | grep -c "com.apple.TimeMachine" 2> /dev/null)
    count=$(echo "$count" | tr -d ' \n')
    count=${count:-0}
    [[ "$count" =~ ^[0-9]+$ ]] && [[ $count -gt 0 ]] && echo "local_snapshots|Local Snapshots|${count} APFS local snapshots detected|true"
}

# Check developer cleanup
check_developer_cleanup() {
    local dirs=(
        "$HOME/Library/Developer/Xcode/DerivedData"
        "$HOME/Library/Developer/Xcode/Archives"
        "$HOME/Library/Developer/Xcode/iOS DeviceSupport"
        "$HOME/Library/Developer/CoreSimulator/Caches"
    )

    local total_kb=0
    for dir in "${dirs[@]}"; do
        total_kb=$((total_kb + $(dir_size_kb "$dir")))
    done

    if [[ $total_kb -gt 0 ]]; then
        local size_str=$(format_size_kb "$total_kb")
        echo "developer_cleanup|Developer Cleanup|Recover ${size_str} of Xcode/simulator data|false"
    fi
}

# Generate JSON output
generate_health_json() {
    # System info
    read -r mem_used mem_total <<< "$(get_memory_info)"
    read -r disk_used disk_total disk_percent <<< "$(get_disk_info)"
    local uptime=$(get_uptime_days)

    # Ensure all values are valid numbers (fallback to 0)
    mem_used=${mem_used:-0}
    mem_total=${mem_total:-0}
    disk_used=${disk_used:-0}
    disk_total=${disk_total:-0}
    disk_percent=${disk_percent:-0}
    uptime=${uptime:-0}

    # Start JSON
    cat << EOF
{
  "memory_used_gb": $mem_used,
  "memory_total_gb": $mem_total,
  "disk_used_gb": $disk_used,
  "disk_total_gb": $disk_total,
  "disk_used_percent": $disk_percent,
  "uptime_days": $uptime,
  "optimizations": [
EOF

    # Collect all optimization items
    local -a items=()

    # Always-on items
    items+=('system_maintenance|System Maintenance|Rebuild system databases & flush caches|true')
    items+=('maintenance_scripts|Maintenance Scripts|Run daily/weekly/monthly scripts & rotate logs|true')
    items+=('radio_refresh|Bluetooth & Wi-Fi Refresh|Reset wireless preference caches|true')
    items+=('recent_items|Recent Items|Clear recent apps/documents/servers lists|true')
    items+=('log_cleanup|Diagnostics Cleanup|Purge old diagnostic & crash logs|true')
    items+=('startup_cache|Startup Cache Rebuild|Rebuild kext caches & prelinked kernel|true')

    # Conditional items
    local item
    item=$(check_cache_refresh || true)
    [[ -n "$item" ]] && items+=("$item")
    item=$(check_mail_downloads || true)
    [[ -n "$item" ]] && items+=("$item")
    item=$(check_saved_state || true)
    [[ -n "$item" ]] && items+=("$item")
    item=$(check_swap_cleanup || true)
    [[ -n "$item" ]] && items+=("$item")
    item=$(check_local_snapshots || true)
    [[ -n "$item" ]] && items+=("$item")
    item=$(check_developer_cleanup || true)
    [[ -n "$item" ]] && items+=("$item")

    # Output items as JSON
    local first=true
    for item in "${items[@]}"; do
        IFS='|' read -r action name desc safe <<< "$item"

        [[ "$first" == "true" ]] && first=false || echo ","

        cat << EOF
    {
      "category": "system",
      "name": "$name",
      "description": "$desc",
      "action": "$action",
      "safe": $safe
    }
EOF
    done

    # Close JSON
    cat << 'EOF'
  ]
}
EOF
}

# Main execution
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
    generate_health_json
fi
