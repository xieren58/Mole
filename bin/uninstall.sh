#!/bin/bash
# Mole - Uninstall Module
# Interactive application uninstaller with keyboard navigation
#
# Usage:
#   uninstall.sh          # Launch interactive uninstaller
#   uninstall.sh --help   # Show help information

set -euo pipefail

# Fix locale issues (avoid Perl warnings on non-English systems)
export LC_ALL=C
export LANG=C

# Get script directory and source common functions
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/../lib/common.sh"
source "$SCRIPT_DIR/../lib/menu_paginated.sh"
source "$SCRIPT_DIR/../lib/ui_app_selector.sh"
source "$SCRIPT_DIR/../lib/uninstall_batch.sh"

# Note: Bundle preservation logic is now in lib/common.sh

# Initialize global variables
selected_apps=() # Global array for app selection
declare -a apps_data=()
declare -a selection_state=()
total_items=0
files_cleaned=0
total_size_cleaned=0

# Get app last used date in human readable format
get_app_last_used() {
    local app_path="$1"
    local last_used
    last_used=$(mdls -name kMDItemLastUsedDate -raw "$app_path" 2> /dev/null)

    if [[ "$last_used" == "(null)" || -z "$last_used" ]]; then
        echo "Never"
    else
        local last_used_epoch
        last_used_epoch=$(date -j -f "%Y-%m-%d %H:%M:%S %z" "$last_used" "+%s" 2> /dev/null)
        local current_epoch
        current_epoch=$(date "+%s")
        local days_ago=$(((current_epoch - last_used_epoch) / 86400))

        if [[ $days_ago -eq 0 ]]; then
            echo "Today"
        elif [[ $days_ago -eq 1 ]]; then
            echo "Yesterday"
        elif [[ $days_ago -lt 30 ]]; then
            echo "${days_ago} days ago"
        elif [[ $days_ago -lt 365 ]]; then
            local months_ago=$((days_ago / 30))
            echo "${months_ago} month(s) ago"
        else
            local years_ago=$((days_ago / 365))
            echo "${years_ago} year(s) ago"
        fi
    fi
}

# Compact the "last used" descriptor for aligned summaries
format_last_used_summary() {
    local value="$1"

    case "$value" in
        "" | "Unknown")
            echo "Unknown"
            return 0
            ;;
        "Never" | "Recent" | "Today" | "Yesterday" | "This year" | "Old")
            echo "$value"
            return 0
            ;;
    esac

    if [[ $value =~ ^([0-9]+)[[:space:]]+days?\ ago$ ]]; then
        echo "${BASH_REMATCH[1]}d ago"
        return 0
    fi
    if [[ $value =~ ^([0-9]+)[[:space:]]+weeks?\ ago$ ]]; then
        echo "${BASH_REMATCH[1]}w ago"
        return 0
    fi
    if [[ $value =~ ^([0-9]+)[[:space:]]+months?\ ago$ ]]; then
        echo "${BASH_REMATCH[1]}m ago"
        return 0
    fi
    if [[ $value =~ ^([0-9]+)[[:space:]]+month\(s\)\ ago$ ]]; then
        echo "${BASH_REMATCH[1]}m ago"
        return 0
    fi
    if [[ $value =~ ^([0-9]+)[[:space:]]+years?\ ago$ ]]; then
        echo "${BASH_REMATCH[1]}y ago"
        return 0
    fi
    echo "$value"
}

# Scan applications and collect information
scan_applications() {
    # Simplified cache: only check timestamp (24h TTL)
    local cache_dir="$HOME/.cache/mole"
    local cache_file="$cache_dir/app_scan_cache"
    local cache_ttl=86400 # 24 hours

    mkdir -p "$cache_dir" 2> /dev/null

    # Check if cache exists and is fresh
    if [[ -f "$cache_file" ]]; then
        local cache_age=$(($(date +%s) - $(get_file_mtime "$cache_file")))
        [[ $cache_age -eq $(date +%s) ]] && cache_age=86401  # Handle missing file
        if [[ $cache_age -lt $cache_ttl ]]; then
            # Cache hit - return immediately
            echo "$cache_file"
            return 0
        fi
    fi

    # Cache miss - show scanning feedback below

    local temp_file
    temp_file=$(create_temp_file)

    # Pre-cache current epoch to avoid repeated calls
    local current_epoch
    current_epoch=$(date "+%s")

    # Spinner for scanning feedback (simple ASCII for compatibility)
    local spinner_chars="|/-\\"
    local spinner_idx=0

    # First pass: quickly collect all valid app paths and bundle IDs
    local -a app_data_tuples=()
    while IFS= read -r -d '' app_path; do
        if [[ ! -e "$app_path" ]]; then continue; fi

        local app_name
        app_name=$(basename "$app_path" .app)

        # Try to get English name from bundle info, fallback to folder name
        local bundle_id="unknown"
        local display_name="$app_name"
        if [[ -f "$app_path/Contents/Info.plist" ]]; then
            bundle_id=$(defaults read "$app_path/Contents/Info.plist" CFBundleIdentifier 2> /dev/null || echo "unknown")

            # Try to get English name from bundle info
            local bundle_executable
            bundle_executable=$(defaults read "$app_path/Contents/Info.plist" CFBundleExecutable 2> /dev/null)

            # Smart display name selection - prefer descriptive names over generic ones
            local candidates=()

            # Get all potential names
            local bundle_display_name
            bundle_display_name=$(plutil -extract CFBundleDisplayName raw "$app_path/Contents/Info.plist" 2> /dev/null)
            local bundle_name
            bundle_name=$(plutil -extract CFBundleName raw "$app_path/Contents/Info.plist" 2> /dev/null)

            # Check if executable name is generic/technical (should be avoided)
            local is_generic_executable=false
            if [[ -n "$bundle_executable" ]]; then
                case "$bundle_executable" in
                    "pake" | "Electron" | "electron" | "nwjs" | "node" | "helper" | "main" | "app" | "binary")
                        is_generic_executable=true
                        ;;
                esac
            fi

            # Priority order for name selection:
            # 1. App folder name (if ASCII and descriptive) - often the most complete name
            if [[ "$app_name" =~ ^[A-Za-z0-9\ ._-]+$ && ${#app_name} -gt 3 ]]; then
                candidates+=("$app_name")
            fi

            # 2. CFBundleDisplayName (if meaningful and ASCII)
            if [[ -n "$bundle_display_name" && "$bundle_display_name" =~ ^[A-Za-z0-9\ ._-]+$ ]]; then
                candidates+=("$bundle_display_name")
            fi

            # 3. CFBundleName (if meaningful and ASCII)
            if [[ -n "$bundle_name" && "$bundle_name" =~ ^[A-Za-z0-9\ ._-]+$ && "$bundle_name" != "$bundle_display_name" ]]; then
                candidates+=("$bundle_name")
            fi

            # 4. CFBundleExecutable (only if not generic and ASCII)
            if [[ -n "$bundle_executable" && "$bundle_executable" =~ ^[A-Za-z0-9._-]+$ && "$is_generic_executable" == false ]]; then
                candidates+=("$bundle_executable")
            fi

            # 5. Fallback to non-ASCII names if no ASCII found
            if [[ ${#candidates[@]} -eq 0 ]]; then
                [[ -n "$bundle_display_name" ]] && candidates+=("$bundle_display_name")
                [[ -n "$bundle_name" && "$bundle_name" != "$bundle_display_name" ]] && candidates+=("$bundle_name")
                candidates+=("$app_name")
            fi

            # Select the first (best) candidate
            display_name="${candidates[0]:-$app_name}"

            # Apply brand name mapping from common.sh
            display_name="$(get_brand_name "$display_name")"
        fi

        # Skip system critical apps (input methods, system components)
        # Note: Paid apps like CleanMyMac, 1Password are NOT protected here - users can uninstall them
        if should_protect_from_uninstall "$bundle_id"; then
            continue
        fi

        # Store tuple: app_path|app_name|bundle_id|display_name
        app_data_tuples+=("${app_path}|${app_name}|${bundle_id}|${display_name}")
    done < <(
        # Scan both system and user application directories
        find /Applications -name "*.app" -maxdepth 1 -print0 2> /dev/null
        find ~/Applications -name "*.app" -maxdepth 1 -print0 2> /dev/null
    )

    # Second pass: process each app with parallel size calculation
    local app_count=0
    local total_apps=${#app_data_tuples[@]}
    # Bound parallelism so small machines stay responsive
    local max_parallel
    max_parallel=$(get_optimal_parallel_jobs "io")
    if [[ $max_parallel -lt 4 ]]; then
        max_parallel=4
    elif [[ $max_parallel -gt 16 ]]; then
        max_parallel=16
    fi
    local pids=()
    local inline_loading=false
    if [[ "${MOLE_INLINE_LOADING:-}" == "1" || "${MOLE_INLINE_LOADING:-}" == "true" ]]; then
        inline_loading=true
        printf "\033[H" >&2 # Position cursor at top of screen
    fi

    # Process app metadata extraction function
    process_app_metadata() {
        local app_data_tuple="$1"
        local output_file="$2"
        local current_epoch="$3"

        IFS='|' read -r app_path app_name bundle_id display_name <<< "$app_data_tuple"

        # Parallel size calculation
        local app_size="N/A"
        local app_size_kb="0"
        if [[ -d "$app_path" ]]; then
            # Get size in KB, then format for display (single du call)
            app_size_kb=$(du -sk "$app_path" 2> /dev/null | awk '{print $1}' || echo "0")
            app_size=$(bytes_to_human "$((app_size_kb * 1024))")
        fi

        # Get real last used date from macOS metadata
        local last_used="Never"
        local last_used_epoch=0

        if [[ -d "$app_path" ]]; then
            local metadata_date
            metadata_date=$(mdls -name kMDItemLastUsedDate -raw "$app_path" 2> /dev/null)

            if [[ "$metadata_date" != "(null)" && -n "$metadata_date" ]]; then
                last_used_epoch=$(date -j -f "%Y-%m-%d %H:%M:%S %z" "$metadata_date" "+%s" 2> /dev/null || echo "0")

                if [[ $last_used_epoch -gt 0 ]]; then
                    local days_ago=$(((current_epoch - last_used_epoch) / 86400))

                    if [[ $days_ago -eq 0 ]]; then
                        last_used="Today"
                    elif [[ $days_ago -eq 1 ]]; then
                        last_used="Yesterday"
                    elif [[ $days_ago -lt 7 ]]; then
                        last_used="${days_ago} days ago"
                    elif [[ $days_ago -lt 30 ]]; then
                        local weeks_ago=$((days_ago / 7))
                        [[ $weeks_ago -eq 1 ]] && last_used="1 week ago" || last_used="${weeks_ago} weeks ago"
                    elif [[ $days_ago -lt 365 ]]; then
                        local months_ago=$((days_ago / 30))
                        [[ $months_ago -eq 1 ]] && last_used="1 month ago" || last_used="${months_ago} months ago"
                    else
                        local years_ago=$((days_ago / 365))
                        [[ $years_ago -eq 1 ]] && last_used="1 year ago" || last_used="${years_ago} years ago"
                    fi
                fi
            else
                # Fallback to file modification time
                last_used_epoch=$(get_file_mtime "$app_path")
                if [[ $last_used_epoch -gt 0 ]]; then
                    local days_ago=$(((current_epoch - last_used_epoch) / 86400))
                    if [[ $days_ago -lt 30 ]]; then
                        last_used="Recent"
                    elif [[ $days_ago -lt 365 ]]; then
                        last_used="This year"
                    else
                        last_used="Old"
                    fi
                fi
            fi
        fi

        # Write to output file atomically
        # Fields: epoch|app_path|display_name|bundle_id|size_human|last_used|size_kb
        echo "${last_used_epoch}|${app_path}|${display_name}|${bundle_id}|${app_size}|${last_used}|${app_size_kb}" >> "$output_file"
    }

    export -f process_app_metadata

    # Process apps in parallel batches
    for app_data_tuple in "${app_data_tuples[@]}"; do
        ((app_count++))

        # Launch background process
        process_app_metadata "$app_data_tuple" "$temp_file" "$current_epoch" &
        pids+=($!)

        # Update progress with spinner
        local spinner_char="${spinner_chars:$((spinner_idx % 4)):1}"
        if [[ $inline_loading == true ]]; then
            printf "\033[H\033[2K${spinner_char} Scanning applications... %d/%d" "$app_count" "$total_apps" >&2
        else
            echo -ne "\r\033[K${spinner_char} Scanning applications... $app_count/$total_apps" >&2
        fi
        ((spinner_idx++))

        # Wait if we've hit max parallel limit
        if ((${#pids[@]} >= max_parallel)); then
            wait "${pids[0]}" 2> /dev/null
            pids=("${pids[@]:1}") # Remove first pid
        fi
    done

    # Wait for remaining background processes
    for pid in "${pids[@]}"; do
        wait "$pid" 2> /dev/null
    done

    # Check if we found any applications
    if [[ ! -s "$temp_file" ]]; then
        if [[ $inline_loading == true ]]; then
            printf "\033[H\033[2K" >&2
        else
            echo -ne "\r\033[K" >&2
        fi
        echo "No applications found to uninstall" >&2
        rm -f "$temp_file"
        return 1
    fi

    if [[ $inline_loading == true ]]; then
        printf "\033[H\033[2K" >&2
    fi

    # Sort by last used (oldest first) and cache the result
    sort -t'|' -k1,1n "$temp_file" > "${temp_file}.sorted" || {
        rm -f "$temp_file"
        return 1
    }
    rm -f "$temp_file"

    # Save to cache (simplified - no metadata)
    cp "${temp_file}.sorted" "$cache_file" 2> /dev/null || true

    # Return sorted file
    if [[ -f "${temp_file}.sorted" ]]; then
        echo "${temp_file}.sorted"
    else
        return 1
    fi
}

# Load applications into arrays
load_applications() {
    local apps_file="$1"

    if [[ ! -f "$apps_file" || ! -s "$apps_file" ]]; then
        log_warning "No applications found for uninstallation"
        return 1
    fi

    # Clear arrays
    apps_data=()
    selection_state=()

    # Read apps into array, skip non-existent apps
    while IFS='|' read -r epoch app_path app_name bundle_id size last_used size_kb; do
        # Skip if app path no longer exists
        [[ ! -e "$app_path" ]] && continue

        apps_data+=("$epoch|$app_path|$app_name|$bundle_id|$size|$last_used|${size_kb:-0}")
        selection_state+=(false)
    done < "$apps_file"

    if [[ ${#apps_data[@]} -eq 0 ]]; then
        log_warning "No applications available for uninstallation"
        return 1
    fi

    return 0
}

# Old display_apps function removed - replaced by new menu system

# Read a single key with proper escape sequence handling
# This function has been replaced by the menu.sh library

# Note: App file discovery and size calculation functions moved to lib/common.sh
# Use find_app_files() and calculate_total_size() from common.sh

# Uninstall selected applications
uninstall_applications() {
    local total_size_freed=0

    echo ""
    echo -e "${PURPLE}${ICON_ARROW} Uninstalling selected applications${NC}"

    if [[ ${#selected_apps[@]} -eq 0 ]]; then
        log_warning "No applications selected for uninstallation"
        return 0
    fi

    for selected_app in "${selected_apps[@]}"; do
        IFS='|' read -r epoch app_path app_name bundle_id size last_used <<< "$selected_app"

        echo ""

        # Check if app is running (use app path for precise matching)
        if pgrep -f "$app_path" > /dev/null 2>&1; then
            echo -e "${YELLOW}${ICON_ERROR} $app_name is currently running${NC}"
            read -p "  Force quit $app_name? (y/N): " -n 1 -r
            echo
            if [[ $REPLY =~ ^[Yy]$ ]]; then
                # Retry kill operation with verification to avoid TOCTOU
                local retry=0
                while [[ $retry -lt 3 ]]; do
                    pkill -f "$app_path" 2> /dev/null || true
                    sleep 1
                    # Verify app was killed
                    if ! pgrep -f "$app_path" > /dev/null 2>&1; then
                        break
                    fi
                    ((retry++))
                done

                # Final check
                if pgrep -f "$app_path" > /dev/null 2>&1; then
                    log_warning "Failed to quit $app_name after $retry attempts"
                fi
            else
                echo -e "  ${BLUE}${ICON_EMPTY}${NC} Skipped $app_name"
                continue
            fi
        fi

        # Find related files (user-level)
        local related_files
        related_files=$(find_app_files "$bundle_id" "$app_name")

        # Find system-level files (requires sudo)
        local system_files
        system_files=$(find_app_system_files "$bundle_id" "$app_name")

        # Calculate total size
        local app_size_kb
        app_size_kb=$(du -sk "$app_path" 2> /dev/null | awk '{print $1}' || echo "0")
        local related_size_kb
        related_size_kb=$(calculate_total_size "$related_files")
        local system_size_kb
        system_size_kb=$(calculate_total_size "$system_files")
        local total_kb=$((app_size_kb + related_size_kb + system_size_kb))

        # Show what will be removed
        echo -e "${BLUE}${ICON_CONFIRM}${NC} $app_name - Files to be removed:"
        echo -e "  ${GREEN}${ICON_SUCCESS}${NC} Application: $(echo "$app_path" | sed "s|$HOME|~|")"

        # Show user-level files
        while IFS= read -r file; do
            [[ -n "$file" && -e "$file" ]] && echo -e "  ${GREEN}${ICON_SUCCESS}${NC} $(echo "$file" | sed "s|$HOME|~|")"
        done <<< "$related_files"

        # Show system-level files
        if [[ -n "$system_files" ]]; then
            while IFS= read -r file; do
                [[ -n "$file" && -e "$file" ]] && echo -e "  ${BLUE}${ICON_SOLID}${NC} System: $file"
            done <<< "$system_files"
        fi

        local size_display
        if [[ $total_kb -gt 1048576 ]]; then # > 1GB
            size_display=$(echo "$total_kb" | awk '{printf "%.2fGB", $1/1024/1024}')
        elif [[ $total_kb -gt 1024 ]]; then # > 1MB
            size_display=$(echo "$total_kb" | awk '{printf "%.1fMB", $1/1024}')
        else
            size_display="${total_kb}KB"
        fi

        echo -e "  ${BLUE}Total size: $size_display${NC}"
        echo

        read -p "  Proceed with uninstalling $app_name? (y/N): " -n 1 -r
        echo

        if [[ $REPLY =~ ^[Yy]$ ]]; then
            # Stop Launch Agents and Daemons before removal
            # User-level Launch Agents
            for plist in ~/Library/LaunchAgents/"$bundle_id"*.plist; do
                if [[ -f "$plist" ]]; then
                    launchctl unload "$plist" 2> /dev/null || true
                fi
            done
            # System-level Launch Agents
            for plist in /Library/LaunchAgents/"$bundle_id"*.plist; do
                if [[ -f "$plist" ]]; then
                    sudo launchctl unload "$plist" 2> /dev/null || true
                fi
            done
            # System-level Launch Daemons
            for plist in /Library/LaunchDaemons/"$bundle_id"*.plist; do
                if [[ -f "$plist" ]]; then
                    sudo launchctl unload "$plist" 2> /dev/null || true
                fi
            done

            # Remove the application
            if rm -rf "$app_path" 2> /dev/null; then
                echo -e "  ${GREEN}${ICON_SUCCESS}${NC} Removed application"
            else
                echo -e "  ${RED}${ICON_ERROR}${NC} Failed to remove $app_path"
                continue
            fi

            # Remove user-level related files
            while IFS= read -r file; do
                if [[ -n "$file" && -e "$file" ]]; then
                    # Handle symbolic links separately (only remove the link, not the target)
                    if [[ -L "$file" ]]; then
                        if rm "$file" 2> /dev/null; then
                            echo -e "  ${GREEN}${ICON_SUCCESS}${NC} Removed $(echo "$file" | sed "s|$HOME|~|" | xargs basename)"
                        fi
                    else
                        if rm -rf "$file" 2> /dev/null; then
                            echo -e "  ${GREEN}${ICON_SUCCESS}${NC} Removed $(echo "$file" | sed "s|$HOME|~|" | xargs basename)"
                        fi
                    fi
                fi
            done <<< "$related_files"

            # Remove system-level files (requires sudo)
            if [[ -n "$system_files" ]]; then
                echo -e "  ${BLUE}${ICON_SOLID}${NC} Admin access required for system files"
                while IFS= read -r file; do
                    if [[ -n "$file" && -e "$file" ]]; then
                        # Handle symbolic links separately (only remove the link, not the target)
                        if [[ -L "$file" ]]; then
                            if sudo rm "$file" 2> /dev/null; then
                                echo -e "  ${GREEN}${ICON_SUCCESS}${NC} Removed $(basename "$file")"
                            else
                                echo -e "  ${YELLOW}${ICON_ERROR}${NC} Failed to remove: $file"
                            fi
                        else
                            if sudo rm -rf "$file" 2> /dev/null; then
                                echo -e "  ${GREEN}${ICON_SUCCESS}${NC} Removed $(basename "$file")"
                            else
                                echo -e "  ${YELLOW}${ICON_ERROR}${NC} Failed to remove: $file"
                            fi
                        fi
                    fi
                done <<< "$system_files"
            fi

            ((total_size_freed += total_kb))
            ((files_cleaned++))
            ((total_items++))

            echo -e "  ${GREEN}${ICON_SUCCESS}${NC} $app_name uninstalled successfully"
        else
            echo -e "  ${BLUE}${ICON_EMPTY}${NC} Skipped $app_name"
        fi
    done

    # Show final summary
    echo -e "${PURPLE}${ICON_ARROW} Uninstallation Summary${NC}"

    if [[ $total_size_freed -gt 0 ]]; then
        local freed_display
        if [[ $total_size_freed -gt 1048576 ]]; then # > 1GB
            freed_display=$(echo "$total_size_freed" | awk '{printf "%.2fGB", $1/1024/1024}')
        elif [[ $total_size_freed -gt 1024 ]]; then # > 1MB
            freed_display=$(echo "$total_size_freed" | awk '{printf "%.1fMB", $1/1024}')
        else
            freed_display="${total_size_freed}KB"
        fi

        echo -e "  ${GREEN}${ICON_SUCCESS}${NC} Freed $freed_display of disk space"
    fi

    echo -e "  ${GREEN}${ICON_SUCCESS}${NC} Applications uninstalled: $files_cleaned"
    ((total_size_cleaned += total_size_freed))
}

# Cleanup function - restore cursor and clean up
cleanup() {
    # Restore cursor using common function
    if [[ "${MOLE_ALT_SCREEN_ACTIVE:-}" == "1" ]]; then
        leave_alt_screen
        unset MOLE_ALT_SCREEN_ACTIVE
    fi
    if [[ -n "${sudo_keepalive_pid:-}" ]]; then
        kill "$sudo_keepalive_pid" 2> /dev/null || true
        wait "$sudo_keepalive_pid" 2> /dev/null || true
        sudo_keepalive_pid=""
    fi
    show_cursor
    exit "${1:-0}"
}

# Set trap for cleanup on exit
trap cleanup EXIT INT TERM

# Main function
main() {
    local use_inline_loading=false
    if [[ -t 1 && -t 2 ]]; then
        use_inline_loading=true
    fi

    # Hide cursor during operation
    hide_cursor

    # Simplified: always check if we need alt screen for scanning
    # (scan_applications handles cache internally)
    local needs_scanning=true
    local cache_file="$HOME/.cache/mole/app_scan_cache"
    if [[ -f "$cache_file" ]]; then
        local cache_age=$(($(date +%s) - $(get_file_mtime "$cache_file")))
        [[ $cache_age -eq $(date +%s) ]] && cache_age=86401  # Handle missing file
        [[ $cache_age -lt 86400 ]] && needs_scanning=false
    fi

    # Only enter alt screen if we need scanning (shows progress)
    if [[ $needs_scanning == true && $use_inline_loading == true ]]; then
        enter_alt_screen
        export MOLE_ALT_SCREEN_ACTIVE=1
        export MOLE_INLINE_LOADING=1
        export MOLE_MANAGED_ALT_SCREEN=1
        printf "\033[2J\033[H" >&2
    else
        unset MOLE_INLINE_LOADING MOLE_MANAGED_ALT_SCREEN MOLE_ALT_SCREEN_ACTIVE
    fi

    # Scan applications
    local apps_file=""
    if ! apps_file=$(scan_applications); then
        if [[ "${MOLE_ALT_SCREEN_ACTIVE:-}" == "1" ]]; then
            printf "\033[2J\033[H" >&2
            leave_alt_screen
            unset MOLE_ALT_SCREEN_ACTIVE
            unset MOLE_INLINE_LOADING MOLE_MANAGED_ALT_SCREEN
        fi
        return 1
    fi

    if [[ "${MOLE_ALT_SCREEN_ACTIVE:-}" == "1" ]]; then
        printf "\033[2J\033[H" >&2
    fi

    if [[ ! -f "$apps_file" ]]; then
        # Error message already shown by scan_applications
        if [[ "${MOLE_ALT_SCREEN_ACTIVE:-}" == "1" ]]; then
            leave_alt_screen
            unset MOLE_ALT_SCREEN_ACTIVE
            unset MOLE_INLINE_LOADING MOLE_MANAGED_ALT_SCREEN
        fi
        return 1
    fi

    # Load applications
    if ! load_applications "$apps_file"; then
        if [[ "${MOLE_ALT_SCREEN_ACTIVE:-}" == "1" ]]; then
            leave_alt_screen
            unset MOLE_ALT_SCREEN_ACTIVE
            unset MOLE_INLINE_LOADING MOLE_MANAGED_ALT_SCREEN
        fi
        rm -f "$apps_file"
        return 1
    fi

    # Interactive selection using paginated menu
    if ! select_apps_for_uninstall; then
        if [[ "${MOLE_ALT_SCREEN_ACTIVE:-}" == "1" ]]; then
            leave_alt_screen
            unset MOLE_ALT_SCREEN_ACTIVE
            unset MOLE_INLINE_LOADING MOLE_MANAGED_ALT_SCREEN
        fi
        show_cursor
        clear_screen
        printf '\033[2J\033[H' >&2 # Also clear stderr
        rm -f "$apps_file"
        return 0
    fi

    # Always clear on exit from selection, regardless of alt screen state
    if [[ "${MOLE_ALT_SCREEN_ACTIVE:-}" == "1" ]]; then
        leave_alt_screen
        unset MOLE_ALT_SCREEN_ACTIVE
        unset MOLE_INLINE_LOADING MOLE_MANAGED_ALT_SCREEN
    fi

    # Restore cursor and clear screen (output to both stdout and stderr for reliability)
    show_cursor
    clear_screen
    printf '\033[2J\033[H' >&2 # Also clear stderr in case of mixed output
    local selection_count=${#selected_apps[@]}
    if [[ $selection_count -eq 0 ]]; then
        echo "No apps selected"
        rm -f "$apps_file"
        return 0
    fi
    # Show selected apps with clean alignment
    echo -e "${BLUE}${ICON_CONFIRM}${NC} Selected ${selection_count} app(s):"
    local -a summary_rows=()
    local max_name_width=0
    local max_size_width=0
    local name_trunc_limit=30

    for selected_app in "${selected_apps[@]}"; do
        IFS='|' read -r epoch app_path app_name bundle_id size last_used size_kb <<< "$selected_app"

        local display_name="$app_name"
        if [[ ${#display_name} -gt $name_trunc_limit ]]; then
            display_name="${display_name:0:$((name_trunc_limit - 3))}..."
        fi
        [[ ${#display_name} -gt $max_name_width ]] && max_name_width=${#display_name}

        local size_display="$size"
        if [[ -z "$size_display" || "$size_display" == "0" || "$size_display" == "N/A" ]]; then
            size_display="Unknown"
        fi
        [[ ${#size_display} -gt $max_size_width ]] && max_size_width=${#size_display}

        local last_display
        last_display=$(format_last_used_summary "$last_used")

        summary_rows+=("$display_name|$size_display|$last_display")
    done

    ((max_name_width < 16)) && max_name_width=16
    ((max_size_width < 5)) && max_size_width=5

    local index=1
    for row in "${summary_rows[@]}"; do
        IFS='|' read -r name_cell size_cell last_cell <<< "$row"
        printf "%d. %-*s  %*s  |  Last: %s\n" "$index" "$max_name_width" "$name_cell" "$max_size_width" "$size_cell" "$last_cell"
        ((index++))
    done

    # Execute batch uninstallation (handles confirmation)
    batch_uninstall_applications

    # Cleanup
    rm -f "$apps_file"
}

# Run main function
main "$@"
