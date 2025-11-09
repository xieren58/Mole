#!/bin/bash
# Create Raycast script commands and Alfred keywords for Mole (clean + uninstall).

set -euo pipefail

BLUE='\033[0;34m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

ICON_STEP="➜"
ICON_SUCCESS="✓"
ICON_WARN="!"
ICON_ERR="✗"

log_step() { echo -e "${BLUE}${ICON_STEP}${NC} $1"; }
log_success() { echo -e "${GREEN}${ICON_SUCCESS}${NC} $1"; }
log_warn() { echo -e "${YELLOW}${ICON_WARN}${NC} $1"; }
log_error() { echo -e "${RED}${ICON_ERR}${NC} $1"; }

detect_mo() {
    if command -v mo >/dev/null 2>&1; then
        command -v mo
    elif command -v mole >/dev/null 2>&1; then
        command -v mole
    else
        log_error "Mole not found. Install it first via Homebrew or ./install.sh."
        exit 1
    fi
}

write_raycast_script() {
    local target="$1"
    local title="$2"
    local mo_bin="$3"
    local subcommand="$4"
    cat > "$target" <<EOF
#!/bin/bash

# Required parameters:
# @raycast.schemaVersion 1
# @raycast.title ${title}
# @raycast.mode fullOutput
# @raycast.packageName Mole

# Optional parameters:
# @raycast.icon 🐹

set -euo pipefail

echo "🐹 Running ${title}..."
echo ""
"${mo_bin}" ${subcommand}
EOF
    chmod +x "$target"
}

create_raycast_commands() {
    local mo_bin="$1"
    local default_dir="$HOME/Library/Application Support/Raycast/script-commands"
    local alt_dir="$HOME/Documents/Raycast/Scripts"
    local dirs=()

    if [[ -d "$default_dir" ]]; then
        dirs+=("$default_dir")
    fi
    if [[ -d "$alt_dir" ]]; then
        dirs+=("$alt_dir")
    fi
    if [[ ${#dirs[@]} -eq 0 ]]; then
        dirs+=("$default_dir")
    fi

    log_step "Installing Raycast commands..."
    for dir in "${dirs[@]}"; do
        mkdir -p "$dir"
        write_raycast_script "$dir/mole-clean.sh" "Clean Mac" "$mo_bin" "clean"
        write_raycast_script "$dir/mole-uninstall.sh" "Uninstall Apps" "$mo_bin" "uninstall"
        log_success "Scripts ready in: $dir"
    done

    if open "raycast://extensions/script-commands" > /dev/null 2>&1; then
        log_step "Raycast settings opened. Run “Reload Script Directories”."
    else
        log_warn "Could not auto-open Raycast. Open it manually to reload scripts."
    fi
}

uuid() {
    if command -v uuidgen >/dev/null 2>&1; then
        uuidgen
    else
        # Fallback pseudo UUID
        openssl rand -hex 16 | sed 's/\(..\)/\1/g' | cut -c1-32
    fi
}

create_alfred_workflow() {
    local mo_bin="$1"
    local prefs_dir="${ALFRED_PREFS_DIR:-$HOME/Library/Application Support/Alfred/Alfred.alfredpreferences}"
    local workflows_dir="$prefs_dir/workflows"

    if [[ ! -d "$workflows_dir" ]]; then
        log_warn "Alfred preferences not found at $workflows_dir. Skipping Alfred workflow."
        return
    fi

    log_step "Installing Alfred workflows..."
    local workflows=(
        "fun.tw93.mole.clean|Mole Clean|mclean|Run Mole clean|\"${mo_bin}\" clean"
        "fun.tw93.mole.uninstall|Mole Uninstall|muninstall|Uninstall apps via Mole|\"${mo_bin}\" uninstall"
    )

    for entry in "${workflows[@]}"; do
        IFS="|" read -r bundle name keyword subtitle command <<< "$entry"
        local workflow_uid="user.workflow.$(uuid | tr '[:upper:]' '[:lower:]')"
        local input_uid
        local action_uid
        input_uid="$(uuid)"
        action_uid="$(uuid)"
        local dir="$workflows_dir/$workflow_uid"
        mkdir -p "$dir"

        cat > "$dir/info.plist" <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>bundleid</key>
    <string>${bundle}</string>
    <key>createdby</key>
    <string>Mole</string>
    <key>name</key>
    <string>${name}</string>
    <key>objects</key>
    <array>
        <dict>
            <key>config</key>
            <dict>
                <key>argumenttype</key>
                <integer>2</integer>
                <key>keyword</key>
                <string>${keyword}</string>
                <key>subtext</key>
                <string>${subtitle}</string>
                <key>text</key>
                <string>${name}</string>
                <key>withspace</key>
                <true/>
            </dict>
            <key>type</key>
            <string>alfred.workflow.input.keyword</string>
            <key>uid</key>
            <string>${input_uid}</string>
            <key>version</key>
            <integer>1</integer>
        </dict>
        <dict>
            <key>config</key>
            <dict>
                <key>concurrently</key>
                <true/>
                <key>escaping</key>
                <integer>102</integer>
                <key>script</key>
                <string>#!/bin/bash
PATH="/usr/local/bin:/opt/homebrew/bin:/usr/bin:/bin"
${command}
</string>
                <key>scriptargtype</key>
                <integer>1</integer>
                <key>scriptfile</key>
                <string></string>
                <key>type</key>
                <integer>0</integer>
            </dict>
            <key>type</key>
            <string>alfred.workflow.action.script</string>
            <key>uid</key>
            <string>${action_uid}</string>
            <key>version</key>
            <integer>2</integer>
        </dict>
    </array>
    <key>connections</key>
    <dict>
        <key>${input_uid}</key>
        <array>
            <dict>
                <key>destinationuid</key>
                <string>${action_uid}</string>
                <key>modifiers</key>
                <integer>0</integer>
                <key>modifiersubtext</key>
                <string></string>
            </dict>
        </array>
    </dict>
    <key>uid</key>
    <string>${workflow_uid}</string>
    <key>version</key>
    <integer>1</integer>
</dict>
</plist>
EOF
        log_success "Workflow ready: ${name} (keyword: ${keyword})"
    done

    log_step "Open Alfred preferences → Workflows if you need to adjust keywords."
}

main() {
    echo ""
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo "  Mole Quick Launchers"
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

    local mo_bin
    mo_bin="$(detect_mo)"
    log_step "Detected Mole binary at: ${mo_bin}"

    create_raycast_commands "$mo_bin"
    create_alfred_workflow "$mo_bin"

    echo ""
    log_success "Done! Raycast (search “Clean Mac”) and Alfred (keywords: mclean / muninstall) are ready."
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo ""
}

main "$@"
