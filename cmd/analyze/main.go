//go:build darwin

package main

import (
	"context"
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"time"

	tea "github.com/charmbracelet/bubbletea"
)

type dirEntry struct {
	name       string
	path       string
	size       int64
	isDir      bool
	lastAccess time.Time
}

type fileEntry struct {
	name string
	path string
	size int64
}

type scanResult struct {
	entries    []dirEntry
	largeFiles []fileEntry
	totalSize  int64
}

type cacheEntry struct {
	Entries    []dirEntry
	LargeFiles []fileEntry
	TotalSize  int64
	ModTime    time.Time
	ScanTime   time.Time
}

type historyEntry struct {
	path          string
	entries       []dirEntry
	largeFiles    []fileEntry
	totalSize     int64
	selected      int
	entryOffset   int
	largeSelected int
	largeOffset   int
	dirty         bool
}

type scanResultMsg struct {
	result scanResult
	err    error
}

type overviewSizeMsg struct {
	path  string
	index int
	size  int64
	err   error
}

type tickMsg time.Time

type deleteProgressMsg struct {
	done  bool
	err   error
	count int64
}

type model struct {
	path                 string
	history              []historyEntry
	entries              []dirEntry
	largeFiles           []fileEntry
	selected             int
	offset               int
	status               string
	totalSize            int64
	scanning             bool
	spinner              int
	filesScanned         *int64
	dirsScanned          *int64
	bytesScanned         *int64
	currentPath          *string
	showLargeFiles       bool
	isOverview           bool
	deleteConfirm        bool
	deleteTarget         *dirEntry
	deleting             bool
	deleteCount          *int64
	cache                map[string]historyEntry
	largeSelected        int
	largeOffset          int
	overviewSizeCache    map[string]int64
	overviewFilesScanned *int64
	overviewDirsScanned  *int64
	overviewBytesScanned *int64
	overviewCurrentPath  *string
	overviewScanning     bool
	overviewScanningSet  map[string]bool // Track which paths are currently being scanned
}

func main() {
	target := os.Getenv("MO_ANALYZE_PATH")
	if target == "" && len(os.Args) > 1 {
		target = os.Args[1]
	}

	var abs string
	var isOverview bool

	if target == "" {
		// Default to overview mode
		isOverview = true
		abs = "/"
	} else {
		var err error
		abs, err = filepath.Abs(target)
		if err != nil {
			fmt.Fprintf(os.Stderr, "cannot resolve %q: %v\n", target, err)
			os.Exit(1)
		}
		isOverview = false
	}

	p := tea.NewProgram(newModel(abs, isOverview), tea.WithAltScreen())
	if err := p.Start(); err != nil {
		fmt.Fprintf(os.Stderr, "analyzer error: %v\n", err)
		os.Exit(1)
	}
}

func newModel(path string, isOverview bool) model {
	var filesScanned, dirsScanned, bytesScanned int64
	currentPath := ""
	var overviewFilesScanned, overviewDirsScanned, overviewBytesScanned int64
	overviewCurrentPath := ""

	m := model{
		path:                 path,
		selected:             0,
		status:               "Preparing scan...",
		scanning:             !isOverview,
		filesScanned:         &filesScanned,
		dirsScanned:          &dirsScanned,
		bytesScanned:         &bytesScanned,
		currentPath:          &currentPath,
		showLargeFiles:       false,
		isOverview:           isOverview,
		cache:                make(map[string]historyEntry),
		overviewFilesScanned: &overviewFilesScanned,
		overviewDirsScanned:  &overviewDirsScanned,
		overviewBytesScanned: &overviewBytesScanned,
		overviewCurrentPath:  &overviewCurrentPath,
		overviewSizeCache:    make(map[string]int64),
		overviewScanningSet:  make(map[string]bool),
	}

	// In overview mode, create shortcut entries
	if isOverview {
		m.scanning = false
		m.hydrateOverviewEntries()
		m.selected = 0
		m.offset = 0
		if nextPendingOverviewIndex(m.entries) >= 0 {
			m.overviewScanning = true
			m.status = "Estimating system roots..."
		} else {
			m.status = "Ready"
		}
	}

	return m
}

func createOverviewEntries() []dirEntry {
	home := os.Getenv("HOME")
	entries := []dirEntry{}

	if home != "" {
		entries = append(entries,
			dirEntry{name: "Home (~)", path: home, isDir: true, size: -1},
			dirEntry{name: "Library (~/Library)", path: filepath.Join(home, "Library"), isDir: true, size: -1},
		)
	}

	entries = append(entries,
		dirEntry{name: "Applications", path: "/Applications", isDir: true, size: -1},
		dirEntry{name: "System Library", path: "/Library", isDir: true, size: -1},
	)

	// Add Volumes shortcut only when it contains real mounted folders (e.g., external disks)
	if hasUsefulVolumeMounts("/Volumes") {
		entries = append(entries, dirEntry{name: "Volumes", path: "/Volumes", isDir: true, size: -1})
	}

	return entries
}

func hasUsefulVolumeMounts(path string) bool {
	entries, err := os.ReadDir(path)
	if err != nil {
		return false
	}

	for _, entry := range entries {
		name := entry.Name()
		// Skip hidden control entries for Spotlight/TimeMachine etc.
		if strings.HasPrefix(name, ".") {
			continue
		}

		info, err := os.Lstat(filepath.Join(path, name))
		if err != nil {
			continue
		}
		if info.Mode()&fs.ModeSymlink != 0 {
			continue // Ignore the synthetic MacintoshHD link
		}
		if info.IsDir() {
			return true
		}
	}
	return false
}

func (m *model) hydrateOverviewEntries() {
	m.entries = createOverviewEntries()
	if m.overviewSizeCache == nil {
		m.overviewSizeCache = make(map[string]int64)
	}
	for i := range m.entries {
		if size, ok := m.overviewSizeCache[m.entries[i].path]; ok {
			m.entries[i].size = size
			continue
		}
		if size, err := loadOverviewCachedSize(m.entries[i].path); err == nil {
			m.entries[i].size = size
			m.overviewSizeCache[m.entries[i].path] = size
		}
	}
	m.totalSize = sumKnownEntrySizes(m.entries)
}

func (m *model) scheduleOverviewScans() tea.Cmd {
	if !m.isOverview {
		return nil
	}

	// Find pending entries (not scanned and not currently scanning)
	var pendingIndices []int
	for i, entry := range m.entries {
		if entry.size < 0 && !m.overviewScanningSet[entry.path] {
			pendingIndices = append(pendingIndices, i)
			if len(pendingIndices) >= maxConcurrentOverview {
				break
			}
		}
	}

	// No more work to do
	if len(pendingIndices) == 0 {
		m.overviewScanning = false
		if !hasPendingOverviewEntries(m.entries) {
			m.status = "Ready"
		}
		return nil
	}

	// Mark all as scanning
	var cmds []tea.Cmd
	for _, idx := range pendingIndices {
		entry := m.entries[idx]
		m.overviewScanningSet[entry.path] = true
		cmd := scanOverviewPathCmd(entry.path, idx)
		cmds = append(cmds, cmd)
	}

	m.overviewScanning = true
	remaining := 0
	for _, e := range m.entries {
		if e.size < 0 {
			remaining++
		}
	}
	if len(pendingIndices) > 0 {
		firstEntry := m.entries[pendingIndices[0]]
		if len(pendingIndices) == 1 {
			m.status = fmt.Sprintf("Scanning %s... (%d left)", firstEntry.name, remaining)
		} else {
			m.status = fmt.Sprintf("Scanning %d directories... (%d left)", len(pendingIndices), remaining)
		}
	}

	cmds = append(cmds, tickCmd())
	return tea.Batch(cmds...)
}

func (m *model) updateScanProgress(files, dirs, bytes int64, path string) {
	if m.filesScanned != nil {
		atomic.StoreInt64(m.filesScanned, files)
	}
	if m.dirsScanned != nil {
		atomic.StoreInt64(m.dirsScanned, dirs)
	}
	if m.bytesScanned != nil {
		atomic.StoreInt64(m.bytesScanned, bytes)
	}
	if m.currentPath != nil && path != "" {
		*m.currentPath = path
	}
}

func (m *model) getScanProgress() (files, dirs, bytes int64) {
	if m.filesScanned != nil {
		files = atomic.LoadInt64(m.filesScanned)
	}
	if m.dirsScanned != nil {
		dirs = atomic.LoadInt64(m.dirsScanned)
	}
	if m.bytesScanned != nil {
		bytes = atomic.LoadInt64(m.bytesScanned)
	}
	return
}

func (m *model) getOverviewScanProgress() (files, dirs, bytes int64) {
	if m.overviewFilesScanned != nil {
		files = atomic.LoadInt64(m.overviewFilesScanned)
	}
	if m.overviewDirsScanned != nil {
		dirs = atomic.LoadInt64(m.overviewDirsScanned)
	}
	if m.overviewBytesScanned != nil {
		bytes = atomic.LoadInt64(m.overviewBytesScanned)
	}
	return
}

func (m model) Init() tea.Cmd {
	if m.isOverview {
		return m.scheduleOverviewScans()
	}
	return tea.Batch(m.scanCmd(m.path), tickCmd())
}

func (m model) scanCmd(path string) tea.Cmd {
	return func() tea.Msg {
		// Try to load from persistent cache first
		if cached, err := loadCacheFromDisk(path); err == nil {
			result := scanResult{
				entries:    cached.Entries,
				largeFiles: cached.LargeFiles,
				totalSize:  cached.TotalSize,
			}
			return scanResultMsg{result: result, err: nil}
		}

		// Use singleflight to avoid duplicate scans of the same path
		// If multiple goroutines request the same path, only one scan will be performed
		v, err, _ := scanGroup.Do(path, func() (interface{}, error) {
			return scanPathConcurrent(path, m.filesScanned, m.dirsScanned, m.bytesScanned, m.currentPath)
		})

		if err != nil {
			return scanResultMsg{err: err}
		}

		result := v.(scanResult)

		// Save to persistent cache asynchronously with error logging
		go func(p string, r scanResult) {
			if err := saveCacheToDisk(p, r); err != nil {
				// Log error but don't fail the scan
				_ = err // Cache save failure is not critical
			}
		}(path, result)

		return scanResultMsg{result: result, err: nil}
	}
}

func tickCmd() tea.Cmd {
	return tea.Tick(time.Millisecond*120, func(t time.Time) tea.Msg {
		return tickMsg(t)
	})
}

func (m model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.KeyMsg:
		return m.updateKey(msg)
	case deleteProgressMsg:
		if msg.done {
			m.deleting = false
			if msg.err != nil {
				m.status = fmt.Sprintf("Failed to delete: %v", msg.err)
			} else {
				m.status = fmt.Sprintf("Deleted %d items", msg.count)
				// Mark all caches as dirty
				for i := range m.history {
					m.history[i].dirty = true
				}
				for path := range m.cache {
					entry := m.cache[path]
					entry.dirty = true
					m.cache[path] = entry
				}
				// Refresh the view
				m.scanning = true
				return m, tea.Batch(m.scanCmd(m.path), tickCmd())
			}
		}
		return m, nil
	case scanResultMsg:
		m.scanning = false
		if msg.err != nil {
			m.status = fmt.Sprintf("Scan failed: %v", msg.err)
			return m, nil
		}
		m.entries = msg.result.entries
		m.largeFiles = msg.result.largeFiles
		m.totalSize = msg.result.totalSize
		m.status = fmt.Sprintf("Scanned %s", humanizeBytes(m.totalSize))
		m.clampEntrySelection()
		m.clampLargeSelection()
		m.cache[m.path] = cacheSnapshot(m)
		if m.totalSize > 0 {
			if m.overviewSizeCache == nil {
				m.overviewSizeCache = make(map[string]int64)
			}
			m.overviewSizeCache[m.path] = m.totalSize
			go func(path string, size int64) {
				_ = storeOverviewSize(path, size)
			}(m.path, m.totalSize)
		}
		return m, nil
	case overviewSizeMsg:
		// Remove from scanning set
		delete(m.overviewScanningSet, msg.path)

		if msg.err == nil {
			if m.overviewSizeCache == nil {
				m.overviewSizeCache = make(map[string]int64)
			}
			m.overviewSizeCache[msg.path] = msg.size
		}

		if m.isOverview {
			// Update entry with result
			for i := range m.entries {
				if m.entries[i].path == msg.path {
					if msg.err == nil {
						m.entries[i].size = msg.size
					} else {
						m.entries[i].size = 0
					}
					break
				}
			}
			m.totalSize = sumKnownEntrySizes(m.entries)

			// Show error briefly if any
			if msg.err != nil {
				m.status = fmt.Sprintf("Unable to measure %s: %v", displayPath(msg.path), msg.err)
			}

			// Schedule next batch of scans
			cmd := m.scheduleOverviewScans()
			return m, cmd
		}
		return m, nil
	case tickMsg:
		// Keep spinner running if scanning or deleting or if there are pending overview items
		hasPending := false
		if m.isOverview {
			for _, entry := range m.entries {
				if entry.size < 0 {
					hasPending = true
					break
				}
			}
		}
		if m.scanning || m.deleting || (m.isOverview && (m.overviewScanning || hasPending)) {
			m.spinner = (m.spinner + 1) % len(spinnerFrames)
			// Update delete progress status
			if m.deleting && m.deleteCount != nil {
				count := atomic.LoadInt64(m.deleteCount)
				if count > 0 {
					m.status = fmt.Sprintf("Deleting... %s items removed", formatNumber(count))
				}
			}
			return m, tickCmd()
		}
		return m, nil
	default:
		return m, nil
	}
}

func (m model) updateKey(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	// Handle delete confirmation
	if m.deleteConfirm {
		if msg.String() == "delete" || msg.String() == "backspace" {
			// Confirm delete - start async deletion
			if m.deleteTarget != nil {
				m.deleteConfirm = false
				m.deleting = true
				var deleteCount int64
				m.deleteCount = &deleteCount
				targetPath := m.deleteTarget.path
				targetName := m.deleteTarget.name
				m.deleteTarget = nil
				m.status = fmt.Sprintf("Deleting %s...", targetName)
				return m, tea.Batch(deletePathCmd(targetPath, m.deleteCount), tickCmd())
			}
			m.deleteConfirm = false
			m.deleteTarget = nil
			return m, nil
		} else if msg.String() == "esc" || msg.String() == "q" {
			// Cancel delete with ESC or Q
			m.status = "Cancelled"
			m.deleteConfirm = false
			m.deleteTarget = nil
			return m, nil
		} else {
			// Any other key also cancels
			m.status = "Cancelled"
			m.deleteConfirm = false
			m.deleteTarget = nil
			return m, nil
		}
	}

	switch msg.String() {
	case "q", "ctrl+c":
		return m, tea.Quit
	case "esc":
		if m.showLargeFiles {
			m.showLargeFiles = false
			return m, nil
		}
		return m, tea.Quit
	case "up", "k":
		if m.showLargeFiles {
			if m.largeSelected > 0 {
				m.largeSelected--
				if m.largeSelected < m.largeOffset {
					m.largeOffset = m.largeSelected
				}
			}
		} else if len(m.entries) > 0 && m.selected > 0 {
			m.selected--
			if m.selected < m.offset {
				m.offset = m.selected
			}
		}
	case "down", "j":
		if m.showLargeFiles {
			if m.largeSelected < len(m.largeFiles)-1 {
				m.largeSelected++
				if m.largeSelected >= m.largeOffset+largeViewport {
					m.largeOffset = m.largeSelected - largeViewport + 1
				}
			}
		} else if len(m.entries) > 0 && m.selected < len(m.entries)-1 {
			m.selected++
			if m.selected >= m.offset+entryViewport {
				m.offset = m.selected - entryViewport + 1
			}
		}
	case "enter":
		if m.showLargeFiles {
			return m, nil
		}
		return m.enterSelectedDir()
	case "right":
		if m.showLargeFiles {
			return m, nil
		}
		return m.enterSelectedDir()
	case "b", "left":
		if m.showLargeFiles {
			m.showLargeFiles = false
			return m, nil
		}
		if len(m.history) == 0 {
			// Return to overview if at top level
			if !m.isOverview {
				return m, m.switchToOverviewMode()
			}
			return m, nil
		}
		last := m.history[len(m.history)-1]
		m.history = m.history[:len(m.history)-1]
		m.path = last.path
		m.selected = last.selected
		m.offset = last.entryOffset
		m.largeSelected = last.largeSelected
		m.largeOffset = last.largeOffset
		m.isOverview = false
		if last.dirty {
			m.status = "Scanning..."
			m.scanning = true
			return m, tea.Batch(m.scanCmd(m.path), tickCmd())
		}
		m.entries = last.entries
		m.largeFiles = last.largeFiles
		m.totalSize = last.totalSize
		m.clampEntrySelection()
		m.clampLargeSelection()
		if len(m.entries) == 0 {
			m.selected = 0
		} else if m.selected >= len(m.entries) {
			m.selected = len(m.entries) - 1
		}
		if m.selected < 0 {
			m.selected = 0
		}
		m.status = fmt.Sprintf("Scanned %s", humanizeBytes(m.totalSize))
		m.scanning = false
		return m, nil
	case "r":
		m.status = "Refreshing..."
		m.scanning = true
		return m, tea.Batch(m.scanCmd(m.path), tickCmd())
	case "l":
		m.showLargeFiles = !m.showLargeFiles
		if m.showLargeFiles {
			m.largeSelected = 0
			m.largeOffset = 0
		}
	case "o":
		// Open selected entry
		if m.showLargeFiles {
			if len(m.largeFiles) > 0 {
				selected := m.largeFiles[m.largeSelected]
				go func(path string) {
					ctx, cancel := context.WithTimeout(context.Background(), openCommandTimeout)
					defer cancel()
					_ = exec.CommandContext(ctx, "open", path).Run()
				}(selected.path)
				m.status = fmt.Sprintf("Opening %s...", selected.name)
			}
		} else if len(m.entries) > 0 {
			selected := m.entries[m.selected]
			go func(path string) {
				ctx, cancel := context.WithTimeout(context.Background(), openCommandTimeout)
				defer cancel()
				_ = exec.CommandContext(ctx, "open", path).Run()
			}(selected.path)
			m.status = fmt.Sprintf("Opening %s...", selected.name)
		}
	case "f", "F":
		// Reveal selected entry in Finder
		if m.showLargeFiles {
			if len(m.largeFiles) > 0 {
				selected := m.largeFiles[m.largeSelected]
				go func(path string) {
					ctx, cancel := context.WithTimeout(context.Background(), openCommandTimeout)
					defer cancel()
					_ = exec.CommandContext(ctx, "open", "-R", path).Run()
				}(selected.path)
				m.status = fmt.Sprintf("Revealing %s in Finder...", selected.name)
			}
		} else if len(m.entries) > 0 {
			selected := m.entries[m.selected]
			go func(path string) {
				ctx, cancel := context.WithTimeout(context.Background(), openCommandTimeout)
				defer cancel()
				_ = exec.CommandContext(ctx, "open", "-R", path).Run()
			}(selected.path)
			m.status = fmt.Sprintf("Revealing %s in Finder...", selected.name)
		}
	case "delete", "backspace":
		// Delete selected file or directory
		if m.showLargeFiles {
			if len(m.largeFiles) > 0 {
				selected := m.largeFiles[m.largeSelected]
				m.deleteConfirm = true
				m.deleteTarget = &dirEntry{
					name:  selected.name,
					path:  selected.path,
					size:  selected.size,
					isDir: false,
				}
			}
		} else if len(m.entries) > 0 && !m.isOverview {
			selected := m.entries[m.selected]
			m.deleteConfirm = true
			m.deleteTarget = &selected
		}
	}
	return m, nil
}

func (m *model) switchToOverviewMode() tea.Cmd {
	m.isOverview = true
	m.path = "/"
	m.scanning = false
	m.showLargeFiles = false
	m.largeFiles = nil
	m.largeSelected = 0
	m.largeOffset = 0
	m.deleteConfirm = false
	m.deleteTarget = nil
	m.selected = 0
	m.offset = 0
	m.hydrateOverviewEntries()
	cmd := m.scheduleOverviewScans()
	if cmd == nil {
		m.status = "Ready"
		return nil
	}
	// Start tick to animate spinner while scanning
	return tea.Batch(cmd, tickCmd())
}

func (m model) enterSelectedDir() (tea.Model, tea.Cmd) {
	if len(m.entries) == 0 {
		return m, nil
	}
	selected := m.entries[m.selected]
	if selected.isDir {
		if !m.isOverview {
			m.history = append(m.history, snapshotFromModel(m))
		}
		m.path = selected.path
		m.selected = 0
		m.offset = 0
		m.status = "Scanning..."
		m.scanning = true
		m.isOverview = false

		// Reset scan counters for new scan
		atomic.StoreInt64(m.filesScanned, 0)
		atomic.StoreInt64(m.dirsScanned, 0)
		atomic.StoreInt64(m.bytesScanned, 0)
		if m.currentPath != nil {
			*m.currentPath = ""
		}

		if cached, ok := m.cache[m.path]; ok && !cached.dirty {
			m.entries = cloneDirEntries(cached.entries)
			m.largeFiles = cloneFileEntries(cached.largeFiles)
			m.totalSize = cached.totalSize
			m.selected = cached.selected
			m.offset = cached.entryOffset
			m.largeSelected = cached.largeSelected
			m.largeOffset = cached.largeOffset
			m.clampEntrySelection()
			m.clampLargeSelection()
			m.status = fmt.Sprintf("Cached view for %s", displayPath(m.path))
			m.scanning = false
			return m, nil
		}
		return m, tea.Batch(m.scanCmd(m.path), tickCmd())
	}
	m.status = fmt.Sprintf("File: %s (%s)", selected.name, humanizeBytes(selected.size))
	return m, nil
}

func (m model) View() string {
	var b strings.Builder
	fmt.Fprintln(&b)

	if m.deleteConfirm && m.deleteTarget != nil {
		// Show delete confirmation prominently at the top
		fmt.Fprintf(&b, "%sDelete: %s (%s)? Press Delete again to confirm, ESC to cancel%s\n",
			colorRed, m.deleteTarget.name, humanizeBytes(m.deleteTarget.size), colorReset)
	}

	if m.isOverview {
		fmt.Fprintf(&b, "%sAnalyze Disk%s\n", colorPurple, colorReset)
		if m.overviewScanning {
			// Check if we're in initial scan (all entries are pending)
			allPending := true
			for _, entry := range m.entries {
				if entry.size >= 0 {
					allPending = false
					break
				}
			}

			if allPending {
				// Show prominent loading screen for initial scan
				fmt.Fprintf(&b, "%s%s%s%s Analyzing disk usage, please wait...%s\n",
					colorCyan, colorBold,
					spinnerFrames[m.spinner],
					colorReset, colorReset)
				return b.String()
			} else {
				// Progressive scanning - show subtle indicator
				fmt.Fprintf(&b, "%sSelect a location to explore:%s  ", colorGray, colorReset)
				fmt.Fprintf(&b, "%s%s%s%s Scanning...\n\n", colorCyan, colorBold, spinnerFrames[m.spinner], colorReset)
			}
		} else {
			// Check if there are still pending items
			hasPending := false
			for _, entry := range m.entries {
				if entry.size < 0 {
					hasPending = true
					break
				}
			}
			if hasPending {
				fmt.Fprintf(&b, "%sSelect a location to explore:%s  ", colorGray, colorReset)
				fmt.Fprintf(&b, "%s%s%s%s Scanning...\n\n", colorCyan, colorBold, spinnerFrames[m.spinner], colorReset)
			} else {
				fmt.Fprintf(&b, "%sSelect a location to explore:%s\n\n", colorGray, colorReset)
			}
		}
	} else {
		fmt.Fprintf(&b, "%sAnalyze Disk%s  %s%s%s", colorPurple, colorReset, colorGray, displayPath(m.path), colorReset)
		if !m.scanning {
			fmt.Fprintf(&b, "  |  Total: %s", humanizeBytes(m.totalSize))
		}
		fmt.Fprintf(&b, "\n\n")
	}

	if m.deleting {
		// Show delete progress
		count := int64(0)
		if m.deleteCount != nil {
			count = atomic.LoadInt64(m.deleteCount)
		}

		fmt.Fprintf(&b, "%s%s%s%s Deleting: %s%s items%s removed, please wait...\n",
			colorCyan, colorBold,
			spinnerFrames[m.spinner],
			colorReset,
			colorYellow, formatNumber(count), colorReset)

		return b.String()
	}

	if m.scanning {
		filesScanned, dirsScanned, bytesScanned := m.getScanProgress()

		fmt.Fprintf(&b, "%s%s%s%s Scanning: %s%s files%s, %s%s dirs%s, %s%s%s\n",
			colorCyan, colorBold,
			spinnerFrames[m.spinner],
			colorReset,
			colorYellow, formatNumber(filesScanned), colorReset,
			colorYellow, formatNumber(dirsScanned), colorReset,
			colorGreen, humanizeBytes(bytesScanned), colorReset)

		if m.currentPath != nil {
			currentPath := *m.currentPath
			if currentPath != "" {
				shortPath := displayPath(currentPath)
				shortPath = truncateMiddle(shortPath, 60)
				fmt.Fprintf(&b, "%s%s%s\n", colorGray, shortPath, colorReset)
			}
		}

		return b.String()
	}

	if m.showLargeFiles {
		if len(m.largeFiles) == 0 {
			fmt.Fprintln(&b, "  No large files found (>=100MB)")
		} else {
			start := m.largeOffset
			if start < 0 {
				start = 0
			}
			end := start + largeViewport
			if end > len(m.largeFiles) {
				end = len(m.largeFiles)
			}
			maxLargeSize := int64(1)
			for _, file := range m.largeFiles {
				if file.size > maxLargeSize {
					maxLargeSize = file.size
				}
			}
			for idx := start; idx < end; idx++ {
				file := m.largeFiles[idx]
				shortPath := displayPath(file.path)
				shortPath = truncateMiddle(shortPath, 35)
				paddedPath := padName(shortPath, 35)
				entryPrefix := "   "
				nameColor := ""
				sizeColor := colorGray
				numColor := ""
				if idx == m.largeSelected {
					entryPrefix = fmt.Sprintf(" %s%s▶%s ", colorCyan, colorBold, colorReset)
					nameColor = colorCyan
					sizeColor = colorCyan
					numColor = colorCyan
				}
				size := humanizeBytes(file.size)
				bar := coloredProgressBar(file.size, maxLargeSize, 0)
				fmt.Fprintf(&b, "%s%s%2d.%s %s  |  📄 %s%s%s  %s%10s%s\n",
					entryPrefix, numColor, idx+1, colorReset, bar, nameColor, paddedPath, colorReset, sizeColor, size, colorReset)
			}
		}
	} else {
		if len(m.entries) == 0 {
			fmt.Fprintln(&b, "  Empty directory")
		} else {
			if m.isOverview {
				maxSize := int64(1)
				for _, entry := range m.entries {
					if entry.size > maxSize {
						maxSize = entry.size
					}
				}
				totalSize := m.totalSize
				for idx, entry := range m.entries {
					icon := "📁"
					sizeVal := entry.size
					barValue := sizeVal
					if barValue < 0 {
						barValue = 0
					}
					var percent float64
					if totalSize > 0 && sizeVal >= 0 {
						percent = float64(sizeVal) / float64(totalSize) * 100
					} else {
						percent = 0
					}
					percentStr := fmt.Sprintf("%5.1f%%", percent)
					if totalSize == 0 || sizeVal < 0 {
						percentStr = "  --  "
					}
					bar := coloredProgressBar(barValue, maxSize, percent)
					sizeText := "pending.."
					if sizeVal >= 0 {
						sizeText = humanizeBytes(sizeVal)
					}
					sizeColor := colorGray
					if sizeVal >= 0 && totalSize > 0 {
						switch {
						case percent >= 50:
							sizeColor = colorRed
						case percent >= 20:
							sizeColor = colorYellow
						case percent >= 5:
							sizeColor = colorCyan
						default:
							sizeColor = colorGray
						}
					}
					entryPrefix := "   "
					name := trimName(entry.name)
					paddedName := padName(name, 28)
					nameSegment := fmt.Sprintf("%s %s", icon, paddedName)
					numColor := ""
					percentColor := ""
					if idx == m.selected {
						entryPrefix = fmt.Sprintf(" %s%s▶%s ", colorCyan, colorBold, colorReset)
						nameSegment = fmt.Sprintf("%s%s %s%s", colorCyan, icon, paddedName, colorReset)
						numColor = colorCyan
						percentColor = colorCyan
						sizeColor = colorCyan
					}
					displayIndex := idx + 1

					// Add unused time label if applicable
					// For overview mode, get access time on-demand if not set
					lastAccess := entry.lastAccess
					if lastAccess.IsZero() && entry.path != "" {
						lastAccess = getLastAccessTime(entry.path)
					}
					unusedLabel := formatUnusedTime(lastAccess)
					if unusedLabel == "" {
						fmt.Fprintf(&b, "%s%s%2d.%s %s %s%s%s  |  %s %s%10s%s\n",
							entryPrefix, numColor, displayIndex, colorReset, bar, percentColor, percentStr, colorReset,
							nameSegment, sizeColor, sizeText, colorReset)
					} else {
						fmt.Fprintf(&b, "%s%s%2d.%s %s %s%s%s  |  %s %s%10s%s  %s%s%s\n",
							entryPrefix, numColor, displayIndex, colorReset, bar, percentColor, percentStr, colorReset,
							nameSegment, sizeColor, sizeText, colorReset,
							colorGray, unusedLabel, colorReset)
					}
				}
			} else {
				// Normal mode with sizes and progress bars
				maxSize := int64(1)
				for _, entry := range m.entries {
					if entry.size > maxSize {
						maxSize = entry.size
					}
				}

				start := m.offset
				if start < 0 {
					start = 0
				}
				end := start + entryViewport
				if end > len(m.entries) {
					end = len(m.entries)
				}

				for idx := start; idx < end; idx++ {
					entry := m.entries[idx]
					icon := "📄"
					if entry.isDir {
						icon = "📁"
					}
					size := humanizeBytes(entry.size)
					name := trimName(entry.name)
					paddedName := padName(name, 28)

					// Calculate percentage
					percent := float64(entry.size) / float64(m.totalSize) * 100
					percentStr := fmt.Sprintf("%5.1f%%", percent)

					// Get colored progress bar
					bar := coloredProgressBar(entry.size, maxSize, percent)

					// Color the size based on magnitude
					var sizeColor string
					if percent >= 50 {
						sizeColor = colorRed
					} else if percent >= 20 {
						sizeColor = colorYellow
					} else if percent >= 5 {
						sizeColor = colorCyan
					} else {
						sizeColor = colorGray
					}

					// Keep chart columns aligned even when arrow is shown
					entryPrefix := "   "
					nameSegment := fmt.Sprintf("%s %s", icon, paddedName)
					numColor := ""
					percentColor := ""
					if idx == m.selected {
						entryPrefix = fmt.Sprintf(" %s%s▶%s ", colorCyan, colorBold, colorReset)
						nameSegment = fmt.Sprintf("%s%s %s%s", colorCyan, icon, paddedName, colorReset)
						numColor = colorCyan
						percentColor = colorCyan
						sizeColor = colorCyan
					}

					displayIndex := idx + 1

					// Add unused time label if applicable
					unusedLabel := formatUnusedTime(entry.lastAccess)
					if unusedLabel == "" {
						fmt.Fprintf(&b, "%s%s%2d.%s %s %s%s%s  |  %s %s%10s%s\n",
							entryPrefix, numColor, displayIndex, colorReset, bar, percentColor, percentStr, colorReset,
							nameSegment, sizeColor, size, colorReset)
					} else {
						fmt.Fprintf(&b, "%s%s%2d.%s %s %s%s%s  |  %s %s%10s%s  %s%s%s\n",
							entryPrefix, numColor, displayIndex, colorReset, bar, percentColor, percentStr, colorReset,
							nameSegment, sizeColor, size, colorReset,
							colorGray, unusedLabel, colorReset)
					}
				}
			}
		}
	}

	fmt.Fprintln(&b)
	if m.isOverview {
		fmt.Fprintf(&b, "%s↑/↓ Nav  |  Enter  |  O Open  |  F Reveal  |  Q Quit%s\n", colorGray, colorReset)
	} else if m.showLargeFiles {
		fmt.Fprintf(&b, "%s↑/↓ Nav  |  O Open  |  F Reveal  |  ⌫ Delete  |  L Back  |  Q Quit%s\n", colorGray, colorReset)
	} else {
		largeFileCount := len(m.largeFiles)
		if largeFileCount > 0 {
			fmt.Fprintf(&b, "%s↑/↓/←/→ Nav  |  Enter  |  O Open  |  F Reveal  |  ⌫ Delete  |  L Large(%d)  |  Q Quit%s\n", colorGray, largeFileCount, colorReset)
		} else {
			fmt.Fprintf(&b, "%s↑/↓/←/→ Nav  |  Enter  |  O Open  |  F Reveal  |  ⌫ Delete  |  Q Quit%s\n", colorGray, colorReset)
		}
	}
	return b.String()
}

func (m *model) clampEntrySelection() {
	if len(m.entries) == 0 {
		m.selected = 0
		m.offset = 0
		return
	}
	if m.selected >= len(m.entries) {
		m.selected = len(m.entries) - 1
	}
	if m.selected < 0 {
		m.selected = 0
	}
	maxOffset := len(m.entries) - entryViewport
	if maxOffset < 0 {
		maxOffset = 0
	}
	if m.offset > maxOffset {
		m.offset = maxOffset
	}
	if m.selected < m.offset {
		m.offset = m.selected
	}
	if m.selected >= m.offset+entryViewport {
		m.offset = m.selected - entryViewport + 1
	}
}

func (m *model) clampLargeSelection() {
	if len(m.largeFiles) == 0 {
		m.largeSelected = 0
		m.largeOffset = 0
		return
	}
	if m.largeSelected >= len(m.largeFiles) {
		m.largeSelected = len(m.largeFiles) - 1
	}
	if m.largeSelected < 0 {
		m.largeSelected = 0
	}
	maxOffset := len(m.largeFiles) - largeViewport
	if maxOffset < 0 {
		maxOffset = 0
	}
	if m.largeOffset > maxOffset {
		m.largeOffset = maxOffset
	}
	if m.largeSelected < m.largeOffset {
		m.largeOffset = m.largeSelected
	}
	if m.largeSelected >= m.largeOffset+largeViewport {
		m.largeOffset = m.largeSelected - largeViewport + 1
	}
}

func sumKnownEntrySizes(entries []dirEntry) int64 {
	var total int64
	for _, entry := range entries {
		if entry.size > 0 {
			total += entry.size
		}
	}
	return total
}

func nextPendingOverviewIndex(entries []dirEntry) int {
	for i, entry := range entries {
		if entry.size < 0 {
			return i
		}
	}
	return -1
}

func hasPendingOverviewEntries(entries []dirEntry) bool {
	for _, entry := range entries {
		if entry.size < 0 {
			return true
		}
	}
	return false
}

func scanOverviewPathCmd(path string, index int) tea.Cmd {
	return func() tea.Msg {
		size, err := measureOverviewSize(path)
		return overviewSizeMsg{
			path:  path,
			index: index,
			size:  size,
			err:   err,
		}
	}
}

// deletePathCmd deletes a path recursively with progress tracking

// measureOverviewSize calculates the size of a directory using multiple strategies:
// 1. Check JSON cache (fast)
// 2. Try du command (fast and accurate)
// 3. Walk the directory to get logical size (accurate but slower)
// 4. Check gob cache (fallback)

// getDirectorySizeFromMetadata attempts to retrieve directory size using macOS Spotlight metadata.
// This is much faster than filesystem traversal but may not be available for all directories.

// getDirectorySizeFromDu calculates directory size using the du command.
// Uses -s to summarize total size including all subdirectories.

// getDirectoryLogicalSize walks the directory tree and sums file sizes to estimate
// the logical (Finder-style) usage.

// Persistent cache functions
