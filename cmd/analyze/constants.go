package main

import "time"

const (
	maxEntries            = 30
	maxLargeFiles         = 30
	barWidth              = 24
	minLargeFileSize      = 100 << 20 // 100 MB
	entryViewport         = 12
	largeViewport         = 12
	overviewCacheTTL      = 7 * 24 * time.Hour // 7 days
	overviewCacheFile     = "overview_sizes.json"
	duTimeout             = 60 * time.Second // Increased for large directories
	mdlsTimeout           = 5 * time.Second
	maxConcurrentOverview = 3                // Scan up to 3 overview dirs concurrently
	batchUpdateSize       = 100              // Batch atomic updates every N items
	cacheModTimeGrace     = 30 * time.Minute // Ignore minor directory mtime bumps

	// Worker pool configuration
	minWorkers         = 8                // Minimum workers for better I/O throughput
	maxWorkers         = 64               // Maximum workers to avoid excessive goroutines
	cpuMultiplier      = 2                // Worker multiplier per CPU core for I/O-bound operations
	maxDirWorkers      = 16               // Maximum concurrent subdirectory scans
	openCommandTimeout = 10 * time.Second // Timeout for open/reveal commands
)

var foldDirs = map[string]bool{
	// Version control
	".git": true,
	".svn": true,
	".hg":  true,

	// JavaScript/Node
	"node_modules":                  true,
	".npm":                          true,
	"_npx":                          true, // ~/.npm/_npx global cache
	"_cacache":                      true, // ~/.npm/_cacache
	"_logs":                         true,
	"_locks":                        true,
	"_quick":                        true,
	"_libvips":                      true,
	"_prebuilds":                    true,
	"_update-notifier-last-checked": true,
	".yarn":                         true,
	".pnpm-store":                   true,
	".next":                         true,
	".nuxt":                         true,
	"bower_components":              true,
	".vite":                         true,
	".turbo":                        true,
	".parcel-cache":                 true,
	".nx":                           true,
	".rush":                         true,
	"tnpm":                          true,
	".tnpm":                         true,
	".bun":                          true,
	".deno":                         true,

	// Python
	"__pycache__":   true,
	".pytest_cache": true,
	".mypy_cache":   true,
	".ruff_cache":   true,
	"venv":          true,
	".venv":         true,
	"virtualenv":    true,
	".tox":          true,
	"site-packages": true,
	".eggs":         true,
	"*.egg-info":    true,
	".pyenv":        true,
	".poetry":       true,
	".pip":          true,
	".pipx":         true,

	// Ruby/Go/PHP (vendor), Java/Kotlin/Scala/Rust (target)
	"vendor":        true,
	".bundle":       true,
	"gems":          true,
	".rbenv":        true,
	"target":        true,
	".gradle":       true,
	".m2":           true,
	".ivy2":         true,
	"out":           true,
	"pkg":           true,
	"composer.phar": true,
	".composer":     true,
	".cargo":        true,

	// Build outputs
	"build":     true,
	"dist":      true,
	".output":   true,
	"coverage":  true,
	".coverage": true,

	// IDE
	".idea":   true,
	".vscode": true,
	".vs":     true,
	".fleet":  true,

	// Cache directories
	".cache":                  true,
	"__MACOSX":                true,
	".DS_Store":               true,
	".Trash":                  true,
	"Caches":                  true,
	".Spotlight-V100":         true,
	".fseventsd":              true,
	".DocumentRevisions-V100": true,
	".TemporaryItems":         true,
	"$RECYCLE.BIN":            true,
	".temp":                   true,
	".tmp":                    true,
	"_temp":                   true,
	"_tmp":                    true,
	".Homebrew":               true,
	".rustup":                 true,
	".sdkman":                 true,
	".nvm":                    true,

	// macOS specific
	"Application Scripts":     true,
	"Saved Application State": true,

	// iCloud
	"Mobile Documents": true,

	// Docker & Containers
	".docker":     true,
	".containerd": true,

	// Mobile development
	"Pods":        true,
	"DerivedData": true,
	".build":      true,
	"xcuserdata":  true,
	"Carthage":    true,

	// Web frameworks
	".angular":    true,
	".svelte-kit": true,
	".astro":      true,
	".solid":      true,

	// Databases
	".mysql":    true,
	".postgres": true,
	"mongodb":   true,

	// Other
	".terraform": true,
	".vagrant":   true,
	"tmp":        true,
	"temp":       true,
}

var skipSystemDirs = map[string]bool{
	"dev":                     true,
	"tmp":                     true,
	"private":                 true,
	"cores":                   true,
	"net":                     true,
	"home":                    true,
	"System":                  true,
	"sbin":                    true,
	"bin":                     true,
	"etc":                     true,
	"var":                     true,
	".vol":                    true,
	".Spotlight-V100":         true,
	".fseventsd":              true,
	".DocumentRevisions-V100": true,
	".TemporaryItems":         true,
}

var skipExtensions = map[string]bool{
	".go":     true,
	".js":     true,
	".ts":     true,
	".tsx":    true,
	".jsx":    true,
	".json":   true,
	".md":     true,
	".txt":    true,
	".yml":    true,
	".yaml":   true,
	".xml":    true,
	".html":   true,
	".css":    true,
	".scss":   true,
	".sass":   true,
	".less":   true,
	".py":     true,
	".rb":     true,
	".java":   true,
	".kt":     true,
	".rs":     true,
	".swift":  true,
	".m":      true,
	".mm":     true,
	".c":      true,
	".cpp":    true,
	".h":      true,
	".hpp":    true,
	".cs":     true,
	".sql":    true,
	".db":     true,
	".lock":   true,
	".gradle": true,
	".mjs":    true,
	".cjs":    true,
	".coffee": true,
	".dart":   true,
	".svelte": true,
	".vue":    true,
	".nim":    true,
	".hx":     true,
}

var spinnerFrames = []string{"|", "/", "-", "\\", "|", "/", "-", "\\"}

const (
	colorPurple = "\033[0;35m"
	colorGray   = "\033[0;90m"
	colorRed    = "\033[0;31m"
	colorYellow = "\033[1;33m"
	colorGreen  = "\033[0;32m"
	colorCyan   = "\033[0;36m"
	colorReset  = "\033[0m"
	colorBold   = "\033[1m"
)
