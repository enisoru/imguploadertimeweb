package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"image"
	"image/jpeg"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/disintegration/imaging"
	"github.com/go-redis/redis/v8"
	"github.com/gorilla/websocket"
	"github.com/jdeng/goheif"
	"github.com/mdouchement/dng"
	_ "github.com/oov/psd"
	"github.com/pbnjay/memory"
	"github.com/shirou/gopsutil/v3/cpu"
	"github.com/shirou/gopsutil/v3/disk"
	"github.com/shirou/gopsutil/v3/mem"
	"github.com/shirou/gopsutil/v3/host"
	_ "golang.org/x/image/bmp"
	_ "golang.org/x/image/tiff"
	_ "golang.org/x/image/webp"
	_ "image/gif"
	_ "image/png"
)

const (
	uploadDir      = "./uploads"
	thumbnailDir   = "./thumbnails"
	thumbnailWidth = 350
)

var (
	upgrader = websocket.Upgrader{
		CheckOrigin: func(r *http.Request) bool {
			return true
		},
	}
	// Map of UserID -> Set of Websocket Connections
	clients = make(map[string]map[*websocket.Conn]bool)
	mu      sync.Mutex // Mutex to protect clients map

	// Redis client
	rdb *redis.Client
	ctx = context.Background()

	// Config
	workerCount          int
	maxUploadSize        int64
	maxPixels            int
	maxConcurrentPerUser int
)

// FileInfo stores status and URL for a file
type FileInfo struct {
	Status       string `json:"status"`
	URL          string `json:"url,omitempty"`
	OriginalName string `json:"original_name"`
	CreatedAt    int64  `json:"created_at"`
}

// Progress stores overall upload progress for a user
type Progress struct {
	Total     int                 `json:"total"`
	Completed int                 `json:"completed"`
	Files     map[string]FileInfo `json:"files"`
}

// SystemStats stores server health and metrics
type SystemStats struct {
	TotalUsers      int64   `json:"total_users"`
	ActiveUploads   int64   `json:"active_uploads"` // Currently processing
	QueueLength     int64   `json:"queue_length"`
	TotalImages     int64   `json:"total_images"`
	TotalSizeGB     float64 `json:"total_size_gb"`
	CPUUsage        float64 `json:"cpu_usage"`
	RAMUsed         uint64  `json:"ram_used"`
	RAMTotal        uint64  `json:"ram_total"`
	DiskFree        uint64  `json:"disk_free"`
	DiskTotal       uint64  `json:"disk_total"`
	GoVersion       string  `json:"go_version"`
	NumGoroutine    int     `json:"num_goroutine"`
	HostTotalMemory uint64  `json:"host_total_memory"`
	MemoryLimit     int64   `json:"memory_limit"`
}

// Job represents an image processing task
type Job struct {
	Filename string `json:"filename"`
	Path     string `json:"path"`
	UserID   string `json:"user_id"`
}

var startTime = time.Now()

func bytesToGB(b uint64) float64 {
	return float64(b) / 1024 / 1024 / 1024
}

// Logs comprehensive information about the system and hardware
func logSystemInfo() {
	log.Println("=======================================")
	log.Println(">>> SYSTEM HARDWARE AND NETWORK INFO <<<")
	log.Println("=======================================")

	// 1. Host and OS Info
	hInfo, err := host.Info()
	if err == nil {
		log.Printf("  Hostname: %s", hInfo.Hostname)
		log.Printf("  OS: %s, Platform: %s %s", hInfo.OS, hInfo.Platform, hInfo.PlatformVersion)
		log.Printf("  Architecture: %s", runtime.GOARCH)
		log.Printf("  Uptime: %.1f days", float64(hInfo.Uptime)/(60*60*24))
	} else {
		log.Printf("  Host Info Error: %v", err)
	}
	
	// 2. CPU and RAM Info
	cInfos, err := cpu.Info()
    if err == nil && len(cInfos) > 0 {
        // Логируем модель первого CPU
        log.Printf("  CPU Model: %s", cInfos[0].ModelName)
    }
	log.Printf("  Total CPU Cores: %d", runtime.NumCPU())
	log.Printf("  Total RAM: %.2f GB", bytesToGB(memory.TotalMemory()))
	log.Printf("  Go Max Processes (GOMAXPROCS): %d", runtime.GOMAXPROCS(-1))
	log.Printf("  App Workers (WORKER_COUNT): %d", workerCount)
	

	// 3. Disk Usage (for the partition where './uploads' lives)
	// Используем путь к директории, чтобы убедиться, что смотрим нужный диск
	absUploadDir, _ := filepath.Abs(uploadDir)
	dUsage, err := disk.Usage(absUploadDir)
	if err == nil {
		log.Printf("  Disk Path: %s", dUsage.Path)
		log.Printf("  Disk Total: %.2f GB | Free: %.2f GB | Used: %.1f%%", 
			bytesToGB(dUsage.Total), 
			bytesToGB(dUsage.Free), 
			dUsage.UsedPercent)
	} else {
        log.Printf("  Disk Usage Error for %s: %v (Is the path accessible?)", absUploadDir, err)
    }
	
	
	log.Println("=======================================")
}

func init() {
	// Initialize Redis client
	redisAddr := os.Getenv("REDIS_ADDR")
	if redisAddr == "" {
		redisAddr = "localhost:6379"
	}

	rdb = redis.NewClient(&redis.Options{
		Addr:     redisAddr,
		Password: "",
		DB:       0,
	})

	// Ping Redis to check connection
	_, err := rdb.Ping(ctx).Result()
	if err != nil {
		log.Fatalf("Could not connect to Redis: %v", err)
	}
	log.Println("Connected to Redis!")

	// Ensure directories exist
	os.MkdirAll(uploadDir, os.ModePerm)
	os.MkdirAll(thumbnailDir, os.ModePerm)

	// Load Config
	// УДАЛИЛ ЛИШНЮЮ СТРОКУ ОТСЮДА (workerCount = ... 15)

	maxUploadSize = int64(getEnvAsInt("MAX_UPLOAD_MB", 300)) * 1024 * 1024
	maxPixels = getEnvAsInt("MAX_PIXELS", 65000)
	maxConcurrentPerUser = getEnvAsInt("MAX_CONCURRENT_PER_USER", 5)

	// --- НОВАЯ ЛОГИКА ---
	defaultWorkers := runtime.NumCPU()
	if defaultWorkers > 1 {
		// Оставляем одно ядро свободным для приема загрузок (Upload Handler) и работы ОС
		defaultWorkers = defaultWorkers - 1
	}

	// Если переменная WORKER_COUNT не задана в .env, используем наш умный подсчет
	workerCount = getEnvAsInt("WORKER_COUNT", defaultWorkers)

	// Защита от дурака: если вдруг получилось меньше 1, ставим 1
	if workerCount < 1 {
		workerCount = 1
	}
	// ---------------------

	log.Printf("System has %d CPUs. Starting %d processing workers.", runtime.NumCPU(), workerCount)
	logSystemInfo()
	// Start Cleanup Worker
	go cleanupWorker()
}

func getEnvAsInt(key string, defaultVal int) int {
	if value, exists := os.LookupEnv(key); exists {
		if i, err := strconv.Atoi(value); err == nil {
			return i
		}
	}
	return defaultVal
}

func main() {
	// 1. Dynamic Memory Limit
	totalMem := memory.TotalMemory()
	limit := int64(float64(totalMem) * 0.95)
	debug.SetMemoryLimit(limit)
	log.Printf("Host Memory: %d MB, GOMEMLIMIT set to: %d MB", totalMem/1024/1024, limit/1024/1024)

	// Serve static files (thumbnails)
	http.Handle("/thumbnails/", http.StripPrefix("/thumbnails/", http.FileServer(http.Dir(thumbnailDir))))

	// Routes
	http.HandleFunc("/", homeHandler)
	http.HandleFunc("/upload", uploadHandler)
	http.HandleFunc("/ws", wsHandler)
	http.HandleFunc("/list", listHandler)
	http.HandleFunc("/delete-all", deleteAllHandler)

	// Start workers
	log.Printf("Starting %d workers...", workerCount)
	for i := 0; i < workerCount; i++ {
		go startWorker()
	}

	// Start Stats Collector
	go statsCollector()

	// Start server
	log.Println("Server starting on :8080")
	if err := http.ListenAndServe(":8080", nil); err != nil {
		log.Fatal(err)
	}
}

func homeHandler(w http.ResponseWriter, r *http.Request) {
	http.ServeFile(w, r, "templates/index.html")
}

func listHandler(w http.ResponseWriter, r *http.Request) {
	userID := r.URL.Query().Get("userID")
	if userID == "" {
		http.Error(w, "UserID required", http.StatusBadRequest)
		return
	}

	// Get files from Redis for this user
	filesMap, err := rdb.HGetAll(ctx, fmt.Sprintf("user:%s:files", userID)).Result()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	var files []FileInfo
	for _, v := range filesMap {
		var info FileInfo
		json.Unmarshal([]byte(v), &info)
		if info.Status == "done" && info.URL != "" {
			files = append(files, info)
		}
	}

	// Sort by CreatedAt DESC (Newest first)
	// Simple bubble sort or slice sort
	for i := 0; i < len(files); i++ {
		for j := i + 1; j < len(files); j++ {
			if files[i].CreatedAt < files[j].CreatedAt {
				files[i], files[j] = files[j], files[i]
			}
		}
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(files)
}

func deleteAllHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	userID := r.URL.Query().Get("userID")
	if userID == "" {
		http.Error(w, "UserID required", http.StatusBadRequest)
		return
	}

	// 1. Get all files to delete physical files
	filesMap, _ := rdb.HGetAll(ctx, fmt.Sprintf("user:%s:files", userID)).Result()
	for _, v := range filesMap {
		var info FileInfo
		json.Unmarshal([]byte(v), &info)
		if info.URL != "" {
			baseName := filepath.Base(info.URL)
			os.Remove(filepath.Join(thumbnailDir, baseName))
			os.Remove(filepath.Join(uploadDir, baseName))
		}
	}

	// 2. Clear Redis keys
	rdb.Del(ctx, fmt.Sprintf("user:%s:files", userID))
	rdb.Del(ctx, fmt.Sprintf("user:%s:total", userID))
	rdb.Del(ctx, fmt.Sprintf("user:%s:completed", userID))
	rdb.Del(ctx, fmt.Sprintf("queue:user:%s", userID)) // Clear user queue
	
	// Remove from active users rotation
	rdb.LRem(ctx, "users_with_pending_jobs", 0, userID)
	rdb.SRem(ctx, "active_users_set", userID)
	rdb.Del(ctx, fmt.Sprintf("processing:user:%s", userID)) // Reset processing count

	// Update global stats (approximate)
	rdb.Decr(ctx, "global:total_users")

	// 3. Notify client via WS to clear UI
	broadcastToUser(userID)

	w.WriteHeader(http.StatusOK)
}

func uploadHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Enforce Max Request Size (Total Payload)
	// Set to 10GB to allow multiple large files, while preventing infinite streams.
	// Individual files are limited by maxUploadSize (300MB) below.
	r.Body = http.MaxBytesReader(w, r.Body, 10*1024*1024*1024)

	// Get UserID from header or query
	userID := r.Header.Get("X-User-ID")
	if userID == "" {
		userID = r.URL.Query().Get("userID")
	}
	if userID == "" {
		http.Error(w, "UserID required", http.StatusBadRequest)
		return
	}

	// Track new user
	rdb.SAdd(ctx, "global:users_set", userID)

	reader, err := r.MultipartReader()
	if err != nil {
		http.Error(w, "File too large or invalid multipart", http.StatusBadRequest)
		return
	}

	for {
		part, err := reader.NextPart()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Println("Error reading part:", err)
			break
		}

		if part.FormName() == "count" {
			buf := new(strings.Builder)
			_, err := io.Copy(buf, part)
			if err == nil {
				var count int
				fmt.Sscanf(buf.String(), "%d", &count)

				// Increment total instead of resetting
				rdb.IncrBy(ctx, fmt.Sprintf("user:%s:total", userID), int64(count))
				broadcastToUser(userID)
			}
			continue
		}

		if part.FormName() == "images" {
			filename := part.FileName()
			if filename == "" {
				continue
			}

			// Create a temp file
			tempDir := filepath.Join(uploadDir, "temp")
			os.MkdirAll(tempDir, os.ModePerm)

			tempFile, err := os.CreateTemp(tempDir, "upload-*.tmp")
			if err != nil {
				log.Println("Error creating temp file:", err)
				continue
			}

			// Limit size of individual file copy
			written, err := io.Copy(tempFile, io.LimitReader(part, maxUploadSize))
			if err != nil {
				log.Println("Error saving temp file:", err)
				tempFile.Close()
				os.Remove(tempFile.Name())
				continue
			}
			tempFile.Close()

			// Update global stats
			rdb.Incr(ctx, "global:total_images")
			rdb.IncrByFloat(ctx, "global:total_size_bytes", float64(written))

			// Generate Unique System Filename
			// Format: <timestamp_nano>_<original_name>
			// This ensures uniqueness even if the same file is uploaded multiple times
			timestamp := time.Now().UnixNano()
			uniqueSystemName := fmt.Sprintf("%d_%s", timestamp, filename)

			// Store metadata
			updateFileStatus(userID, uniqueSystemName, "processing", "", filename, timestamp)

			// Enqueue job to USER SPECIFIC queue
			// We pass the uniqueSystemName as the "Filename" to the worker, so it knows what key to update
			// But we keep the original temp path
			job := Job{Filename: uniqueSystemName, Path: tempFile.Name(), UserID: userID}
			jobJSON, _ := json.Marshal(job)
			
			// 1. Push to user's personal queue
			rdb.RPush(ctx, fmt.Sprintf("queue:user:%s", userID), jobJSON)

			// 2. Ensure user is in the rotation list
			// Use SAdd to atomically check and add to set. If added (1), then add to list.
			if added, _ := rdb.SAdd(ctx, "active_users_set", userID).Result(); added > 0 {
				// LPush to add to "Left" (Newest). RPOPLPUSH takes from "Right" (Oldest).
				// This ensures FIFO (Fair) rotation.
				rdb.LPush(ctx, "users_with_pending_jobs", userID)
			}
		}
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]bool{"ok": true})
}

func startWorker() {
	for {
		// 1. Round-Robin: Rotate user list to get next user
		// RPOPLPUSH: Removes last element, pushes to front, and returns it.
		// This rotates the list: [A, B, C] -> Pop C -> Push C -> [C, A, B] -> Return C
		userID, err := rdb.RPopLPush(ctx, "users_with_pending_jobs", "users_with_pending_jobs").Result()
		
		if err == redis.Nil || userID == "" {
			// No active users
			time.Sleep(500 * time.Millisecond)
			continue
		} else if err != nil {
			log.Println("Redis error:", err)
			time.Sleep(1 * time.Second)
			continue
		}

		// 2. Check Per-User Concurrency Limit
		activeJobs, _ := rdb.Get(ctx, fmt.Sprintf("processing:user:%s", userID)).Int()
		if activeJobs >= maxConcurrentPerUser {
			// User has too many active jobs, skip them for now.
			// They are already rotated to the back of the line.
			// We continue to the next iteration to try the next user immediately.
			// Add a small sleep to prevent tight loop if ALL users are capped.
			time.Sleep(50 * time.Millisecond)
			continue
		}

		// 3. Try to get a job from this user's queue
		result, err := rdb.LPop(ctx, fmt.Sprintf("queue:user:%s", userID)).Result()
		
		if err == redis.Nil {
			// Queue is empty for this user.
			// Remove them from rotation.
			rdb.LRem(ctx, "users_with_pending_jobs", 0, userID)
			rdb.SRem(ctx, "active_users_set", userID)

			// Double check: If user uploaded a file JUST as we were removing them,
			// we might have a race condition where they are in queue but not in rotation.
			qLen, _ := rdb.LLen(ctx, fmt.Sprintf("queue:user:%s", userID)).Result()
			if qLen > 0 {
				if added, _ := rdb.SAdd(ctx, "active_users_set", userID).Result(); added > 0 {
					rdb.LPush(ctx, "users_with_pending_jobs", userID)
				}
			}
			continue
		} else if err != nil {
			log.Println("Error popping job:", err)
			continue
		}

		var job Job
		if err := json.Unmarshal([]byte(result), &job); err != nil {
			log.Println("Error unmarshaling job:", err)
			continue
		}

		// Track active processing
		rdb.Incr(ctx, "global:active_uploads")
		rdb.Incr(ctx, fmt.Sprintf("processing:user:%s", userID)) // Track user specific load
		
		processImage(job)
		
		rdb.Decr(ctx, "global:active_uploads")
		rdb.Decr(ctx, fmt.Sprintf("processing:user:%s", userID))
		
		os.Remove(job.Path)
	}
}

func processImage(job Job) {
	// Safety check: Image Dimensions
	f, err := os.Open(job.Path)
	if err == nil {
		cfg, _, err := image.DecodeConfig(f)
		f.Close()
		if err == nil {
			if cfg.Width > maxPixels || cfg.Height > maxPixels {
				updateFileStatus(job.UserID, job.Filename, "error", "", "", 0)
				log.Printf("Image too large: %dx%d", cfg.Width, cfg.Height)
				return
			}
		}
	}

	data, err := os.ReadFile(job.Path)
	if err != nil {
		updateFileStatus(job.UserID, job.Filename, "error", "", "", 0)
		log.Printf("error reading file %s: %v", job.Path, err)
		return
	}

	ext := filepath.Ext(job.Filename)
	lowerExt := strings.ToLower(ext)
	var img image.Image

	reader := bytes.NewReader(data)

	if lowerExt == ".dng" {
		img, err = dng.Decode(reader)
		if err != nil {
			log.Printf("dng.Decode failed for %s: %v. Retrying with generic decoder", job.Filename, err)
			reader.Seek(0, 0)
			img, _, err = image.Decode(reader)
		}
	} else if lowerExt == ".heic" || lowerExt == ".heif" {
		img, err = goheif.Decode(reader)
	} else {
		img, _, err = image.Decode(reader)
	}

	if err != nil {
		updateFileStatus(job.UserID, job.Filename, "error", "", "", 0)
		log.Printf("error decoding image %s: %v", job.Filename, err)
		return
	}

	// Use the unique filename generated at upload time, but ensure .jpg extension
	// job.Filename is already unique (e.g. 123456_myimage.png)
	baseName := job.Filename
	// Strip original extension if present to avoid double extensions like .png.jpg
	if idx := strings.LastIndex(baseName, "."); idx != -1 {
		baseName = baseName[:idx]
	}
	uniqueName := baseName + ".jpg"

	uploadPath := filepath.Join(uploadDir, uniqueName)
	out, err := os.Create(uploadPath)
	if err != nil {
		updateFileStatus(job.UserID, job.Filename, "error", "", "", 0)
		log.Printf("error creating file %s: %v", job.Filename, err)
		return
	}
	defer out.Close()

	err = jpeg.Encode(out, img, &jpeg.Options{Quality: 85})
	if err != nil {
		updateFileStatus(job.UserID, job.Filename, "error", "", "", 0)
		log.Printf("error saving image %s: %v", job.Filename, err)
		return
	}

	thumb := imaging.Resize(img, thumbnailWidth, 0, imaging.Lanczos)
	thumbPath := filepath.Join(thumbnailDir, uniqueName)
	err = imaging.Save(thumb, thumbPath, imaging.JPEGQuality(85))
	if err != nil {
		updateFileStatus(job.UserID, job.Filename, "error", "", "", 0)
		log.Printf("error creating thumbnail %s: %v", job.Filename, err)
		return
	}

	updateFileStatus(job.UserID, job.Filename, "done", "/thumbnails/"+uniqueName, "", 0)
}

func wsHandler(w http.ResponseWriter, r *http.Request) {
	userID := r.URL.Query().Get("userID")
	if userID == "" {
		http.Error(w, "UserID required", http.StatusBadRequest)
		return
	}

	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Println(err)
		return
	}
	defer conn.Close()

	mu.Lock()
	if clients[userID] == nil {
		clients[userID] = make(map[*websocket.Conn]bool)
	}
	clients[userID][conn] = true
	mu.Unlock()

	// Send initial state
	sendProgress(conn, userID)

	for {
		_, _, err := conn.ReadMessage()
		if err != nil {
			mu.Lock()
			if clients[userID] != nil {
				delete(clients[userID], conn)
				if len(clients[userID]) == 0 {
					delete(clients, userID)
				}
			}
			mu.Unlock()
			break
		}
	}
}

func broadcastToUser(userID string) {
	mu.Lock()
	defer mu.Unlock()

	userClients, ok := clients[userID]
	if !ok || len(userClients) == 0 {
		return
	}

	p := getUserProgress(userID)
	data, err := json.Marshal(p)
	if err != nil {
		log.Println(err)
		return
	}

	for client := range userClients {
		err := client.WriteMessage(websocket.TextMessage, data)
		if err != nil {
			log.Println(err)
			client.Close()
			delete(userClients, client)
		}
	}
}

func sendProgress(conn *websocket.Conn, userID string) {
	p := getUserProgress(userID)
	data, err := json.Marshal(p)
	if err != nil {
		log.Println(err)
		return
	}
	conn.WriteMessage(websocket.TextMessage, data)
}

func getUserProgress(userID string) Progress {
	total, _ := rdb.Get(ctx, fmt.Sprintf("user:%s:total", userID)).Int()
	completed, _ := rdb.Get(ctx, fmt.Sprintf("user:%s:completed", userID)).Int()
	filesMap, _ := rdb.HGetAll(ctx, fmt.Sprintf("user:%s:files", userID)).Result()

	p := Progress{
		Total:     total,
		Completed: completed,
		Files:     make(map[string]FileInfo),
	}

	for k, v := range filesMap {
		var info FileInfo
		json.Unmarshal([]byte(v), &info)
		p.Files[k] = info
	}
	return p
}

func updateFileStatus(userID, filename, status, url, originalName string, createdAt int64) {
	// We need to fetch existing info to preserve OriginalName/CreatedAt if not provided
	var info FileInfo
	existingData, err := rdb.HGet(ctx, fmt.Sprintf("user:%s:files", userID), filename).Result()
	if err == nil {
		json.Unmarshal([]byte(existingData), &info)
	}

	info.Status = status
	if url != "" {
		info.URL = url
	}
	if originalName != "" {
		info.OriginalName = originalName
	}
	if createdAt != 0 {
		info.CreatedAt = createdAt
	}

	data, _ := json.Marshal(info)

	rdb.HSet(ctx, fmt.Sprintf("user:%s:files", userID), filename, data)

	if status == "done" || status == "error" {
		rdb.Incr(ctx, fmt.Sprintf("user:%s:completed", userID))
	}

	broadcastToUser(userID)
}

// --- Stats Collection ---

func statsCollector() {
	ticker := time.NewTicker(2 * time.Second)
	for range ticker.C {
		stats := collectSystemStats()
		broadcastStats(stats)
	}
}

func collectSystemStats() SystemStats {
	var stats SystemStats

	// Redis Stats
	stats.TotalUsers, _ = rdb.SCard(ctx, "global:users_set").Result()
	stats.TotalImages, _ = rdb.Get(ctx, "global:total_images").Int64()
	totalBytes, _ := rdb.Get(ctx, "global:total_size_bytes").Float64()
	stats.TotalSizeGB = totalBytes / 1024 / 1024 / 1024
	
	// Calculate Global Queue (Total Expected - Total Completed across all users)
	var totalExpected, totalCompleted int64
	users, _ := rdb.SMembers(ctx, "global:users_set").Result()
	for _, userID := range users {
		t, _ := rdb.Get(ctx, fmt.Sprintf("user:%s:total", userID)).Int64()
		c, _ := rdb.Get(ctx, fmt.Sprintf("user:%s:completed", userID)).Int64()
		totalExpected += t
		totalCompleted += c
	}
	
	queue := totalExpected - totalCompleted
	if queue < 0 {
		queue = 0
	}
	stats.QueueLength = queue

	stats.ActiveUploads, _ = rdb.Get(ctx, "global:active_uploads").Int64()

	// System Stats
	v, _ := mem.VirtualMemory()
	if v != nil {
		stats.RAMUsed = v.Used
		stats.RAMTotal = v.Total
	}

	c, _ := cpu.Percent(0, false)
	if len(c) > 0 {
		stats.CPUUsage = c[0]
	}

	d, _ := disk.Usage("/")
	if d != nil {
		stats.DiskFree = d.Free
		stats.DiskTotal = d.Total
	}

	stats.GoVersion = runtime.Version()
	stats.NumGoroutine = runtime.NumGoroutine()
	stats.HostTotalMemory = memory.TotalMemory()
	stats.MemoryLimit = debug.SetMemoryLimit(-1) // Read current limit

	return stats
}

func broadcastStats(stats SystemStats) {
	data, err := json.Marshal(map[string]interface{}{
		"type":  "stats",
		"stats": stats,
	})
	if err != nil {
		return
	}

	mu.Lock()
	defer mu.Unlock()

	for _, userClients := range clients {
		for client := range userClients {
			client.WriteMessage(websocket.TextMessage, data)
		}
	}
}

func cleanupWorker() {
	ticker := time.NewTicker(1 * time.Hour)
	for range ticker.C {
		log.Println("Running cleanup...")
		users, err := rdb.SMembers(ctx, "global:users_set").Result()
		if err != nil {
			continue
		}

		threshold := time.Now().Add(-72 * time.Hour).UnixNano()

		for _, userID := range users {
			filesMap, err := rdb.HGetAll(ctx, fmt.Sprintf("user:%s:files", userID)).Result()
			if err != nil {
				continue
			}

			for k, v := range filesMap {
				var info FileInfo
				json.Unmarshal([]byte(v), &info)

				if info.CreatedAt < threshold && info.CreatedAt > 0 {
					// Delete physical files
					if info.URL != "" {
						baseName := filepath.Base(info.URL)
						os.Remove(filepath.Join(thumbnailDir, baseName))
						os.Remove(filepath.Join(uploadDir, baseName))
					}
					
					// Delete from Redis
					rdb.HDel(ctx, fmt.Sprintf("user:%s:files", userID), k)
					
					// Update stats (approximate)
					rdb.Decr(ctx, "global:total_images")
				}
			}
		}
	}
}