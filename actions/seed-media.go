package actions

import (
	"context"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// Pools (static)
var (
	DETECTION_POOL = []string{
		"animal", "pedestrian", "cyclist", "motorbike", "lorry", "car", "handbag", "suitcase", "cell phone",
	}
	TAG_POOL = []string{
		"outdoor", "indoor", "evening", "sunny", "crowd", "single-subject", "normal", "rainy", "night", "vehicle",
		"urban", "rural", "busy", "quiet", "sports", "event", "construction", "park", "school", "shopping", "office",
		"residential", "traffic", "festival", "emergency", "public-transport", "parking-lot", "playground", "market",
		"bridge", "tunnel",
	}
	COLOR_POOL = []string{"red", "blue", "green", "gray", "black", "white", "yellow"}
	VIDEO_POOL = []string{
		"demo/1751987393_3-641_falcon_420-234-408-321_397_29896.mp4",
		"demo/1751987410_3-505_dublin_1596-648-78-118_1105_26520.mp4",
		"demo/1751987440_3-425_dublin_1594-708-57-29_1252_29880.mp4",
		"demo/1751987476_3-482_nashville_1134-654-205-45_691_30440.mp4",
		"demo/1751987663_3-913_falcon_622-257-301-322_7130_29897.mp4",
		"demo/1751987924_3-818_nashville_651-649-688-332_9458_30394.mp4",
	}
)

var stopFlag int32

func HandleSignals() {
	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-c
		fmt.Println("\n[signal] Received interrupt, stopping after current batches...")
		atomic.StoreInt32(&stopFlag, 1)
	}()
}

func SampleUnique(pool []string, n int) []string {
	if n <= 0 {
		return []string{}
	}
	if n > len(pool) {
		n = len(pool)
	}
	perm := rand.Perm(len(pool))
	out := make([]string, n)
	for i := 0; i < n; i++ {
		out[i] = pool[perm[i]]
	}
	return out
}

func BuildBatchDocs(n int, now int64) []interface{} {
	docs := make([]interface{}, 0, n)
	for i := 0; i < n; i++ {
		st := now - int64(rand.Intn(3600))
		en := st + int64(rand.Intn(21)+5)
		doc := bson.M{
			"_id":             primitive.NewObjectID(),
			"startTimestamp":  st,
			"endTimestamp":    en,
			"duration":        en - st,
			"deviceId":        fmt.Sprintf("DEV-%06d", rand.Intn(5001)),
			"organisationId":  fmt.Sprintf("ORG-%03d", rand.Intn(100)+1),
			"storageSolution": "kstorage",
			"videoProvider":   "azure-production",
			"videoFile":       VIDEO_POOL[rand.Intn(len(VIDEO_POOL))],
			"analysisId":      fmt.Sprintf("AN-%06d", rand.Intn(5001)),
			"description":     "synthetic media sample for load test",
			"detections":      SampleUnique(DETECTION_POOL, rand.Intn(4)+1),
			"dominantColors":  SampleUnique(COLOR_POOL, rand.Intn(3)+1),
			"count":           rand.Intn(11) - 5,
			"tags":            SampleUnique(TAG_POOL, rand.Intn(4)+1),
			"metadata": bson.M{
				"tags":            SampleUnique(TAG_POOL, rand.Intn(3)+1),
				"classifications": []string{"normal_activity"},
			},
		}
		docs = append(docs, doc)
	}
	return docs
}

func InsertBatch(ctx context.Context, col *mongo.Collection, docs []interface{}) int {
	if len(docs) == 0 {
		return 0
	}
	_, err := col.InsertMany(ctx, docs, options.InsertMany().SetOrdered(false))
	if err != nil {
		fmt.Printf("[warn] batch error: %v\n", err)
		return 0
	}
	return len(docs)
}

func CreateIndexes(ctx context.Context, col *mongo.Collection) {
	indexes := []mongo.IndexModel{
		{Keys: bson.D{{Key: "startTimestamp", Value: 1}}},
		{Keys: bson.D{{Key: "deviceId", Value: 1}}},
		{Keys: bson.D{{Key: "organisationId", Value: 1}}},
		{Keys: bson.D{{Key: "tags", Value: 1}}},
		{Keys: bson.D{{Key: "detections", Value: 1}}},
		{Keys: bson.D{{Key: "duration", Value: 1}}},
	}
	_, err := col.Indexes().CreateMany(ctx, indexes)
	if err != nil {
		fmt.Printf("[info] index creation skipped: %v\n", err)
	}
}

func PromptInt(prompt string) int {
	var val int
	for {
		fmt.Print(prompt)
		_, err := fmt.Scanln(&val)
		if err == nil && val > 0 {
			return val
		}
		fmt.Println("Please enter a positive integer.")
	}
}

func PromptString(prompt string) string {
	var val string
	for {
		fmt.Print(prompt)
		_, err := fmt.Scanln(&val)
		if err == nil && val != "" {
			return val
		}
		fmt.Println("Please enter a non-empty value.")
	}
}

func SeedMedia() {
	rand.Seed(time.Now().UnixNano())
	HandleSignals()

	var (
		target      = flag.Int("target", 0, "Total documents to insert (required)")
		batchSize   = flag.Int("batch", 0, "Documents per batch (required)")
		parallel    = flag.Int("parallel", 0, "Concurrent batch workers (required)")
		uri         = flag.String("uri", "", "MongoDB URI (required)")
		dbName      = flag.String("db", "", "Database name (required)")
		collName    = flag.String("collection", "", "Collection name (required)")
		noIndex     = flag.Bool("no-index", false, "Skip index creation")
		reportEvery = flag.Int("report-every", 10, "Report progress every N batches")
	)
	flag.Parse()

	// Interactive prompt for missing required flags
	if *target <= 0 {
		*target = PromptInt("Enter total documents to insert (--target): ")
	}
	if *batchSize <= 0 {
		*batchSize = PromptInt("Enter documents per batch (--batch): ")
	}
	if *parallel <= 0 {
		*parallel = PromptInt("Enter concurrent batch workers (--parallel): ")
	}
	if *uri == "" {
		*uri = PromptString("Enter MongoDB URI (--uri): ")
	}
	if *dbName == "" {
		*dbName = PromptString("Enter database name (--db): ")
	}
	if *collName == "" {
		*collName = PromptString("Enter collection name (--collection): ")
	}

	ctx := context.Background()
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(*uri).SetServerSelectionTimeout(10*time.Second))
	if err != nil {
		fmt.Printf("[error] MongoDB connect: %v\n", err)
		os.Exit(1)
	}
	col := client.Database(*dbName).Collection(*collName)

	if !*noIndex {
		CreateIndexes(ctx, col)
	}

	var (
		totalInserted int64
		batchCounter  int64
		startTime     = time.Now()
		wg            sync.WaitGroup
		batchCh       = make(chan []interface{}, *parallel*2)
	)

	fmt.Printf("[start] target=%d batch=%d parallel=%d uri=%s db=%s.%s\n",
		*target, *batchSize, *parallel, *uri, *dbName, *collName)

	// Workers
	for i := 0; i < *parallel; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for docs := range batchCh {
				if atomic.LoadInt32(&stopFlag) != 0 {
					return
				}
				inserted := InsertBatch(ctx, col, docs)
				atomic.AddInt64(&totalInserted, int64(inserted))
			}
		}()
	}

mainLoop:
	for atomic.LoadInt64(&totalInserted) < int64(*target) && atomic.LoadInt32(&stopFlag) == 0 {
		remaining := int64(*target) - atomic.LoadInt64(&totalInserted)
		current := *batchSize
		if remaining < int64(*batchSize) {
			current = int(remaining)
		}
		now := time.Now().Unix()
		docs := BuildBatchDocs(current, now)
		batchCh <- docs
		atomic.AddInt64(&batchCounter, 1)

		if atomic.LoadInt64(&batchCounter)%int64(*reportEvery) == 0 {
			elapsed := time.Since(startTime).Seconds()
			rate := float64(atomic.LoadInt64(&totalInserted)) / elapsed
			pct := 100 * float64(atomic.LoadInt64(&totalInserted)) / float64(*target)
			fmt.Printf("[progress] batches=%d inserted=%d (%.2f%%) rate=%.0f/s\n",
				atomic.LoadInt64(&batchCounter), atomic.LoadInt64(&totalInserted), pct, rate)
		}
		if atomic.LoadInt32(&stopFlag) != 0 {
			break mainLoop
		}
	}
	close(batchCh)
	wg.Wait()

	elapsed := time.Since(startTime).Seconds()
	rate := float64(atomic.LoadInt64(&totalInserted)) / elapsed
	fmt.Printf("[done] inserted=%d elapsed=%.2fs rate=%.0f/s stop_flag=%v\n",
		atomic.LoadInt64(&totalInserted), elapsed, rate, atomic.LoadInt32(&stopFlag) != 0)
}
