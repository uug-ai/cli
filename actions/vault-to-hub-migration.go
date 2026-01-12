package actions

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gosuri/uiprogress"
	"github.com/uug-ai/cli/database"
	modelsOld "github.com/uug-ai/cli/models"
	"github.com/uug-ai/cli/queue"
	"github.com/uug-ai/models/pkg/models"
)

var TIMEOUT = 10 * time.Second

func VaultToHubMigration(mode string,
	mongodbURI string,
	mongodbHost string,
	mongodbPort string,
	mongodbSourceDatabase string,
	mongodbDestinationDatabase string,
	mongodbDatabaseCredentials string,
	mongodbUsername string,
	mongodbPassword string,
	queueName string,
	vaultURLOverride string,
	username string,
	startTimestamp int64,
	endTimestamp int64,
	timezone string,
	pipeline string,
	operationCount int,
	batchSize int,
	batchDelay int,
) {

	// Translate to date time using timezone
	if timezone == "" {
		timezone = "UTC"
	}

	location, _ := time.LoadLocation(timezone)
	startTime := time.Unix(startTimestamp, 0).In(location)
	endTime := time.Unix(endTimestamp, 0).In(location)

	log.Println("====================================")
	log.Println("Configuration:")
	log.Println("  MongoDB URI:", mongodbURI)
	log.Println("  MongoDB Host:", mongodbHost)
	log.Println("  MongoDB Port:", mongodbPort)
	log.Println("  MongoDB Source Database:", mongodbSourceDatabase)
	log.Println("  MongoDB Destination Database:", mongodbDestinationDatabase)
	log.Println("  MongoDB Database Credentials:", mongodbDatabaseCredentials)
	log.Println("  MongoDB Username:", mongodbUsername)
	log.Println("  MongoDB Password:", "************")
	log.Println("  Queue:", queueName)
	log.Println("  Username:", username)
	log.Println("  Start Time", startTime)
	log.Println("  End Time", endTime)
	log.Println("  Pipeline", pipeline)
	log.Println("====================================")
	fmt.Println("")

	fmt.Println(" >> Please wait while we migrate the data. Press Ctrl+C to stop the process.")
	var steps = []string{
		"MongoDB: verify connection",
		"MongoDB: open connection",
		"Vault: query media",
		"Hub: get user",
		"Hub: query media",
		"Vault: get queue connection",
		"Queue: connect",
		"Vault to Hub: calculating delta",
		"Vault to Hub: delta complete",
	}
	bar := uiprogress.AddBar(len(steps)).AppendCompleted().PrependElapsed()
	bar.PrependFunc(func(b *uiprogress.Bar) string {
		return steps[b.Current()-1]
	})
	time.Sleep(time.Second * 2)
	uiprogress.Start() // start rendering

	// #####################################
	//
	// Step 1: Verify the connection to MongoDB
	// Verify the connection to MongoDB
	//
	bar.Incr()
	if mongodbURI == "" {
		// If no URI is provided, use the host/port
		if mongodbHost == "" || mongodbPort == "" {
			log.Println("Please provide a valid MongoDB URI or host/port")
			return
		}
	}
	time.Sleep(time.Second * 2)

	// #####################################
	//
	// Step 2: Connect to MongoDB
	// Connect to MongoDB
	//
	bar.Incr()
	var db *database.DB
	if mongodbURI != "" {
		db = database.NewMongoDBURI(mongodbURI)
	} else {
		db = database.NewMongoDBHost(mongodbHost, mongodbPort, mongodbDatabaseCredentials, mongodbUsername, mongodbPassword)
	}
	client := db.Client
	defer client.Disconnect(context.Background())
	time.Sleep(time.Second * 2)

	// #####################################
	//
	// Step 3: Get the media from the vault database
	// Get all the media from a specific user between two timestamps
	//
	bar.Incr()
	vaultMedia := database.GetMediaOfUserFromMongodb(client, mongodbSourceDatabase, username, startTimestamp, endTimestamp)
	accounts := database.GetAccountsFromMongodb(client, mongodbSourceDatabase)
	accountByName := make(map[string]modelsOld.Account, len(accounts))
	for _, account := range accounts {
		accountByName[account.Account] = account
	}
	time.Sleep(time.Second * 2)

	// #####################################
	//
	// Step 4: Get the user from the hub database
	// Get the user from the hub database
	//
	bar.Incr()
	user := database.GetUserFromMongodb(client, mongodbDestinationDatabase, username)
	if user.Username == "" {
		log.Println("Aborting: specified user not found in the hub database")
		return
	}
	pipelineStages := parsePipelineStages(pipeline)
	analysisRequested := stageInList("analysis", pipelineStages)
	var pipelineUser models.User
	var pipelineSubscription models.Subscription
	var pipelinePlans map[string]interface{}
	if analysisRequested {
		pipelineUser = database.GetPipelineUserFromMongodb(client, mongodbDestinationDatabase, username)
		if pipelineUser.Username == "" {
			log.Println("Warning: unable to load pipeline user for analysis-only events")
		}
		pipelineSubscription = database.GetActiveSubscriptionFromMongodb(client, mongodbDestinationDatabase, user.Id.Hex())
		pipelinePlans = database.GetPlansFromMongodb(client, mongodbDestinationDatabase, pipelineUser)
	}
	time.Sleep(time.Second * 2)

	// #####################################
	//
	// Step 5: Get the sequences from the hub database
	// Get the sequences from the user.
	//
	bar.Incr()
	userId := user.Id.Hex()
	sequences := database.GetSequencesFromMongodb(client, mongodbDestinationDatabase, userId, startTimestamp, endTimestamp)
	// Iterate over the sequences and media and look for differences.
	var hubMedia []modelsOld.Media
	for _, sequence := range sequences {
		hubMedia = append(hubMedia, sequence.Images...)
	}

	// #####################################
	//
	// Step 6: Retrieve the selected queue and the connection settings
	// Select the queue from the vault database
	//
	bar.Incr()
	queueFound := false
	var selectedQueue modelsOld.Queue
	queues := database.GetActiveQueuesFromMongodb(client, mongodbSourceDatabase)
	for _, q := range queues {
		if q.Name == queueName {
			queueFound = true
			selectedQueue = q
			break
		}
	}
	if !queueFound {
		log.Println("Aborting: specified queue was not found in the vault database")
		return
	}

	// #####################################
	//
	// Step 7: Connect to the queue service
	// Connect to the queue service
	//
	bar.Incr()
	queueService, err := queue.CreateQueue(selectedQueue.Queue, selectedQueue)
	if err != nil {
		log.Printf("Aborting: failed to connect to queue %s: %v\n", selectedQueue.Queue, err)
		return
	}

	// #####################################
	//
	// Step 7: Compare the media in the vault with the media in the hub
	// Now we will verify if the media in the vault is in the hub.
	//
	bar.Incr()
	var delta []modelsOld.VaultMedia

	// Transform the hub media to a map for faster lookup
	hubMediaMap := make(map[string]bool)
	for _, media := range hubMedia {
		hubMediaMap[media.Key] = true
	}

	vaultMediaByKey := make(map[string]modelsOld.VaultMedia, len(vaultMedia))
	for _, vMedia := range vaultMedia {
		vaultMediaByKey[vMedia.FileName] = vMedia
	}

	// Iterate over the vault media and check if it is in the hub
	for _, vMedia := range vaultMedia {
		// Check if media is a mp4
		if vMedia.FileName[len(vMedia.FileName)-4:] != ".mp4" {
			continue
		}
		if _, ok := hubMediaMap[vMedia.FileName]; !ok {
			delta = append(delta, vMedia)
		}
	}

	var missingAnalysis []modelsOld.VaultMedia
	if analysisRequested {
		analysisIDSet := make(map[string]bool)
		sequenceKeySet := make(map[string]bool)
		for _, sequence := range sequences {
			for _, image := range sequence.Images {
				if image.Key != "" {
					sequenceKeySet[image.Key] = true
				}
				analysisID := strings.TrimSpace(image.AnalysisID)
				if analysisID == "" {
					continue
				}
				analysisIDSet[analysisID] = true
			}
		}
		sequenceKeys := make([]string, 0, len(sequenceKeySet))
		for key := range sequenceKeySet {
			sequenceKeys = append(sequenceKeys, key)
		}
		analysisIDs := make([]string, 0, len(analysisIDSet))
		for analysisID := range analysisIDSet {
			analysisIDs = append(analysisIDs, analysisID)
		}
		analysisByIDMap := database.AnalysisInfoByIDs(client, mongodbDestinationDatabase, userId, analysisIDs)
		analysisByKeyMap := database.AnalysisInfoByKeys(client, mongodbDestinationDatabase, userId, sequenceKeys)
		missingAnalysisMap := make(map[string]bool)
		for _, sequence := range sequences {
			for _, image := range sequence.Images {
				analysisID := strings.TrimSpace(image.AnalysisID)
				if analysisID != "" {
					if info, ok := analysisByIDMap[analysisID]; ok {
						if operationCount > 0 && info.ResolvedCount < operationCount {
							database.DeleteAnalysisByID(client, mongodbDestinationDatabase, userId, analysisID)
						} else {
							continue
						}
					}
				}
				if info, ok := analysisByKeyMap[image.Key]; ok {
					if operationCount > 0 && info.ResolvedCount < operationCount {
						if info.ID != "" {
							database.DeleteAnalysisByID(client, mongodbDestinationDatabase, userId, info.ID)
						} else {
							database.DeleteAnalysisByKey(client, mongodbDestinationDatabase, userId, image.Key)
						}
					} else {
						continue
					}
				}
				if missingAnalysisMap[image.Key] {
					continue
				}
				vMedia, ok := vaultMediaByKey[image.Key]
				if !ok {
					log.Printf("Warning: sequence media not found in vault media list: %s", image.Key)
					continue
				}
				missingAnalysis = append(missingAnalysis, vMedia)
				missingAnalysisMap[image.Key] = true
			}
		}
	}
	if analysisRequested && len(missingAnalysis) > 0 {
		if pipelineUser.Id.IsZero() || pipelineUser.Email == "" {
			log.Println("Warning: missing user details for analysis pipeline; skipping analysis-only events")
			missingAnalysis = nil
		}
	}

	// #####################################
	//
	// Step 8: Send the delta to the queue
	// Send the delta to the queue

	bar.Incr()
	if len(delta) == 0 && len(missingAnalysis) == 0 {
		log.Println("No media to transfer")
		return
	}

	var barProgressMedia *uiprogress.Bar
	if len(delta) > 0 {
		barProgressMedia = uiprogress.AddBar(len(delta)).AppendCompleted().PrependElapsed()
		barProgressMedia.PrependFunc(func(b *uiprogress.Bar) string {
			return "Transferring media"
		})
		uiprogress.Start()
	}
	var barProgressAnalysis *uiprogress.Bar
	if len(missingAnalysis) > 0 {
		barProgressAnalysis = uiprogress.AddBar(len(missingAnalysis)).AppendCompleted().PrependElapsed()
		barProgressAnalysis.PrependFunc(func(b *uiprogress.Bar) string {
			return "Forwarding to analysis"
		})
		uiprogress.Start()
	}

	batch := 0
	vaultURL := strings.TrimSpace(vaultURLOverride)
	if vaultURL == "" {
		vaultURL = selectedQueue.VaultUrl
	}
	if vaultURL == "" {
		log.Printf("Warning: queue %s has no vault_url configured; signed URLs cannot be generated", selectedQueue.Name)
	}
	for _, media := range delta {
		var event models.PipelineEvent

		if len(pipelineStages) > 0 {
			event.Stages = pipelineStages
		} else {
			event.Stages = []string{"monitor", "sequence"}
		}

		event.Storage = "kstorage"
		event.Request = "persist"
		event.Provider = media.Provider
		event.Operation = "event"
		event.Timestamp = media.Timestamp

		// Get instance name from media filename
		instanceName, regionCoordinates, numberOfChanges, duration := parseMediaAttributes(media.FileName)
		if duration == "" && media.Metadata.Duration > 0 {
			duration = strconv.FormatUint(media.Metadata.Duration, 10)
		}

		timestampString := strconv.FormatInt(media.Timestamp, 10)

		signedURL := ""
		if vaultURL != "" {
			account := accountByName[media.Account]
			if account.AccessKey == "" && selectedQueue.AccessKey != "" && selectedQueue.Secret != "" {
				account = modelsOld.Account{
					AccessKey:       selectedQueue.AccessKey,
					SecretAccessKey: selectedQueue.Secret,
				}
			}
			signedURLErr := error(nil)
			signedURL, signedURLErr = getSignedURLFromVault(vaultURL, account, media.FileName, media.Provider)
			if signedURLErr != nil {
				log.Printf("Warning: unable to fetch signed url for %s: %v", media.FileName, signedURLErr)
			} else {
				log.Printf("%s", signedURL)
			}
		}

		fileSize := media.FileSize
		if fileSize == 0 {
			fileSize = 1
		}

		event.Payload = models.PipelinePayload{
			Timestamp:        media.Timestamp,
			FileName:         media.FileName,
			FileSize:         fileSize,
			Duration:         duration,
			SignedURL:        signedURL,
			IsFragmented:     media.Metadata.IsFragmented,
			BytesRanges:      media.Metadata.BytesRanges,
			BytesRangeOnTime: convertBytesRangeOnTime(media.Metadata.BytesRangeOnTime),
			Metadata: models.PipelineMetadata{
				Timestamp:         timestampString,
				Duration:          duration,
				UploadTime:        timestampString,
				DeviceId:          media.Device,
				DeviceName:        instanceName,
				RegionCoordinates: regionCoordinates,
				NumberOfChanges:   numberOfChanges,
			},
		}

		if mode == "dry-run" {
			// Nothing to do here..
			log.Printf("Dry-run: would send event for %s", media.FileName)

		} else if mode == "live" {
			if !queueFound {
				log.Println("Queue not found")
				return
			} else {
				e, err := json.Marshal(event)
				if err == nil {
					queueService.SendMessage(selectedQueue.Topic, string(e), 0)
				} else {
					log.Println("Error marshalling the event")
				}
			}
		}

		time.Sleep(time.Millisecond * 100)
		if barProgressMedia != nil {
			barProgressMedia.Incr()
		}

		// Specify the batch size and delay
		batch++
		if batch%batchSize == 0 {
			time.Sleep(time.Millisecond * time.Duration(batchDelay))
		}
	}

	if analysisRequested && len(missingAnalysis) > 0 {
		analysisStages := sliceStagesFrom("analysis", pipelineStages)
		monitorStage := buildMonitorStage(pipelineUser, pipelineSubscription, pipelinePlans)
		for _, media := range missingAnalysis {
			var event models.PipelineEvent

			event.Stages = analysisStages
			event.Storage = "kstorage"
			event.Request = "persist"
			event.Provider = media.Provider
			event.Operation = "event"
			event.Timestamp = media.Timestamp
			event.TraceId = generateTraceID()
			event.MonitorStage = &monitorStage

			instanceName, regionCoordinates, numberOfChanges, duration := parseMediaAttributes(media.FileName)
			if duration == "" && media.Metadata.Duration > 0 {
				duration = strconv.FormatUint(media.Metadata.Duration, 10)
			}

			timestampString := strconv.FormatInt(media.Timestamp, 10)

			signedURL := ""
			if vaultURL != "" {
				account := accountByName[media.Account]
				if account.AccessKey == "" && selectedQueue.AccessKey != "" && selectedQueue.Secret != "" {
					account = modelsOld.Account{
						AccessKey:       selectedQueue.AccessKey,
						SecretAccessKey: selectedQueue.Secret,
					}
				}
				signedURLErr := error(nil)
				signedURL, signedURLErr = getSignedURLFromVault(vaultURL, account, media.FileName, media.Provider)
				if signedURLErr != nil {
					log.Printf("Warning: unable to fetch signed url for %s: %v", media.FileName, signedURLErr)
				} else {
					log.Printf("%s", signedURL)
				}
			}

			fileSize := media.FileSize
			if fileSize == 0 {
				fileSize = 1
			}

			event.Payload = models.PipelinePayload{
				Timestamp:        media.Timestamp,
				FileName:         media.FileName,
				FileSize:         fileSize,
				Duration:         duration,
				SignedURL:        signedURL,
				IsFragmented:     media.Metadata.IsFragmented,
				BytesRanges:      media.Metadata.BytesRanges,
				BytesRangeOnTime: convertBytesRangeOnTime(media.Metadata.BytesRangeOnTime),
				Metadata: models.PipelineMetadata{
					Timestamp:         timestampString,
					Duration:          duration,
					UploadTime:        timestampString,
					DeviceId:          media.Device,
					DeviceName:        instanceName,
					RegionCoordinates: regionCoordinates,
					NumberOfChanges:   numberOfChanges,
				},
			}

			if mode == "dry-run" {
				log.Printf("Dry-run: would send analysis event for %s", media.FileName)
			} else if mode == "live" {
				e, err := json.Marshal(event)
				if err == nil {
					queueService.SendMessage("kcloud-analysis-queue", string(e), 0)
				} else {
					log.Println("Error marshalling the event")
				}
			}

			time.Sleep(time.Millisecond * 100)
			if barProgressAnalysis != nil {
				barProgressAnalysis.Incr()
			}
			batch++
			if batch%batchSize == 0 {
				time.Sleep(time.Millisecond * time.Duration(batchDelay))
			}
		}
	}

	queueService.Close()
	if barProgressMedia != nil {
		barProgressMedia.Set(100)
	}
	if barProgressAnalysis != nil {
		barProgressAnalysis.Set(100)
	}
	bar.Set(100)

	// #####################################
	//
	// Printout a table with the media that was transferred
	//

	log.Println("")
	log.Println(">>Media transferred:")
	log.Println("")
	log.Println("  +---------------------------------------------------------------------------------------+-----------------+-----------------+-------------------------------------+")
	log.Println("  | File Name                                                                             | File Size       | Timestamp       | Device                              |")
	log.Println("  +---------------------------------------------------------------------------------------+-----------------+-----------------+-------------------------------------+")
	for _, media := range delta {
		log.Printf("  | %-85s | %-15d | %-15d | %-35s |\n", media.FileName, media.FileSize, media.Timestamp, media.Device)
	}
	log.Println("  +---------------------------------------------------------------------------------------+-----------------+-----------------+-------------------------------------+")
	log.Println("")
	if len(missingAnalysis) > 0 {
		log.Println(">>Media forwarded to analysis:")
		log.Println("")
		log.Println("  +---------------------------------------------------------------------------------------+-----------------+-----------------+-------------------------------------+")
		log.Println("  | File Name                                                                             | File Size       | Timestamp       | Device                              |")
		log.Println("  +---------------------------------------------------------------------------------------+-----------------+-----------------+-------------------------------------+")
		for _, media := range missingAnalysis {
			log.Printf("  | %-85s | %-15d | %-15d | %-35s |\n", media.FileName, media.FileSize, media.Timestamp, media.Device)
		}
		log.Println("  +---------------------------------------------------------------------------------------+-----------------+-----------------+-------------------------------------+")
		log.Println("")
	}
	log.Println("Migration completed.")
}

func parseMediaAttributes(fileName string) (string, string, string, string) {
	parts := strings.Split(fileName, "/")
	if len(parts) < 2 {
		return "", "", "", ""
	}
	file := strings.TrimSuffix(parts[1], ".mp4")
	fileParts := strings.Split(file, "_")
	if len(fileParts) < 6 {
		return "", "", "", ""
	}
	return fileParts[2], fileParts[3], fileParts[4], fileParts[5]
}

func convertBytesRangeOnTime(ranges []modelsOld.BytesRangeOnTime) []models.FragmentedBytesRangeOnTime {
	if len(ranges) == 0 {
		return nil
	}
	converted := make([]models.FragmentedBytesRangeOnTime, 0, len(ranges))
	for _, r := range ranges {
		converted = append(converted, models.FragmentedBytesRangeOnTime{
			Duration: r.Duration,
			Time:     r.Time,
			Range:    r.Range,
		})
	}
	return converted
}

func parsePipelineStages(pipeline string) []string {
	pipeline = strings.TrimSpace(pipeline)
	if pipeline == "" {
		return nil
	}
	parts := strings.Split(pipeline, ",")
	stages := make([]string, 0, len(parts))
	for _, part := range parts {
		stage := strings.TrimSpace(part)
		if stage == "" {
			continue
		}
		stages = append(stages, stage)
	}
	return stages
}

func stageInList(stage string, stages []string) bool {
	for _, s := range stages {
		if s == stage {
			return true
		}
	}
	return false
}

func sliceStagesFrom(stage string, stages []string) []string {
	for i, s := range stages {
		if s == stage {
			return stages[i:]
		}
	}
	return []string{stage}
}

func generateTraceID() string {
	bytes := make([]byte, 16)
	if _, err := rand.Read(bytes); err != nil {
		return strconv.FormatInt(time.Now().UnixNano(), 10)
	}
	return hex.EncodeToString(bytes)
}

func buildMonitorStage(user models.User, subscription models.Subscription, plans map[string]interface{}) models.MonitorStage {
	stage := models.NewMonitorStage()
	stage.User = user
	stage.Subscription = subscription
	if plans != nil {
		stage.Plans = plans
	} else {
		stage.Plans = map[string]interface{}{}
	}
	return stage
}

func getSignedURLFromVault(vaultURL string, account modelsOld.Account, fileName string, provider string) (string, error) {
	if vaultURL == "" {
		return "", errors.New("vault url missing")
	}
	if account.AccessKey == "" || account.SecretAccessKey == "" {
		return "", errors.New("vault access key/secret missing")
	}
	if provider == "" {
		return "", errors.New("provider missing")
	}
	endpoint := strings.TrimRight(vaultURL, "/")
	if strings.HasSuffix(endpoint, "/api") {
		endpoint = endpoint + "/storage"
	} else {
		endpoint = endpoint + "/api/storage"
	}

	req, err := http.NewRequest("GET", endpoint, nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Kerberos-Storage-FileName", fileName)
	req.Header.Set("X-Kerberos-Storage-Provider", provider)
	req.Header.Set("X-Kerberos-Storage-AccessKey", account.AccessKey)
	req.Header.Set("X-Kerberos-Storage-SecretAccessKey", account.SecretAccessKey)

	client := &http.Client{Timeout: TIMEOUT}
	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	var response struct {
		Data string `json:"data"`
	}
	if err := json.Unmarshal(bodyBytes, &response); err != nil {
		return "", fmt.Errorf("vault response decode error: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		bodyString := strings.TrimSpace(string(bodyBytes))
		if bodyString == "" {
			bodyString = "empty body"
		}
		return "", fmt.Errorf("vault returned status %d: %s", resp.StatusCode, bodyString)
	}
	if response.Data == "" {
		return "", errors.New("vault response missing data")
	}
	return response.Data, nil
}
