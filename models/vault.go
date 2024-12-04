package models

import (
	"time"

	"go.mongodb.org/mongo-driver/bson/primitive"
)

type ProviderItems struct {
	ProviderName string
	Provider     Provider
	Files        []string
}

type Provider struct {
	Id         primitive.ObjectID `json:"id" bson:"_id,omitempty"`
	Name       string             `json:"name" bson:"name"`
	Provider   string             `json:"provider" bson:"provider"`
	AccessKey  string             `json:"access_key" bson:"access_key"` // AWS
	Secret     string             `json:"secret" bson:"secret"`         // AWS
	Host       string             `json:"host" bson:"host"`             // Minio, Storj
	Region     string             `json:"region" bson:"region"`         // AWS, Minio, Storj
	Bucket     string             `json:"bucket" bson:"bucket"`         // AWS, GCP, Minio, Storj
	ServiceKey string             `json:"serviceKey" bson:"serviceKey"` // GCP
	UseSSL     string             `json:"use_ssl" bson:"use_ssl"`       // AWS, Minio, Storj
	Enabled    string             `json:"enabled" bson:"enabled"`
	Temporary  string             `json:"temporary" bson:"temporary"`
}

type Queue struct {
	Id      primitive.ObjectID `json:"id" bson:"_id,omitempty"`
	Name    string             `json:"name" bson:"name"`
	Queue   string             `json:"queue" bson:"queue"`
	Enabled string             `json:"enabled" bson:"enabled"`

	Broker    string `json:"broker" bson:"broker"`       // kafka, rabbitmq
	Group     string `json:"group" bson:"group"`         // kafka
	Username  string `json:"username" bson:"username"`   // kafka, Kerberos Vault, rabbitmq
	Password  string `json:"password" bson:"password"`   // kafka, rabbitmq
	Topic     string `json:"topic" bson:"topic"`         // kafka, SQS
	Mechanism string `json:"mechanism" bson:"mechanism"` // kafka
	Security  string `json:"security" bson:"security"`   // kafka

	Exchange string `json:"exchange" bson:"exchange"` // rabbitmq
	Host     string `json:"host" bson:"host"`         // rabbitmq
	Port     string `json:"port" bson:"port"`         // rabbitmq

	Region    string `json:"region" bson:"region"`         // AWS
	AccessKey string `json:"access_key" bson:"access_key"` // AWS, Kerberos Vault
	Secret    string `json:"secret" bson:"secret"`         // AWS, Kerberos Vault

	CloudKey string `json:"cloudkey" bson:"cloudkey"` // Kerberos Hub
	Url      string `json:"url" bson:"url"`           // Kerberos Hub, Kerberos Vault

	VaultUrl    string `json:"vault_url" bson:"vault_url"`       // Kerberos Vault
	Provider    string `json:"provider" bson:"provider"`         // Kerberos Vault
	ForwardType string `json:"forward_type" bson:"forward_type"` // Kerberos Vault

	PreviewWidth   string `json:"preview_width" bson:"preview_width"`
	PreviewEnabled string `json:"preview_enabled" bson:"preview_enabled"`

	Type string `json:"type" bson:"type"` // "queue", "forward", "webhook", etc...
}
type Media struct {
	Timestamp         int64         `json:"timestamp" bson:"timestamp"`
	FileName          string        `json:"filename" bson:"filename"`
	FileSize          int64         `json:"filesize" bson:"filesize"`
	Device            string        `json:"device" bson:"device"`
	Account           string        `json:"account" bson:"account"`
	Provider          string        `json:"provider" bson:"provider"`
	Status            string        `json:"status" bson:"status"`
	Finished          bool          `json:"finished" bson:"finished"`
	Temporary         bool          `json:"temporary" bson:"temporary"`
	Forwarded         bool          `json:"forwarded" bson:"forwarded"`
	ToBeForwarded     bool          `json:"to_be_forwarded" bson:"to_be_forwarded"`
	Uploaded          bool          `json:"uploaded" bson:"uploaded"`
	ForwarderId       string        `json:"forwarder_id" bson:"forwarder_id"`
	ForwarderType     string        `json:"forwarder_type" bson:"forwarder_type"`
	ForwarderWorker   string        `json:"forwarder_worker" bson:"forwarder_worker"`
	ForwardTimestamp  int64         `json:"forward_timestamp" bson:"forward_timestamp"`
	Events            []MediaEvent  `json:"events" bson:"events"`
	MainProvider      bool          `json:"main_provider" bson:"main_provider"`
	SecondaryProvider bool          `json:"secondary_provider" bson:"secondary_provider"`
	Metadata          MediaMetadata `json:"metadata" bson:"metadata"`
}

type MediaMetadata struct {
	BytesRanges      string             `json:"bytes_ranges" bson:"bytes_ranges"`
	BytesRangeOnTime []BytesRangeOnTime `json:"bytes_range_on_time" bson:"bytes_range_on_time"`
	IsFragmented     bool               `json:"is_fragmented" bson:"is_fragmented"`
	Duration         uint64             `json:"duration" bson:"duration"`
	Timescale        uint32             `json:"timescale" bson:"timescale"`
}

type BytesRangeOnTime struct {
	Duration string `json:"duration" bson:"duration"`
	Time     string `json:"time" bson:"time"`
	Range    string `json:"range" bson:"range"`
}

type MediaFragmentCollection struct {
	Key              string             `json:"key" bson:"key"`
	FileName         string             `json:"filename" bson:"filename"`
	CameraId         string             `json:"camera_id" bson:"camera_id"`
	Timestamp        int64              `json:"timestamp" bson:"timestamp"`
	Url              string             `json:"url" bson:"url"`
	Start            float64            `json:"start" bson:"start"`
	End              float64            `json:"end" bson:"end"`
	Duration         float64            `json:"duration" bson:"duration"`
	BytesRanges      string             `json:"bytes_ranges" bson:"bytes_ranges"`
	BytesRangeOnTime []BytesRangeOnTime `json:"bytes_range_on_time" bson:"bytes_range_on_time"`
}

type MediaEvent struct {
	Timestamp   int64  `json:"timestamp" bson:"timestamp"`
	Title       string `json:"title" bson:"title"`
	Description string `json:"description" bson:"description"`
}

type Thumbnail struct {
	Base64  string `json:"base64" bson:"base64"`
	Height  string `json:"height" bson:"height"`
	Width   string `json:"width" bson:"width"`
	Quality string `json:"quality" bson:"quality"`
}

type Account struct {
	Id                 primitive.ObjectID `json:"id" bson:"_id,omitempty"`
	Account            string             `json:"account" bson:"account"`
	Directory          string             `json:"directory" bson:"directory"` // AWS, GCP
	AccessKey          string             `json:"access_key" bson:"access_key"`
	SecretAccessKey    string             `json:"secret_access_key" bson:"secret_access_key"`
	Provider           string             `json:"provider" bson:"provider"`
	SecondaryProviders []string           `json:"secondary_providers" bson:"secondary_providers"`
	Queues             []string           `json:"queues" bson:"queues"`
	Forward            string             `json:"forward" bson:"forward"`
	EdgeAnalysis       string             `json:"edge_analysis" bson:"edge_analysis"`
	Limit              string             `json:"limit" bson:"limit"`
	Enabled            string             `json:"enabled" bson:"enabled"`
	CloudAnalysis      string             `json:"cloud_analysis" bson:"cloud_analysis"`
}

type HLSPayload struct {
	Filenames []string `json:"filenames" bson:"filenames"`
	Start     int64    `json:"start" bson:"start"`
	End       int64    `json:"end" bson:"end"`
	Devices   []string `json:"device" bson:"device"`
	Provider  string   `json:"provider" bson:"provider"`
}

type Heartbeat struct {
	Key            string `json:"key,omitempty"`
	Enterprise     bool   `json:"enterprise,omitempty"`
	Hash           string `json:"hash,omitempty"`
	Version        string `json:"version,omitempty"`
	CpuID          string `json:"cpuid,omitempty" bson:"cpuid,omitempty"`
	CloudUser      string `json:"clouduser,omitempty" bson:"clouduser,omitempty"`
	CloudPublicKey string `json:"cloudpublicKey,omitempty" bson:"cloudpublickey,omitempty"`
	CameraName     string `json:"cameraname,omitempty" bson:"cameraname,omitempty"`
	CameraType     string `json:"cameratype,omitempty" bson:"cameratype,omitempty"`
	Kubernetes     bool   `json:"kubernetes,omitempty"`
	Docker         bool   `json:"docker,omitempty"`
	Kios           bool   `json:"kios,omitempty"`
	Raspberrypi    bool   `json:"raspberrypi,omitempty"`
	Board          string `json:"board,omitempty"`
	Disk1Size      string `json:"disk1size,omitempty" bson:"disk1size,omitempty"`
	Disk3Size      string `json:"disk3size,omitempty" bson:"disk3size,omitempty"`
	DiskVDASize    string `json:"diskvdasize,omitempty" bson:"diskvdasize,omitempty"`
	NumberOfFiles  string `json:"numberofiles,omitempty" bson:"numberoffiles,omitempty"`
	Temperature    string `json:"temperature,omitempty"`
	WifiSSID       string `json:"wifissid,omitempty" bson:"wifissid,omitempty"`
	WifiStrength   string `json:"wifistrength,omitempty" bson:"wifisstrength,omitempty"`
	Uptime         string `json:"uptime,omitempty"`
	Timestamp      int64  `json:"timestamp" bson:"timestamp"`
}

type Device struct {
	Key                  string      `json:"key" bson:"key,omitempty"`
	UserId               string      `json:"user_id" bson:"user_id,omitempty"`
	Enterprise           bool        `json:"enterprise" bson:"enterprise,omitempty"`
	Status               string      `json:"status" bson:"status,omitempty"`
	Color                string      `json:"color" bson:"color,omitempty"`
	Brand                string      `json:"brand" bson:"brand,omitempty"`
	Model                string      `json:"model" bson:"model,omitempty"`
	Description          string      `json:"description" bson:"description,omitempty"`
	LatestMediaTimestamp int64       `json:"latestMediaTimestamp" bson:"latestMediaTimestamp,omitempty"`
	LastMaintenance      time.Time   `json:"lastMaintenance" bson:"lastMaintenance,omitempty"`
	InstallationDate     time.Time   `json:"installationDate" bson:"installationDate,omitempty"`
	Mute                 int64       `json:"mute" bson:"mute,omitempty"`
	LatestMedia          Media       `json:"latestMedia" bson:"latestMedia,omitempty"`
	Analytics            []Heartbeat `json:"analytics" bson:"analytics,omitempty"`
}
