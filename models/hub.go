package models

import (
	"time"

	"go.mongodb.org/mongo-driver/bson/primitive"
)

type User struct {
	Id           primitive.ObjectID `json:"id" bson:"_id,omitempty"`
	Username     string             `json:"username" bson:"username"`
	Email        string             `json:"email" bson:"email"`
	StripePlan   string             `json:"stripe_plan" bson:"stripe_plan"`
	IsActive     int                `json:"isActive" bson:"isActive"`
	Reachedlimit bool               `json:"reachedlimit" bson:"reachedlimit"`
	Devices      []interface{}      `json:"devices" bson:"devices"`
	CreatedAt    time.Time          `json:"created_at" bson:"created_at"`
	Subscription UserSubscription   `json:"subscription" bson:"subscription"`
}

type UserSubscription struct {
	Id         primitive.ObjectID `json:"id" bson:"_id,omitempty"`
	Name       string             `json:"name" bson:"name"`
	UserId     string             `json:"user_id" bson:"user_id"`
	StripeId   string             `json:"stripe_id" bson:"stripe_id"`
	StripePlan string             `json:"stripe_plan" bson:"stripe_plan"`
	Quantity   int                `json:"quantity" bson:"isActive"`
	EndsAt     time.Time          `json:"ends_at" bson:"ends_at"`
	CreatedAt  time.Time          `json:"created_at" bson:"created_at"`
}

type Settings struct {
	Key string                 `json:"key" bson:"key"`
	Map map[string]interface{} `json:"map" bson:"map"`
}

type SubscriptionSettings struct {
	Key string                  `json:"key" bson:"key"`
	Map map[string]Subscription `json:"map" bson:"map"`
}

type Subscription struct {
	Level         int `json:"level" bson:"level"`
	Usage         int `json:"usage" bson:"usage"`
	UploadLimit   int `json:"uploadLimit" bson:"uploadLimit"`
	VideoLimit    int `json:"videoLimit" bson:"videoLimit"`
	AnalysisLimit int `json:"analysisLimit" bson:"analysisLimit"`
	DayLimit      int `json:"dayLimit" bson:"dayLimit"`
}

type MediaWrapper struct {
	Time         string   `json:"time" bson:"time"`
	Timestamp    int64    `json:"timestamp" bson:"timestamp"`
	Description  string   `json:"description" bson:"description"`
	Index        int      `json:"index" bson:"index"`
	InstanceName string   `json:"instanceName" bson:"instanceName"`
	Labels       []string `json:"labels" bson:"labels"`
	Properties   []string `json:"properties" bson:"properties"`
	MetaData     Media    `json:"metadata" bson:"metadata"`
	FileUrl      string   `json:"src" bson:"src"`
	ThumbnailUrl string   `json:"thumbnailUrl" bson:"thumbnailUrl"`
	SpriteUrl    string   `json:"spriteUrl" bson:"spriteUrl"`
	Type         string   `json:"type" bson:"type"`
	Vault        bool     `json:"vault" bson:"vault"`
	TaskCreated  bool     `json:"task_created" bson:"task_created"`
	Persisted    bool     `json:"persisted" bson:"persisted"`
}

type Media struct {
	Key               string   `json:"key" bson:"key"`
	Path              string   `json:"path" bson:"path"`
	Url               string   `json:"src" bson:"src"`
	Timestamp         int64    `json:"timestamp" bson:"timestamp"`
	Date              string   `json:"date" bson:"date"`
	Time              string   `json:"time" bson:"time"`
	InstanceName      string   `json:"instanceName" bson:"instanceName"`
	Type              string   `json:"type" bson:"type"`
	Provider          string   `json:"provider" bson:"provider"`
	Source            string   `json:"source" bson:"source"`
	CameraId          string   `json:"camera_id" bson:"camera_id"`
	TaskCreated       bool     `json:"task_created" bson:"task_created"`
	Persisted         bool     `json:"persisted" bson:"persisted"`
	Properties        []string `json:"properties" bson:"properties"`
	AnalysisID        string   `json:"analysis_id" bson:"analysis_id"`
	Thumbnail         string   `json:"thumbnail" bson:"thumbnail"`
	ThumbnailFile     string   `json:"thumbnailFile" bson:"thumbnailFile"`
	ThumbnailProvider string   `json:"thumbnailProvider" bson:"thumbnailProvider"`
	SpriteFile        string   `json:"spriteFile" bson:"spriteFile"`
	SpriteProvider    string   `json:"spriteProvider" bson:"spriteProvider"`
	SpriteInterval    int      `json:"spriteInterval" bson:"spriteInterval"`
	Dominantcolor     []string `json:"dominantcolor" bson:"dominantcolor"`
}

type MediaShort struct {
	Key               string `json:"key" bson:"key"`
	Path              string `json:"path" bson:"path"`
	Url               string `json:"src" bson:"src"`
	Timestamp         int64  `json:"timestamp" bson:"timestamp"`
	CameraId          string `json:"camera_id" bson:"camera_id"`
	Date              string `json:"date" bson:"date"`
	Time              string `json:"time" bson:"time"`
	Type              string `json:"type" bson:"type"`
	Provider          string `json:"provider" bson:"provider"`
	Source            string `json:"source" bson:"source"`
	SpriteUrl         string `json:"sprite_url" bson:"sprite_url"`
	ThumbnailUrl      string `json:"thumbnail_url" bson:"thumbnail_url"`
	ThumbnailFile     string `json:"thumbnailFile" bson:"thumbnailFile"`
	ThumbnailProvider string `json:"thumbnailProvider" bson:"thumbnailProvider"`
	SpriteFile        string `json:"spriteFile" bson:"spriteFile"`
	SpriteProvider    string `json:"spriteProvider" bson:"spriteProvider"`
	SpriteInterval    int    `json:"spriteInterval" bson:"spriteInterval"`
}

type Filter struct {
	LastMedia       int64     `json:"lastMedia" bson:"lastMedia"`
	GlobalSearch    bool      `json:"globalSearch" bson:"globalSearch"`
	Dates           []string  `json:"dates" bson:"dates"`
	Instances       []string  `json:"instances" bson:"instances"`
	Regions         []Regions `json:"regions" bson:"regions"`
	Classifications []string  `json:"classifications" bson:"classifications"`
	Sort            string    `json:"sort" bson:"sort"`
	Favourite       bool      `json:"favourite" bson:"favourite"`
	HasLabel        bool      `json:"hasLabel" bson:"hasLabel"`
	HourRange       HourRange `json:"hourRange" bson:"online"`
	ViewStyle       string    `json:"viewStyle" bson:"viewStyle"`
	Offset          int64     `json:"offset" bson:"offset"`
	Limit           int64     `json:"limit" bson:"limit"`
}

type Regions struct {
	Id           string  `json:"id" bson:"id"`
	Device       string  `json:"device" bson:"device"`
	Width        int     `json:"width" bson:"width"`
	Height       int     `json:"height" bson:"height"`
	RegionPoints []Point `json:"regionPoints" bson:"regionPoints"`
}

type Point struct {
	X float64 `json:"x" bson:"x"`
	Y float64 `json:"y" bson:"y"`
}

type HourRange struct {
	Start int64 `json:"start" bson:"start"`
	End   int64 `json:"end" bson:"end"`
}

type Sequences struct {
	Id       primitive.ObjectID `json:"id" bson:"_id,omitempty"`
	Start    int64              `json:"start" bson:"start"`
	End      int64              `json:"end" bson:"end"`
	UserId   string             `json:"user_id" bson:"user_id"`
	Images   []Media            `json:"images" bson:"images"`
	Notified bool               `json:"notified" bson:"notified"`
	Star     bool               `json:"star" bson:"star"`
	Devices  []string           `json:"devices" bson:"devices"`
}

type SequencesWithMedia struct {
	Id        primitive.ObjectID `json:"id" bson:"_id,omitempty"`
	Start     int64              `json:"start" bson:"start"`
	StartDate string             `json:"startDate" bson:"startDate"`
	StartTime string             `json:"startTime" bson:"startTime"`
	End       int64              `json:"end" bson:"end"`
	EndDate   string             `json:"endDate" bson:"endDate"`
	EndTime   string             `json:"endTime" bson:"endTime"`
	UserId    string             `json:"user_id" bson:"user_id"`
	Images    []MediaWrapper     `json:"images" bson:"images"`
	Notified  bool               `json:"notified" bson:"notified"`
	Star      bool               `json:"star" bson:"star"`
	Devices   []string           `json:"devices" bson:"devices"`
}

// Case labels
type Label struct {
	Id          primitive.ObjectID `json:"id" bson:"_id,omitempty,omitempty"`
	Name        string             `json:"name" bson:"name,omitempty"`
	Description string             `json:"description" bson:"description,omitempty"`
	Color       string             `json:"color" bson:"color,omitempty"`
	UserId      string             `json:"user_id" bson:"user_id,omitempty"`       // Log which user created the label
	OwnerId     string             `json:"owner_id" bson:"owner_id,omitempty"`     // Log which master account the user belongs to, used to group labels
	CreatedAt   int64              `json:"created_at" bson:"created_at,omitempty"` // For sorting purposes
	IsPrivate   bool               `json:"is_private" bson:"is_private"`           // Could be available on only private tasks in future
	Types       []string           `json:"type" bson:"type,omitempty"`             // Could be used to group labels by tasks, media, alerts,...
}
