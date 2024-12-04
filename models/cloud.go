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
