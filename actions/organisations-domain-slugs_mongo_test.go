package actions

import (
	"context"
	"strings"
	"testing"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo/integration/mtest"
)

// The slug is set only while the organisation has none; a slug another
// organisation took meanwhile, or one this organisation got meanwhile, is an
// error in the report, never an overwrite.
func TestWriteDomainSlugOnlyFillsAnEmptySlug(t *testing.T) {
	mt := mtest.New(t, mtest.NewOptions().ClientType(mtest.Mock))
	for name, test := range map[string]struct {
		response bson.D
		err      string
	}{
		"written":          {mtest.CreateSuccessResponse(bson.E{Key: "n", Value: 1}, bson.E{Key: "nModified", Value: 1}), ""},
		"got a slug":       {mtest.CreateSuccessResponse(bson.E{Key: "n", Value: 0}, bson.E{Key: "nModified", Value: 0}), "meanwhile"},
		"taken by another": {mtest.CreateWriteErrorsResponse(mtest.WriteError{Index: 0, Code: 11000, Message: "E11000 duplicate key error"}), "taken meanwhile"},
	} {
		mt.Run(name, func(mt *mtest.T) {
			mt.AddMockResponses(test.response)
			entry := domainSlugEntry{Domain: "uat", OrganisationId: primitive.NewObjectID().Hex()}
			err := writeDomainSlug(context.Background(), mt.DB, &entry)
			if test.err == "" {
				if err != nil || !entry.Written {
					t.Fatalf("err %v written %v", err, entry.Written)
				}
				filter := mt.GetStartedEvent().Command.Lookup("updates").Array().Index(0).Value().Document().Lookup("q").Document()
				if _, err := filter.LookupErr("slug", "$exists"); err != nil {
					t.Fatalf("update does not require an absent slug: %s", filter)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), test.err) || entry.Written {
				t.Fatalf("err %v written %v", err, entry.Written)
			}
		})
	}
}
