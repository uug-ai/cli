package actions

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/uug-ai/models/pkg/models"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// organisations-domain-slugs copies legacy login domains (users.domain) to
// organisation slugs (RBAC spec 4.16). On multi-tenant deployments a domain
// is the tenant: the sign-in page takes it from the subdomain or its Domain
// field. The organisation now is the tenant, and its slug is its sign-in
// name, so a domain becomes the slug of the organisation that uses it.
//
// A domain is copied only when exactly one organisation uses it (its master
// accounts and their sub-accounts), it is a valid, unreserved slug, that
// organisation has a document and no slug yet, and no other organisation
// holds the slug. Everything else is reported and left alone. An operator
// settles a shared domain by naming the organisation that gets it
// (-domain with -organisation-id). users.domain itself is never changed:
// password sign-in keeps using it.

// OrganisationsDomainSlugsConfig configures the action.
type OrganisationsDomainSlugsConfig struct {
	Mode       string // dry-run (default) or live
	MongoDBURI string
	Database   string
	// Domain limits the run to one domain.
	Domain string
	// OrganisationID, with Domain, names the organisation that gets a shared
	// domain. It must be one of the organisations using the domain.
	OrganisationID string
	ReportFile     string // optional JSON report
}

// Outcomes of a domain.
const (
	domainSlugCopy           = "copy"            // copied, or copied in live mode
	domainSlugAlreadySet     = "already_set"     // the organisation's slug is the domain already
	domainSlugShared         = "shared"          // several organisations use the domain
	domainSlugInvalid        = "invalid"         // not a valid slug
	domainSlugReserved       = "reserved"        // a reserved word
	domainSlugNoOrganisation = "no_organisation" // the organisation has no document (yet)
	domainSlugHasSlug        = "has_other_slug"  // the organisation has another slug
	domainSlugTaken          = "taken"           // another organisation holds the slug
	domainSlugAmbiguousOwner = "ambiguous_owner" // a master owns several organisations, none with its id
)

// domainSlugOrganisation is what the plan knows of a master's organisation.
type domainSlugOrganisation struct {
	Id   primitive.ObjectID
	Slug string
}

// domainSlugInputs are what the plan decides from: the organisations (master
// account ids) each domain is used by, each master's organisation, and the
// slugs organisations hold.
type domainSlugInputs struct {
	// Domains maps a normalised domain to the masters whose accounts use it,
	// with the number of accounts.
	Domains map[string]map[primitive.ObjectID]int
	// Organisations maps a master to its organisation; absent when it has
	// none.
	Organisations map[primitive.ObjectID]domainSlugOrganisation
	// AmbiguousOwners are masters that own several organisations, none with
	// the master's own id.
	AmbiguousOwners map[primitive.ObjectID]bool
	// Slugs maps a slug to the organisation holding it.
	Slugs map[string]primitive.ObjectID
}

// domainSlugEntry is the decision for one domain.
type domainSlugEntry struct {
	Domain         string   `json:"domain"`
	Outcome        string   `json:"outcome"`
	OrganisationId string   `json:"organisationId,omitempty"`
	Masters        []string `json:"masters"`
	Accounts       int      `json:"accounts"`
	// Detail explains an outcome left to an operator.
	Detail  string `json:"detail,omitempty"`
	Written bool   `json:"written,omitempty"`
	Error   string `json:"error,omitempty"`
}

type domainSlugReport struct {
	Mode        string            `json:"mode"`
	Database    string            `json:"database"`
	StartedAt   string            `json:"startedAt"`
	CompletedAt string            `json:"completedAt"`
	Accounts    int               `json:"accounts"`
	Outcomes    map[string]int    `json:"outcomes"`
	Domains     []domainSlugEntry `json:"domains"`
}

// normaliseLegacyDomain lowercases and trims a domain: subdomains compare
// without case.
func normaliseLegacyDomain(domain string) string {
	return strings.ToLower(strings.TrimSpace(domain))
}

// planDomainSlugs decides each domain. chosen names, for one domain, the
// organisation (master) an operator gives a shared domain to.
func planDomainSlugs(in domainSlugInputs, onlyDomain string, chosen primitive.ObjectID) ([]domainSlugEntry, error) {
	names := make([]string, 0, len(in.Domains))
	for domain := range in.Domains {
		if onlyDomain == "" || domain == onlyDomain {
			names = append(names, domain)
		}
	}
	sort.Strings(names)
	if onlyDomain != "" && len(names) == 0 {
		return nil, fmt.Errorf("no account uses the domain %q", onlyDomain)
	}
	// A slug is given to one domain only, also within one run.
	planned := map[string]primitive.ObjectID{}
	var entries []domainSlugEntry
	for _, domain := range names {
		masters := in.Domains[domain]
		entry := domainSlugEntry{Domain: domain}
		var ids []primitive.ObjectID
		for master, accounts := range masters {
			ids = append(ids, master)
			entry.Accounts += accounts
		}
		sort.Slice(ids, func(i, j int) bool { return ids[i].Hex() < ids[j].Hex() })
		for _, id := range ids {
			entry.Masters = append(entry.Masters, id.Hex())
		}
		master := primitive.NilObjectID
		switch {
		case len(ids) == 1:
			master = ids[0]
		case !chosen.IsZero():
			if _, uses := masters[chosen]; !uses {
				return nil, fmt.Errorf("-organisation-id %s does not use the domain %q", chosen.Hex(), domain)
			}
			master = chosen
		default:
			entry.Outcome, entry.Detail = domainSlugShared, fmt.Sprintf("%d organisations use it; name one with -domain and -organisation-id", len(ids))
			entries = append(entries, entry)
			continue
		}
		if err := models.ValidateOrganisationSlugFormat(domain); err != nil {
			entry.Outcome, entry.Detail = domainSlugInvalid, err.Error()
			entries = append(entries, entry)
			continue
		}
		if models.IsReservedOrganisationSlug(domain) {
			entry.Outcome = domainSlugReserved
			entries = append(entries, entry)
			continue
		}
		if in.AmbiguousOwners[master] {
			entry.Outcome, entry.Detail = domainSlugAmbiguousOwner, "the master owns several organisations"
			entries = append(entries, entry)
			continue
		}
		organisation, found := in.Organisations[master]
		if !found {
			entry.Outcome, entry.Detail = domainSlugNoOrganisation, "created at the first sign-in; run again later"
			entries = append(entries, entry)
			continue
		}
		entry.OrganisationId = organisation.Id.Hex()
		switch {
		case organisation.Slug == domain:
			entry.Outcome = domainSlugAlreadySet
		case organisation.Slug != "":
			entry.Outcome, entry.Detail = domainSlugHasSlug, "the organisation's slug is "+organisation.Slug
		default:
			holder, taken := in.Slugs[domain]
			if other, plannedFor := planned[domain]; plannedFor {
				holder, taken = other, true
			}
			if taken && holder != organisation.Id {
				entry.Outcome, entry.Detail = domainSlugTaken, "held by organisation "+holder.Hex()
			} else {
				entry.Outcome = domainSlugCopy
				planned[domain] = organisation.Id
			}
		}
		entries = append(entries, entry)
	}
	return entries, nil
}

// OrganisationsDomainSlugs runs the action.
func OrganisationsDomainSlugs(cfg OrganisationsDomainSlugsConfig) error {
	live := strings.EqualFold(strings.TrimSpace(cfg.Mode), "live")
	if !live && strings.TrimSpace(cfg.Mode) != "" && !strings.EqualFold(strings.TrimSpace(cfg.Mode), "dry-run") {
		return fmt.Errorf("-mode must be dry-run or live, got %q", cfg.Mode)
	}
	if strings.TrimSpace(cfg.MongoDBURI) == "" || strings.TrimSpace(cfg.Database) == "" {
		return fmt.Errorf("-mongodb-uri and -mongodb-destination-database are required")
	}
	onlyDomain := normaliseLegacyDomain(cfg.Domain)
	var chosen primitive.ObjectID
	if value := strings.TrimSpace(cfg.OrganisationID); value != "" {
		if onlyDomain == "" {
			return fmt.Errorf("-organisation-id needs -domain: it names who gets one shared domain")
		}
		id, err := primitive.ObjectIDFromHex(value)
		if err != nil || id.IsZero() {
			return fmt.Errorf("-organisation-id must be the ObjectID of a master account (its organisation)")
		}
		chosen = id
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(cfg.MongoDBURI).SetServerSelectionTimeout(ServerSelectionTimeoutCheck*time.Second))
	if err != nil {
		return fmt.Errorf("connect mongo: %w", err)
	}
	defer client.Disconnect(ctx)
	db := client.Database(cfg.Database)

	report := domainSlugReport{Mode: "dry-run", Database: cfg.Database, StartedAt: time.Now().UTC().Format(time.RFC3339), Outcomes: map[string]int{}}
	if live {
		report.Mode = "live"
	}
	inputs, accounts, err := loadDomainSlugInputs(ctx, db)
	if err != nil {
		return err
	}
	report.Accounts = accounts
	entries, err := planDomainSlugs(inputs, onlyDomain, chosen)
	if err != nil {
		return err
	}
	for index := range entries {
		entry := &entries[index]
		if live && entry.Outcome == domainSlugCopy {
			if err := writeDomainSlug(ctx, db, entry); err != nil {
				entry.Error = err.Error()
			}
		}
		report.Outcomes[entry.Outcome]++
	}
	report.Domains = entries
	report.CompletedAt = time.Now().UTC().Format(time.RFC3339)

	for _, entry := range entries {
		line := fmt.Sprintf("  %-16s %-24s accounts=%d", entry.Outcome, entry.Domain, entry.Accounts)
		if entry.OrganisationId != "" {
			line += " organisation=" + entry.OrganisationId
		}
		if entry.Detail != "" {
			line += " (" + entry.Detail + ")"
		}
		if entry.Error != "" {
			line += " ERROR: " + entry.Error
		}
		fmt.Println(line)
	}
	fmt.Printf("[info] %d accounts with a domain, %d domains: %v (live=%v)\n", report.Accounts, len(entries), report.Outcomes, live)
	if cfg.ReportFile != "" {
		data, err := json.MarshalIndent(report, "", "  ")
		if err != nil {
			return err
		}
		if err := os.WriteFile(cfg.ReportFile, data, 0o600); err != nil {
			return err
		}
	}
	for _, entry := range entries {
		if entry.Error != "" {
			return errors.New("some slugs could not be written; see the report")
		}
	}
	return nil
}

// loadDomainSlugInputs reads the accounts with a domain, their masters'
// organisations and the slugs in use.
func loadDomainSlugInputs(ctx context.Context, db *mongo.Database) (domainSlugInputs, int, error) {
	in := domainSlugInputs{
		Domains:         map[string]map[primitive.ObjectID]int{},
		Organisations:   map[primitive.ObjectID]domainSlugOrganisation{},
		AmbiguousOwners: map[primitive.ObjectID]bool{},
		Slugs:           map[string]primitive.ObjectID{},
	}
	cursor, err := db.Collection("users").Find(ctx, bson.M{"domain": bson.M{"$exists": true, "$nin": bson.A{nil, ""}}},
		options.Find().SetProjection(bson.M{"_id": 1, "domain": 1, "user_id": 1}))
	if err != nil {
		return in, 0, fmt.Errorf("read users: %w", err)
	}
	accounts := 0
	masters := map[primitive.ObjectID]bool{}
	for cursor.Next(ctx) {
		var user struct {
			Id     primitive.ObjectID `bson:"_id"`
			Domain string             `bson:"domain"`
			Master string             `bson:"user_id"`
		}
		if err := cursor.Decode(&user); err != nil {
			continue
		}
		domain := normaliseLegacyDomain(user.Domain)
		if domain == "" {
			continue
		}
		master := user.Id
		if value := strings.TrimSpace(user.Master); value != "" {
			id, err := primitive.ObjectIDFromHex(value)
			if err != nil {
				continue // an orphaned sub-account: the bootstrap reports it
			}
			master = id
		}
		if in.Domains[domain] == nil {
			in.Domains[domain] = map[primitive.ObjectID]int{}
		}
		in.Domains[domain][master]++
		masters[master] = true
		accounts++
	}
	if err := cursor.Err(); err != nil {
		return in, 0, fmt.Errorf("read users: %w", err)
	}
	cursor.Close(ctx)

	ids := make([]primitive.ObjectID, 0, len(masters))
	for id := range masters {
		ids = append(ids, id)
	}
	// A master's organisation has the master's id (bootstrap); an older
	// signup organisation is found by its owner when it is the only one.
	orgs, err := db.Collection("organisation").Find(ctx, bson.M{"$or": bson.A{bson.M{"_id": bson.M{"$in": ids}}, bson.M{"ownerId": bson.M{"$in": ids}}}},
		options.Find().SetProjection(bson.M{"_id": 1, "ownerId": 1, "slug": 1}))
	if err != nil {
		return in, 0, fmt.Errorf("read organisations: %w", err)
	}
	owned := map[primitive.ObjectID][]domainSlugOrganisation{}
	for orgs.Next(ctx) {
		var organisation struct {
			Id      primitive.ObjectID `bson:"_id"`
			OwnerId primitive.ObjectID `bson:"ownerId"`
			Slug    string             `bson:"slug"`
		}
		if err := orgs.Decode(&organisation); err != nil {
			continue
		}
		value := domainSlugOrganisation{Id: organisation.Id, Slug: organisation.Slug}
		if masters[organisation.Id] {
			in.Organisations[organisation.Id] = value
		}
		if masters[organisation.OwnerId] && organisation.OwnerId != organisation.Id {
			owned[organisation.OwnerId] = append(owned[organisation.OwnerId], value)
		}
	}
	if err := orgs.Err(); err != nil {
		return in, 0, fmt.Errorf("read organisations: %w", err)
	}
	orgs.Close(ctx)
	for master, list := range owned {
		if _, own := in.Organisations[master]; own {
			continue
		}
		if len(list) == 1 {
			in.Organisations[master] = list[0]
		} else {
			in.AmbiguousOwners[master] = true
		}
	}

	slugs, err := db.Collection("organisation").Find(ctx, bson.M{"slug": bson.M{"$type": "string"}}, options.Find().SetProjection(bson.M{"_id": 1, "slug": 1}))
	if err != nil {
		return in, 0, fmt.Errorf("read slugs: %w", err)
	}
	defer slugs.Close(ctx)
	for slugs.Next(ctx) {
		var organisation struct {
			Id   primitive.ObjectID `bson:"_id"`
			Slug string             `bson:"slug"`
		}
		if err := slugs.Decode(&organisation); err == nil && organisation.Slug != "" {
			in.Slugs[organisation.Slug] = organisation.Id
		}
	}
	return in, accounts, slugs.Err()
}

// writeDomainSlug sets the slug only while the organisation still has none;
// the unique slug index refuses one another organisation took meanwhile.
func writeDomainSlug(ctx context.Context, db *mongo.Database, entry *domainSlugEntry) error {
	id, err := primitive.ObjectIDFromHex(entry.OrganisationId)
	if err != nil {
		return err
	}
	result, err := db.Collection("organisation").UpdateOne(ctx,
		bson.M{"_id": id, "slug": bson.M{"$exists": false}},
		bson.M{"$set": bson.M{"slug": entry.Domain, "audit.updatedAt": time.Now().UTC(), "audit.lastAction": "organisation.domain_slug_copied"}})
	if mongo.IsDuplicateKeyError(err) {
		return fmt.Errorf("slug taken meanwhile")
	}
	if err != nil {
		return err
	}
	if result.ModifiedCount != 1 {
		return fmt.Errorf("the organisation got a slug meanwhile")
	}
	entry.Written = true
	return nil
}
