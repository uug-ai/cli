package actions

import (
	"strings"
	"testing"

	"go.mongodb.org/mongo-driver/bson/primitive"
)

type domainSlugFixture struct {
	in domainSlugInputs
}

func newDomainSlugFixture() *domainSlugFixture {
	return &domainSlugFixture{in: domainSlugInputs{
		Domains:         map[string]map[primitive.ObjectID]int{},
		Organisations:   map[primitive.ObjectID]domainSlugOrganisation{},
		AmbiguousOwners: map[primitive.ObjectID]bool{},
		Slugs:           map[string]primitive.ObjectID{},
	}}
}

// uses records accounts of a master under a domain; withOrganisation gives
// the master an organisation with its own id.
func (f *domainSlugFixture) uses(domain string, accounts int, withOrganisation bool) primitive.ObjectID {
	master := primitive.NewObjectID()
	if f.in.Domains[domain] == nil {
		f.in.Domains[domain] = map[primitive.ObjectID]int{}
	}
	f.in.Domains[domain][master] = accounts
	if withOrganisation {
		f.in.Organisations[master] = domainSlugOrganisation{Id: master}
	}
	return master
}

func outcomeOf(t *testing.T, entries []domainSlugEntry, domain string) domainSlugEntry {
	t.Helper()
	for _, entry := range entries {
		if entry.Domain == domain {
			return entry
		}
	}
	t.Fatalf("no entry for %q", domain)
	return domainSlugEntry{}
}

func TestDomainSlugPlan(t *testing.T) {
	f := newDomainSlugFixture()
	uat := f.uses("uat", 3, true)
	f.uses("shared", 1, true)
	f.uses("shared", 2, true)
	f.uses("https://api.example/", 1, true)
	f.uses("demo", 1, true)
	f.uses("later", 1, false)
	named := f.uses("named", 1, true)
	f.in.Organisations[named] = domainSlugOrganisation{Id: named, Slug: "other-name"}
	same := f.uses("same", 1, true)
	f.in.Organisations[same] = domainSlugOrganisation{Id: same, Slug: "same"}
	f.uses("held", 1, true)
	f.in.Slugs["held"] = primitive.NewObjectID()
	owner := f.uses("owner", 1, false)
	f.in.AmbiguousOwners[owner] = true

	entries, err := planDomainSlugs(f.in, "", primitive.NilObjectID)
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]string{
		"uat": domainSlugCopy, "shared": domainSlugShared, "https://api.example/": domainSlugInvalid,
		"demo": domainSlugReserved, "later": domainSlugNoOrganisation, "named": domainSlugHasSlug,
		"same": domainSlugAlreadySet, "held": domainSlugTaken, "owner": domainSlugAmbiguousOwner,
	}
	for domain, outcome := range want {
		if got := outcomeOf(t, entries, domain); got.Outcome != outcome {
			t.Errorf("%s: %s (%s), want %s", domain, got.Outcome, got.Detail, outcome)
		}
	}
	copied := outcomeOf(t, entries, "uat")
	if copied.OrganisationId != uat.Hex() || copied.Accounts != 3 || len(copied.Masters) != 1 {
		t.Fatalf("uat entry %+v", copied)
	}
	if shared := outcomeOf(t, entries, "shared"); shared.Accounts != 3 || len(shared.Masters) != 2 || shared.OrganisationId != "" {
		t.Fatalf("shared entry %+v", shared)
	}
	// Entries come sorted, so reports compare between runs.
	for i := 1; i < len(entries); i++ {
		if entries[i-1].Domain > entries[i].Domain {
			t.Fatal("entries not sorted")
		}
	}
}

// An operator gives a shared domain to one of the organisations using it.
func TestDomainSlugOperatorChoosesASharedDomain(t *testing.T) {
	f := newDomainSlugFixture()
	first := f.uses("shared", 1, true)
	f.uses("shared", 1, true)
	f.uses("uat", 1, true)
	entries, err := planDomainSlugs(f.in, "shared", first)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || entries[0].Outcome != domainSlugCopy || entries[0].OrganisationId != first.Hex() {
		t.Fatalf("entries %+v", entries)
	}
	if _, err := planDomainSlugs(f.in, "shared", primitive.NewObjectID()); err == nil || !strings.Contains(err.Error(), "does not use") {
		t.Fatalf("an organisation not using the domain was accepted: %v", err)
	}
	if _, err := planDomainSlugs(f.in, "missing", primitive.NilObjectID); err == nil {
		t.Fatal("an unknown domain was accepted")
	}
}

// Domains are compared without case, as subdomains are.
func TestNormaliseLegacyDomain(t *testing.T) {
	if got := normaliseLegacyDomain("  UAT "); got != "uat" {
		t.Fatalf("got %q", got)
	}
}
