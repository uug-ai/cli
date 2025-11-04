package utils

import (
	"encoding/base32"
	"math/rand"

	"regexp"
	"strings"
)

func GenerateRandomUsername(prefix string) string {
	b := make([]byte, 6)
	_, _ = rand.Read(b)
	token := strings.ToLower(base32.StdEncoding.EncodeToString(b))
	token = strings.TrimRight(token, "=")
	// keep alphanumeric only
	token = regexp.MustCompile("[^a-z0-9]").ReplaceAllString(token, "")
	if len(token) > 10 {
		token = token[:10]
	}
	if prefix == "" {
		prefix = "user-"
	}
	return prefix + token
}

func PickOne(pool []string) string {
	if len(pool) == 0 {
		return ""
	}
	return pool[rand.Intn(len(pool))]
}

func SampleUnique(pool []string, n int) []string {
	if n <= 0 || len(pool) == 0 {
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
