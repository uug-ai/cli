package actions

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"sync/atomic"
	"syscall"
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

func PromptInt(prompt string) int {
	for {
		if atomic.LoadInt32(&stopFlag) != 0 {
			fmt.Println("\n[info] Interrupt received, exiting prompt.")
			os.Exit(130)
		}
		fmt.Print(prompt)
		var input string
		_, err := fmt.Scanln(&input)
		if err != nil {
			return 0
		}
		if input == "" {
			return 0
		}
		var val int
		_, err = fmt.Sscanf(input, "%d", &val)
		if err == nil && val >= 0 {
			return val
		}
		fmt.Println("Enter a non-negative integer or press Enter for default.")
	}
}

func PromptString(prompt string) string {
	if atomic.LoadInt32(&stopFlag) != 0 {
		fmt.Println("\n[info] Interrupt received, exiting prompt.")
		os.Exit(130)
	}
	fmt.Print(prompt)
	var val string
	_, _ = fmt.Scanln(&val)
	return strings.TrimSpace(val)
}

func PromptBool(prompt string, def bool) bool {
	if atomic.LoadInt32(&stopFlag) != 0 {
		fmt.Println("\n[info] Interrupt received, exiting prompt.")
		os.Exit(130)
	}
	fmt.Print(prompt)
	var val string
	_, _ = fmt.Scanln(&val)
	val = strings.ToLower(strings.TrimSpace(val))
	if val == "" {
		return def
	}
	if val == "y" || val == "yes" || val == "1" || val == "true" {
		return true
	}
	if val == "n" || val == "no" || val == "0" || val == "false" {
		return false
	}
	fmt.Println("[warn] Invalid boolean input, using default.")
	return def
}

func WasFlagPassed(name string) bool {
	found := false
	flag.Visit(func(f *flag.Flag) {
		if f.Name == name {
			found = true
		}
	})
	return found
}
