package actions

import "fmt"

func SeedUsers(
	prefix string,
	count int,
	mongodbURI string,
	dbName string,
	userCollName string,
	subscriptionCollName string,
	settingsCollName string,
) {
	if WasFlagPassed("mongodb-uri") {
		fmt.Printf("[info] using flag -mongodb-uri = %s\n", mongodbURI)
	} else {
		input := PromptString("MongoDB URI (default 'mongodb://localhost:27017'): ")
		if input != "" {
			mongodbURI = input
			fmt.Printf("[info] using input -mongodb-uri = %s\n", mongodbURI)
		} else if mongodbURI == "" {
			mongodbURI = "mongodb://localhost:27017"
			fmt.Printf("[info] using default -mongodb-uri = %s\n", mongodbURI)
		}
	}

	// --- db name ---
	if WasFlagPassed("db-name") {
		fmt.Printf("[info] using flag -db-name = %s\n", dbName)
	} else {
		fmt.Printf("[info] using default -db-name = %s\n", dbName)
	}

	// --- user collection ---
	if WasFlagPassed("user-collection") {
		fmt.Printf("[info] using flag -user-collection = %s\n", userCollName)
	} else {
		fmt.Printf("[info] using default -user-collection = %s\n", userCollName)
	}

	// --- settings collection ---
	if WasFlagPassed("settings-collection") {
		fmt.Printf("[info] using flag -settings-collection = %s\n", settingsCollName)
	} else {
		fmt.Printf("[info] using default -settings-collection = %s\n", settingsCollName)
	}

	// --- subscription collection ---
	if WasFlagPassed("subscription-collection") {
		fmt.Printf("[info] using flag -subscription-collection = %s\n", subscriptionCollName)
	} else {
		fmt.Printf("[info] using default -subscription-collection = %s\n", subscriptionCollName)
	}

	// --- user prefix ---
	if WasFlagPassed("user-prefix") {
		fmt.Printf("[info] using flag -user-prefix = %s\n", prefix)
	} else {
		input := PromptString("User prefix (default 'user'): ")
		if input != "" {
			prefix = input
			fmt.Printf("[info] using input -user-prefix = %s\n", prefix)
		} else if input == "" {
			prefix = "user"
			fmt.Printf("[info] using default -user-prefix = %s\n", prefix)
		}
	}

	// --- user count ---
	if WasFlagPassed("user-count") {
		if count < 1 || count > 10000000 {
			fmt.Printf("[error] -user-count must be between 1 and 10000000, got %d\n", count)
			count = 100
			fmt.Printf("[info] using default -user-count = %d\n", count)
		} else {
			fmt.Printf("[info] using flag -user-count = %d\n", count)
		}
	} else {
		input := PromptInt("Number of users to create (default 100): ")
		if input != 0 {
			if input < 1 || input > 10000000 {
				fmt.Printf("[error] user count must be between 1 and 10000000, got %d\n", input)
				count = 100
				fmt.Printf("[info] using default -user-count = %d\n", count)
			} else {
				count = input
				fmt.Printf("[info] using input -user-count = %d\n", count)
			}
		} else if count == 0 {
			count = 100
			fmt.Printf("[info] using default -user-count = %d\n", count)
		}
	}

	fmt.Printf("[info] Seeding %d users with prefix '%s'...\n", count, prefix)

}
