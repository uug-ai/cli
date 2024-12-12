# UUG AI CLI

This repository contains CLI tools for performing specific automations.

- `vault-to-hub-migration`: Migrating data from a Vault database to a Hub database.

## Run

To be written

## Installation and contributing

1. Clone the repository:

   ```sh
   git clone https://github.com/uug-ai/cli.git
   cd cli
   ```

2. Install dependencies:

   ```sh
   go mod tidy
   ```

3. Run example. This will execute the `vault-to-hub-migration` action. Please have a look at the various options you can provide for each action.

   ```sh
   go run main.go -action vault-to-hub-migration \
                  ...options
   ```

## Usage

### Vault to Hub Migration

This tool migrates data from a Vault database to a Hub database.

#### Command Line Arguments

- `-action`: The action to take (required). For migration, use `vault-to-hub-migration`.
- `-mongodb-uri`: The MongoDB URI (optional if host and port are provided).
- `-mongodb-host`: The MongoDB host (optional if URI is provided).
- `-mongodb-port`: The MongoDB port (optional if URI is provided).
- `-mongodb-source-database`: The source database name (required).
- `-mongodb-destination-database`: The destination database name (required).
- `-mongodb-database-credentials`: The database credentials (optional).
- `-mongodb-username`: The MongoDB username (optional).
- `-mongodb-password`: The MongoDB password (optional).
- `-username`: The username to filter data (required).
- `-start-timestamp`: The start timestamp for filtering data (required).
- `-end-timestamp`: The end timestamp for filtering data (required).
- `-timezone`: The timezone for converting timestamps (optional, default is `UTC`).
- `-pipeline`: The pipeline to execute (optional, default is `monitor,sequence`).
- `-batch-size`: The size of each batch (optional, default is `10`).
- `-batch-delay`: The delay between batches in milliseconds (optional, default is `1000`).

#### Example

To run the Vault to Hub migration, use the following command:

```sh
go run main.go -action vault-to-hub-migration \
               -mongodb-uri "mongodb+srv://<username>:<password>@<host>/<database>?retryWrites=true&w=majority&appName=<appName>" \
               -mongodb-source-database=<sourceDatabase> \
               -mongodb-destination-database=<destinationDatabase> \
               -username <username> \
               -start-timestamp <startTimestamp> \
               -end-timestamp <endTimestamp> \
               -timezone <timezone>
```
