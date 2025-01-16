# uug-ai/cli

This repository contains CLI tools for performing specific automations.

- `vault-to-hub-migration`: Migrating data from a Vault database to a Hub database.

## Run

You can run the migration as a Kubernetes job in your cluster. The benefit is that you do not need to expose anything, and use the internal Kubernetes dns.

```sh
kubectl apply -f kubernetes-job.yaml
```

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
- `-queue`: The integration used to transfer events to the hub pipeline.
- `-start-timestamp`: The start timestamp for filtering data (required).
- `-end-timestamp`: The end timestamp for filtering data (required).
- `-timezone`: The timezone for converting timestamps (optional, default is `UTC`).
- `-pipeline`: The pipeline to execute (optional, default is `monitor,sequence`).
- `-batch-size`: The size of each batch (optional, default is `10`).
- `-batch-delay`: The delay between batches in milliseconds (optional, default is `1000`).
- `-mode`: You can choose to run a `dry-run` or `live`.

#### Example

To run the Vault to Hub migration, use the following command:

```sh
go run main.go -action vault-to-hub-migration \
               -mongodb-uri "mongodb+srv://<username>:<password>@<host>/<database>?retryWrites=true&w=majority&appName=<appName>" \
               -mongodb-source-database=<sourceDatabase> \
               -mongodb-destination-database=<destinationDatabase> \
               -queue <rabbitmq-integration> \
               -username <username> \
               -start-timestamp <startTimestamp> \
               -end-timestamp <endTimestamp> \
               -timezone <timezone> \
               -pipeline 'monitor,sequence,analysis' \
               -mode 'dry-run' \
               -batch-size 100 \
               -batch-delay 1000
```

#### Output

            _    _ _    _  _____     _____ _ _
            | |  | | |  | |/ ____|   / ____(_) |
            | |  | | |  | | |  __   | |     _| |
            | |  | | |  | | | |_ |  | |    | | |
            | |__| | |__| | |__| |  | |____| | |
            \____/ \____/ \_____|   \_____|_|_|


      Starting Vault to Hub migration...
      2024/12/12 09:37:39 ====================================
      2024/12/12 09:37:39 Configuration:
      2024/12/12 09:37:39   MongoDB URI: mongodb+srv://xxxx
      2024/12/12 09:37:39   MongoDB Host:
      2024/12/12 09:37:39   MongoDB Port:
      2024/12/12 09:37:39   MongoDB Source Database: KerberosStorage
      2024/12/12 09:37:39   MongoDB Destination Database: Kerberos
      2024/12/12 09:37:39   MongoDB Database Credentials:
      2024/12/12 09:37:39   MongoDB Username:
      2024/12/12 09:37:39   MongoDB Password: ************
      2024/12/12 09:37:39   Queue: rabbitmq-xxxx
      2024/12/12 09:37:39   Username: xxxx
      2024/12/12 09:37:39   Start Time 2024-04-01 08:47:40 +0200 CEST
      2024/12/12 09:37:39   End Time 2025-04-06 17:41:00 +0200 CEST
      2024/12/12 09:37:39   Pipeline monitor,sequence,analysis
      2024/12/12 09:37:39 ====================================

      >> Please wait while we migrate the data. Press Ctrl+C to stop the process.
      Vault to Hub: delta complete   42s [====================================================================] 100%
      Transferring media    1s [====================================================================] 100%
      2024/12/12 09:38:26
      2024/12/12 09:38:26 >>Media transferred:
      2024/12/12 09:38:26
      2024/12/12 09:38:26   +---------------------------------------------------------------------------------------+-----------------+-----------------+-------------------------------------+
      2024/12/12 09:38:26   | File Name                                                                             | File Size       | Timestamp       | Device                              |
      2024/12/12 09:38:26   +---------------------------------------------------------------------------------------+-----------------+-----------------+-------------------------------------+
      2024/12/12 09:38:26   | xxxxxxxx/1733992176_6-967003_melle-insidegarage_200-200-400-400_25819_769.mp4         | 7261430         | 1733992211      | melle-insidegarage                  |
      2024/12/12 09:38:26   | xxxxxxxx/1733991780_6-967003_melle-garage_200-200-400-400_256_769.mp4                 | 14206272        | 1733991818      | melle-garage                        |
      2024/12/12 09:38:26   | xxxxxxxx/1733991781_6-967003_melle-street_200-200-400-400_819_769.mp4                 | 3603494         | 1733991817      | melle-street                        |
      2024/12/12 09:38:26   | xxxxxxxx/1733991587_6-967003_gb-side_200-200-400-400_342_769.mp4                      | 8167534         | 1733991611      | vSQBjrhqGGOXseLoidIBFhKeJjCjTM      |
      2024/12/12 09:38:26   | xxxxxxxx/1733991547_6-967003_gb-side_200-200-400-400_883_769.mp4                      | 12197049        | 1733991586      | vSQBjrhqGGOXseLoidIBFhKeJjCjTM      |
      2024/12/12 09:38:26   | xxxxxxxx/1733991501_6-967003_gb-side_200-200-400-400_1472_769.mp4                     | 12130152        | 1733991538      | vSQBjrhqGGOXseLoidIBFhKeJjCjTM      |
      2024/12/12 09:38:26   | xxxxxxxx/1733990945_6-967003_gb-side_200-200-400-400_6547_769.mp4                     | 7097803         | 1733990966      | vSQBjrhqGGOXseLoidIBFhKeJjCjTM      |
      2024/12/12 09:38:26   | xxxxxxxx/1733990903_6-967003_gb-side_200-200-400-400_9313_769.mp4                     | 7709073         | 1733990928      | vSQBjrhqGGOXseLoidIBFhKeJjCjTM      |
      2024/12/12 09:38:26   | xxxxxxxx/1733990868_6-967003_gb-frontdoor_200-200-400-400_158_769.mp4                 | 1882747         | 1733990885      | lQtymLrehWpHkTavfcNTFgwfDMoSfg      |
      2024/12/12 09:38:26   | xxxxxxxx/1733990742_6-967003_gb-side_200-200-400-400_214198_769.mp4                   | 7015592         | 1733990765      | vSQBjrhqGGOXseLoidIBFhKeJjCjTM      |
      2024/12/12 09:38:26   | xxxxxxxx/1733990698_6-967003_gb-frontdoor_200-200-400-400_155_769.mp4                 | 1487304         | 1733990713      | lQtymLrehWpHkTavfcNTFgwfDMoSfg      |
      2024/12/12 09:38:26   | xxxxxxxx/1733990683_6-967003_gb-frontdoor_200-200-400-400_177_769.mp4                 | 1966309         | 1733990701      | lQtymLrehWpHkTavfcNTFgwfDMoSfg      |
      2024/12/12 09:38:26   | xxxxxxxx/1733990499_6-967003_gb-frontdoor_200-200-400-400_176_769.mp4                 | 3430326         | 1733990530      | lQtymLrehWpHkTavfcNTFgwfDMoSfg      |
      2024/12/12 09:38:26   | xxxxxxxx/1733990314_6-967003_gb-frontdoor_200-200-400-400_168_769.mp4                 | 3667828         | 1733990347      | lQtymLrehWpHkTavfcNTFgwfDMoSfg      |
      2024/12/12 09:38:26   | xxxxxxxx/1733990130_6-967003_gb-frontdoor_200-200-400-400_153_769.mp4                 | 3839485         | 1733990165      | lQtymLrehWpHkTavfcNTFgwfDMoSfg      |
      2024/12/12 09:38:26   | xxxxxxxx/1733989945_6-967003_gb-frontdoor_200-200-400-400_151_769.mp4                 | 3590120         | 1733989979      | lQtymLrehWpHkTavfcNTFgwfDMoSfg      |
      2024/12/12 09:38:26   | xxxxxxxx/1733989761_6-967003_gb-frontdoor_200-200-400-400_164_769.mp4                 | 3730173         | 1733989794      | lQtymLrehWpHkTavfcNTFgwfDMoSfg      |
      2024/12/12 09:38:26   | xxxxxxxx/1733989573_6-967003_gb-frontdoor_200-200-400-400_154_769.mp4                 | 3878792         | 1733989606      | lQtymLrehWpHkTavfcNTFgwfDMoSfg      |
      2024/12/12 09:38:26   | xxxxxxxx/1733989397_6-967003_gb-frontdoor_200-200-400-400_166_769.mp4                 | 3846902         | 1733989430      | lQtymLrehWpHkTavfcNTFgwfDMoSfg      |
      2024/12/12 09:38:26   | xxxxxxxx/1733989379_6-967003_gb-frontdoor_200-200-400-400_162_769.mp4                 | 2107887         | 1733989400      | lQtymLrehWpHkTavfcNTFgwfDMoSfg      |
      2024/12/12 09:38:26   +---------------------------------------------------------------------------------------+-----------------+-----------------+-------------------------------------+
