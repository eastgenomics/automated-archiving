# Automated Archiving

## What does the script do
Check for 002, 003 projects, directories in staging52 and staging53 which are not modified for the last X months (inactive). Compile all archivable into a list and send Slack notification to notify all to-be-archived files in the next run. Tag `no-archive` to skip archiving.

## Typical use case
Monthly check for archivable projects or directories on DNANexus & send Slack notification

## Configs required
A config file (txt) with variables:
- `DNANEXUS_TOKEN` : DNANexus API Token
- `SLACK_TOKEN` : Slack Bot API Token
- `PROJECT_52` : project-id
- `PROJECT_53` : project-id
- `AUTOMATED_MONTH` : Inactivty period (e.g. 4/5/6)
- `AUTOMATED_ARCHIVE_PICKLE_PATH` : pickle file directory
- `AUTOMATED_ARCHIVED_TXT_PATH` : directory to output txt file listing all archived projects & directories

## Logging
The main logging script is `helper.py`

The script will generate a log file `automated-archiving.log` in `/var/log/monitoring`

## Output file
The script will generate a txt file `archived.txt` at the location `AUTOMATED_ARCHIVED_TXT_PATH`. The text file list all the archived project-id, directories in `staging52` and `staging53`

## Docker
`Dockerfile` is included for rebuilding docker image

To rebuild image: `docker build -t <image name> .`

Current tested command (local):

```docker run --env-file <config.txt> -v ~/github/automated_archiving/var:/var <image name> ```

## Automation
A cron job will be set up to run the script every month
