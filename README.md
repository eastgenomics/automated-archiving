# Automated Archiving

#### python v3.8.17

## What does the script do
Check for 002, 003 projects and directories in staging52 which are not modified for the last X months (inactive). Compile all archivable into a list and send Slack notification to notify all to-be-archived files in the next run. 

Tag `no-archive` or `never-archive` to skip archiving.

## Typical use case
Monthly check for archivable projects or directories on DNANexus & send Slack notification

## Archive Pickle
The script generates a pickle file at location specified at `AUTOMATED_ARCHIVE_PICKLE_PATH`. 

This acts as the memory of the script to remember to-be-archived projects and files

## Member
The script requires `members.py` in `member` folder on the server (`/member/members.py`)

The `.py` file should have a `MEMBER_LIST` (dict) which contain key `DNANexus Username` - value `Slack Username`. An example `members.py` is included in repo

## Script Workflow
1. Check today's date.
2. If 1st or 15th, checks for to-be-archived projects in memory 
    - if yes, run archiving, then run `find_projects` & `find_precision_projects`
    - if no, run `find_projects` & `find_precision_projects`
3. If not 1st or 15th, sends countdown message to Slack

![script workflow](demo/script_workflow_updated.png)

## Example Notification

#### 003 Slack Notification
![notification](demo/003_demo.png)

#### tar.gz Slack Notification
![tar notification](demo/tar_files_demo.png)

## Environment Variables Required
#### dnanexus
- `DNANEXUS_TOKEN` : DNANexus API Token
- `PROJECT_52` : staging52 project-id
- `PROJECT_53` : staging53 project-id
#### general envs
- `AUTOMATED_MONTH_002` : period (in months) before being marked for archiving (e.g. 6) for 002 projects
- `AUTOMATED_MONTH_003` : period (in months) before being marked for archiving (e.g. 3) for 003 projects
- `AUTOMATED_ARCHIVE_PICKLE_PATH` : pickle file (memory) directory pathway
- `AUTOMATED_ARCHIVED_TXT_PATH` : directory to output txt file listing all archived projects & directories
- `AUTOMATED_ARCHIVE_FAILED_PATH`: path to store txt file containing all file-id that failed archiving
- `TAR_MONTH`: period (in months) for `tar.gz` being inactive to be considered 'old enough' (only used by `get_old_tar_and_notify` function)
- `ARCHIVE_MODIFIED_MONTH`: period (in months) to determine whether to skip archiving if project or file is modified within this month (e.g. 1)
- `ARCHIVE_DEBUG`: env to comment out actionable codes (e.g. tag file, remove file tag, archive)
- `AUTOMATED_REGEX_EXCLUDE`: comma-separated regex word e.g. megaqc.json,some-filename\..*,^megapc.csv
- `PRECISION_ARCHIVING`: comma separated project-id that need specific archiving (folder by folder archiving)
#### slack
- `SLACK_TOKEN` : Slack Bot API Token

## Logging
The main logging script is `helper.py`

The script will generate a log file `automated-archiving.log` in `/monitoring`

## Tags
There are 3 tags recognized by the script:
- `no-archive`
- `never-archive`
- `archive`

#### #no-archive
Projects tagged will temporarily bypass archiving. 

For directories in Staging52, if one file within a directory (`/210202_A12905_003`) is tagged, the whole directory will temporarily bypass archiving. 

The tag will be removed if remain inactive for X months (`MONTH_002`)

#### #never-archive
Projects tagged will bypass archiving indefintely, same goes to any directory within staging52.

#### #archive
Tagged project or directory will be listed for archiving, regardless of modified date

## Tagging Function
Script will check each project (002, 003) and add archival status tags:
- `fully archived`
- `partial archived`

## Output file
The script will generate a txt file `archived.txt` at the location specified at `AUTOMATED_ARCHIVED_TXT_PATH`. 

The text file contains all the archived project-id and directories in `staging52`

## Docker
`Dockerfile` is included for rebuilding docker image

To rebuild image: `docker build -t <image name> .`

Current docker command (server):

```docker run --env-file <config.file> -v /var/log/monitoring:/monitoring: -v /member:/member <image> --datetime 20231001```

- optional argument `datetime` to override script datetime

## Automation
A cron job can be set up to run the script on 1st and 15th of each month
