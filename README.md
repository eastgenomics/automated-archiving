# Automated Archiving

## What does the script do
Check for 002, 003 projects and directories in staging52 which are not modified for the last X months (inactive). Compile all archivable into a list and send Slack notification to notify all to-be-archived files in the next run. 

Tag `no-archive` or `never-archive` to skip archiving.

## Typical use case
Monthly check for archivable projects or directories on DNANexus & send Slack notification

## Archive Pickle
The script generates a pickle file at location specified at `AUTOMATED_ARCHIVE_PICKLE_PATH`. 

This acts as the memory of the script to remember to-be-archived projects and files

## Member
The script requires `members.py` in `member` folder on the server. 

The `.py` file should have a `MEMBER_LIST` (dict) which contain key `DNANexus Username` - value `Slack Username`

## Script Workflow
1. Check today's date.
2. If it's 1st or 15th, it checks for files in memory (tar.gz function only runs on 1st of each month).
3. If not 1st or 15th, it sends a countdown message to Slack
4. If there're files in memory, proceed with `archiving_function`
5. If nothing in memory, proceed with `find_proj_and_notify function`

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
- `AUTOMATED_MONTH_002` : Period of file being inactive after which to archive (months) for 002 projects & generally
- `AUTOMATED_MONTH_003` : Period of file being inactive after which to archive (months) for 003 projects
- `AUTOMATED_ARCHIVE_PICKLE_PATH` : pickle file directory
- `AUTOMATED_ARCHIVED_TXT_PATH` : directory to output txt file listing all archived projects & directories
- `TAR_MONTH`: Period of tar.gz being inactive to be considered 'old enough' (only used by `get_old_tar_and_notify` function)
- `ARCHIVE_MODIFIED_MONTH`: During archiving_function, if file if modified in the last `ARCHIVE_MODIFIED_MONTH` month, we skip archiving it
- `ARCHIVE_DEBUG`: (exist or comment out) if TRUE, comment out actionable codes (e.g. tag file, remove file tag, archive)
- `AUTOMATED_REGEX_EXCLUDE`: comma-separated regex word e.g. megaqc.json,some-filename\..*,^megapc.csv
#### slack
- `SLACK_TOKEN` : Slack Bot API Token
#### server
- `ANSIBLE_SERVER`: (for sending helpdesk email) server host
- `ANSIBLE_PORT`: (for sending helpdesk email) server port
- `SENDER`: (for sending helpdesk email) BioinformaticsTeamGeneticsLab@addenbrookes.nhs.uk
- `RECEIVERS`: (for sending helpdesk email) emails separated by comma (e.g. abc.domain,bbc.domain)

## Logging
The main logging script is `helper.py`

The script will generate a log file `automated-archiving.log` in `/var/log/monitoring`

## Tags
There are 3 tags recognized by the script:
- `no-archive`
- `never-archive`
- `archive`

#### #no-archive
Projects tagged will temporarily bypass archiving. 

For directories in staging52, if one file within a directory (`/210202_A12905_003`) is tagged, the whole directory will temporarily bypass archiving. 

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

```docker run --env-file <config.file> -v /var/log/monitoring:/var/log/monitoring:z -v /home/lingj-loc/member:/member <image>```

## Automation
A cron job will be set up to run the script on 1st and 15th of each month
