# Automated Archiving

## What does the script do
Check for 002, 003 projects and directories in staging52 which are not modified for the last X months (inactive). Compile all archivable into a list and send Slack notification to notify all to-be-archived files in the next run. Tag `no-archive` or `never-archive` to skip archiving.

## Typical use case
Monthly check for archivable projects or directories on DNANexus & send Slack notification

## Archive Pickle
The script generates a pickle file at location specified at `AUTOMATED_ARCHIVE_PICKLE_PATH`. This acts as the memory of the script to remember to-be-archived projects and files

## Script Workflow
When the script is executed, it checks if today is 1st or 15th of the month, if it is, it check for files in memory (to_be_archived, staging52). 

If `today.day == 1`: It checks for old enough `tar.gz` in staging52 and send Slack notification

If there is 'to-be-archived' in memory, it runs the archiving function

If there is nothing in the memory, it proceeds to find 'archivable' projects (find_projs_and_notify) and send Slack notification

If today is not 1st or 15th, it checks for the next run date and send a message to Slack (`egg-alerts`)
```
archive_pickle = read_or_new_pickle(ARCHIVE_PICKLE_PATH)
to_be_archived = archive_pickle['to_be_archived']
staging52 = archive_pickle['staging_52']

if today.day in [1, 15]:
    if today.day == 1: get_old_tar_and_notify()
    
    if to_be_archived or staging52:
        archiving_function(archive_pickle)
    else:
        find_projs_and_notify(archive_pickle)
```

![script workflow](demo/script_workflow_updated.png)

## Example Notification

#### 003 Slack Notification
![notification](demo/003_demo.png)

#### tar.gz Slack Notification
![tar notification](demo/tar_files_demo.png)

## Configs required
A config file (txt) with variables:
- `DNANEXUS_TOKEN` : DNANexus API Token
- `SLACK_TOKEN` : Slack Bot API Token
- `PROJECT_52` : staging52 project-id
- `PROJECT_53` : staging53 project-id
- `AUTOMATED_MONTH_002` : Period of file being inactive after which to archive (months) for 002 projects & generally
- `AUTOMATED_MONTH_003` : Period of file being inactive after which to archive (months) for 003 projects
- `AUTOMATED_ARCHIVE_PICKLE_PATH` : pickle file directory
- `AUTOMATED_ARCHIVED_TXT_PATH` : directory to output txt file listing all archived projects & directories
- `ANSIBLE_SERVER`: (for sending helpdesk email) server host
- `ANSIBLE_PORT`: (for sending helpdesk email) server port
- `SENDER`: (for sending helpdesk email) BioinformaticsTeamGeneticsLab@addenbrookes.nhs.uk
- `RECEIVERS`: (for sending helpdesk email) emails separated by comma (e.g. abc.domain,bbc.domain)
- `TAR_MONTH`: Period of tar.gz being inactive to be considered 'old enough' (only used by `get_old_tar_and_notify` function)
- `ARCHIVE_MODIFIED_MONTH`: During archiving_function, if file if modified in the last `ARCHIVE_MODIFIED_MONTH` month, we skip archiving it

## Logging
The main logging script is `helper.py`

The script will generate a log file `automated-archiving.log` in `/var/log/monitoring`

## Tags
There are 3 tags recognized by the script:
- `no-archive`
- `never-archive`
- `archive`

#### no-archive
Projects tagged will temporarily bypass archiving. 

For directories in staging52, if one file within a directory (`/210202_A12905_003`) is tagged, the whole directory will temporarily bypass archiving. 

The tag will be removed if remain inactive for X months (`MONTH_002`)

#### never-archive
Projects tagged will bypass archiving indefintely, same goes to any directory within staging52.

#### archive
Tagged project or directory will be listed for archiving, regardless of modified date


## Output file
The script will generate a txt file `archived.txt` at the location specified at `AUTOMATED_ARCHIVED_TXT_PATH`. 

The text file contains all the archived project-id and directories in `staging52`

## Docker
`Dockerfile` is included for rebuilding docker image

To rebuild image: `docker build -t <image name> .`

Current tested command (local):

```docker run --env-file <config.txt> -v /var/log:/var/log <image name> ```

## Automation
A cron job will be set up to run the script on 1st and 15th of each month
