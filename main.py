"""
Automated-archiving

This script will check for projs and directories within staging52/53
which has not been active for the past X months (inactive). It will then
send a Slack notification to notify the will-be-archived files

The second run of the script will start the archiving process previously
noted to-be-archive files.
It skips files tagged with 'no-archive' / 'never-archive'

"""

import os
import datetime as dt

from bin.helper import get_logger
from bin.slack import SlackClass
from bin.util import (
    read_or_new_pickle,
    dx_login,
    get_old_tar_and_notify,
    archiving_function,
    find_projs_and_notify,
    get_next_archiving_date,
)

from member.members import MEMBER_LIST

logger = get_logger(__name__)

URL_PREFIX = "https://platform.dnanexus.com/panx/projects"


if __name__ == "__main__":
    try:
        logger.info("Reading env variables")

        DEBUG = os.environ.get("ARCHIVE_DEBUG", False)
        logger.info(f"Running in DEBUG mode {DEBUG}")

        # auth token
        SLACK_TOKEN = os.environ["SLACK_TOKEN"]
        DNANEXUS_TOKEN = os.environ["DNANEXUS_TOKEN"]

        # project-ids
        PROJECT_52 = os.environ.get(
            "PROJECT_52", "project-FpVG0G84X7kzq58g19vF1YJQ"
        )
        PROJECT_53 = os.environ.get(
            "PROJECT_53", "project-FvbzbX84gG9Z3968BJjxYZ1k"
        )

        # number data envs
        MONTH2 = int(os.environ.get("AUTOMATED_MONTH_002", 6))
        MONTH3 = int(os.environ.get("AUTOMATED_MONTH_003", 3))
        TAR_MONTH = int(os.environ.get("TAR_MONTH", 3))
        ARCHIVE_MODIFIED_MONTH = int(
            os.environ.get("ARCHIVE_MODIFIED_MONTH", 1)
        )

        # file pathway envs
        ARCHIVE_PICKLE_PATH = os.environ.get(
            "AUTOMATED_ARCHIVE_PICKLE_PATH", "/monitoring/archive_dict.pickle"
        )
        ARCHIVE_FAILED_PATH = os.environ.get(
            "AUTOMATED_ARCHIVE_FAILED_PATH", "/monitoring/failed_archive.txt"
        )
        ARCHIVED_TXT_PATH = os.environ.get(
            "AUTOMATED_ARCHIVED_TXT_PATH", "/monitoring/archived.txt"
        )

        # regex envs
        AUTOMATED_REGEX_EXCLUDE = [
            text.strip()
            for text in os.environ["AUTOMATED_REGEX_EXCLUDE"].split(",")
            if text.strip()
        ]

    except KeyError as missing_env:
        logger.error(f"env {missing_env} cannot be found in config file")

        raise KeyError(f"env {missing_env} cannot be found in config file")

    # import Slack class
    slack = SlackClass(SLACK_TOKEN, TAR_MONTH, DEBUG)

    # re-define env variables for debug / testing
    if DEBUG:
        ARCHIVE_PICKLE_PATH = "/monitoring/archive_dict.test.pickle"
        ARCHIVE_FAILED_PATH = "/monitoring/failed_archive.test.txt"
        ARCHIVED_TXT_PATH = "/monitoring/archived.test.txt"

    # read pickle memory
    archive_pickle = read_or_new_pickle(ARCHIVE_PICKLE_PATH)
    to_be_archived = archive_pickle["to_be_archived"]
    staging52 = archive_pickle["staging_52"]

    today = dt.date.today()
    logger.info(today)

    if today.day in [1, 15]:
        dx_login(today, DNANEXUS_TOKEN, slack)

        if today.day == 1:
            get_old_tar_and_notify(today, TAR_MONTH, slack, PROJECT_52)

        # if there is something in memory
        # we run archive function
        # else we find_and_notify
        if to_be_archived or staging52:
            archiving_function(
                archive_pickle=archive_pickle,
                today=today,
                regex_excludes=AUTOMATED_REGEX_EXCLUDE,
                debug=DEBUG,
                archived_modified_month=ARCHIVE_MODIFIED_MONTH,
                archived_txt_path=ARCHIVED_TXT_PATH,
                archived_pickle_path=ARCHIVE_PICKLE_PATH,
                archived_failed_path=ARCHIVE_FAILED_PATH,
                slack=slack,
                project_52=PROJECT_52,
            )

            find_projs_and_notify(
                archive_pickle,
                today,
                {},
                MONTH2,
                MONTH3,
                DEBUG,
                MEMBER_LIST,
                ARCHIVE_PICKLE_PATH,
                slack,
                URL_PREFIX,
                PROJECT_52,
                PROJECT_53,
            )

        else:
            find_projs_and_notify(
                archive_pickle,
                today,
                {},
                MONTH2,
                MONTH3,
                DEBUG,
                MEMBER_LIST,
                ARCHIVE_PICKLE_PATH,
                slack,
                URL_PREFIX,
                PROJECT_52,
                PROJECT_53,
            )

    else:
        if to_be_archived or staging52:
            # if there's to-be-archived in memory
            # we do the countdown to egg-alerts
            # else we just keep silence

            next_archiving_date = get_next_archiving_date(today)
            diff = next_archiving_date - today

            slack.post_message_to_slack(
                "#egg-alerts",
                "countdown",
                today,
                days_till_archiving=diff.days,
                archiving_date=next_archiving_date,
            )
        else:
            logger.info("No data in memory")

    logger.info("End of script.")
