import os
import datetime as dt

from bin.helper import get_logger
from bin.slack import SlackClass
from bin.util import (
    read_or_new_pickle,
    dx_login,
    get_old_tar_and_notify,
)
from bin.archiving import (
    archiving_function,
    find_projects_and_notify,
    get_next_archiving_date,
)


from member.members import MEMBER_LIST

logger = get_logger(__name__)

URL_PREFIX: str = "https://platform.dnanexus.com/panx/projects"


if __name__ == "__main__":
    # env imports
    try:
        logger.info("Reading env variables")

        # debug mode
        DEBUG: bool = os.environ.get("ARCHIVE_DEBUG", False)
        logger.info(f"Running in DEBUG mode {DEBUG}")

        # slack token
        SLACK_TOKEN: str = os.environ["SLACK_TOKEN"]
        # dnanexus token
        DNANEXUS_TOKEN: str = os.environ["DNANEXUS_TOKEN"]

        # project ids
        PROJECT_52: str = os.environ.get(
            "PROJECT_52",
            "project-FpVG0G84X7kzq58g19vF1YJQ",
        )
        PROJECT_53: str = os.environ.get(
            "PROJECT_53",
            "project-FvbzbX84gG9Z3968BJjxYZ1k",
        )

        # inactivity weeks for 002 projects
        MONTH2: int = int(os.environ.get("AUTOMATED_MONTH_002", 6))
        # inactivity weeks for 003 projects
        MONTH3: int = int(os.environ.get("AUTOMATED_MONTH_003", 3))
        # inactivity weeks for .tar files in staging52
        TAR_MONTH: int = int(os.environ.get("TAR_MONTH", 3))

        # grace period
        ARCHIVE_MODIFIED_MONTH: int = int(
            os.environ.get(
                "ARCHIVE_MODIFIED_MONTH",
                1,
            )
        )

        # pathway for memory pickle file
        ARCHIVE_PICKLE_PATH = os.environ.get(
            "AUTOMATED_ARCHIVE_PICKLE_PATH",
            "/monitoring/archive_dict.pickle",
        )
        # pathway for txt file to record file id that failed archiving
        ARCHIVE_FAILED_PATH = os.environ.get(
            "AUTOMATED_ARCHIVE_FAILED_PATH",
            "/monitoring/failed_archive.txt",
        )
        # pathway for txt file to record file id that has been archived
        ARCHIVED_TXT_PATH = os.environ.get(
            "AUTOMATED_ARCHIVED_TXT_PATH",
            "/monitoring/archived.txt",
        )

        # regex to exclude filename that fits the pattern
        AUTOMATED_REGEX_EXCLUDE = [
            text.strip()
            for text in os.environ["AUTOMATED_REGEX_EXCLUDE"].split(",")
            if text.strip()
        ]

    except KeyError as missing_env:
        logger.error(f"env {missing_env} cannot be found in config file")

        raise KeyError(f"env {missing_env} cannot be found in config file")

    # define Slack class
    slack = SlackClass(SLACK_TOKEN, TAR_MONTH, DEBUG)

    # re-define env variables for debug / testing
    if DEBUG:
        ARCHIVE_PICKLE_PATH = "/monitoring/archive_dict.test.pickle"
        ARCHIVE_FAILED_PATH = "/monitoring/failed_archive.test.txt"
        ARCHIVED_TXT_PATH = "/monitoring/archived.test.txt"

    # read pickle memory
    archive_pickle: dict = read_or_new_pickle(ARCHIVE_PICKLE_PATH)
    to_be_archived: list = archive_pickle["to_be_archived"]
    staging52: list = archive_pickle["staging_52"]

    today: dt.date = dt.date.today()
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

            find_projects_and_notify(
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
            find_projects_and_notify(
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
