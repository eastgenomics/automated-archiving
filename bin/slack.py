import requests
import json
import datetime as dt

from requests.adapters import HTTPAdapter
from urllib3.util import Retry

from bin.helper import get_logger
from bin.environment import EnvironmentVariableClass

logger = get_logger(__name__)


class SlackClass:
    """
    Slack class to send messages to Slack
    """

    MAX_LEN = 7995  # NOTE: 7995 is the magic number that slack api can handle

    def __init__(
        self,
        env: EnvironmentVariableClass,
        datetime: dt.date,
    ) -> None:
        self.env = env
        self.datetime = datetime

        # http session with retry
        self._http = requests.Session()
        self._retries = Retry(
            total=5,
            backoff_factor=10,
            allowed_methods=["POST"],
        )
        self._http.mount(
            "https://",
            HTTPAdapter(max_retries=self._retries),
        )

        # aims and messages
        self.messages = {
            "projects2": "002 Projects to be archived.",
            "projects3": "003 Projects to be archived.",
            "directories": "Directories in `staging52` to be archived.",
            "precisions": "Folders to be archived in `precision` projects.",
            "archived": "Projects or directory archived.",
        }

    def _get_archiving_date(self) -> dt.date:
        """
        Function to fetch next archiving date based on today's date
        Archiving date is either 1st or 15th of the month

        Returns:
            `datetime`: archiving date
        """

        archiving_date = self.datetime

        if archiving_date.day in [1, 15]:
            archiving_date += dt.timedelta(1)

        while archiving_date.day not in [1, 15]:
            archiving_date += dt.timedelta(1)

        return archiving_date

    def post_simple_message_to_slack(
        self,
        channel: str,
        message: str,
    ) -> None:
        """
        Function to send simple message to Slack

        Parameters:
        :param: channel: `str` channel to send message to
        :param: message: `str` message to send
        """

        if self.env.ARCHIVE_DEBUG:
            channel: str = "#egg-test"

        response = self._http.post(
            "https://slack.com/api/chat.postMessage",
            {
                "token": self.env.SLACK_TOKEN,
                "channel": f"{channel}",
                "text": message,
            },
        ).json()

        if response["ok"]:
            logger.info(f"POST request to {channel} successful")
        else:
            # slack api request failed
            logger.error(response["error"])

    def _send_message_with_pretext(
        self,
        channel: str,
        pretext: str,
        data: str,
    ) -> None:
        """
        Function to send message with pretext to Slack

        Parameters:
        :param: channel: `str` channel to send message to
        :param: pretext: `str` pretext to send
        :param: data: `str` data to send
        """
        try:
            response = self._http.post(
                "https://slack.com/api/chat.postMessage",
                {
                    "token": self.env.SLACK_TOKEN,
                    "channel": f"{channel}",
                    "attachments": json.dumps(
                        [
                            {"pretext": pretext, "text": data},
                        ]
                    ),
                },
            ).json()
        except Exception as e:
            logger.error(e)

        if response["ok"]:
            logger.info(f"POST request to {channel} successful")
        else:
            # slack api request failed
            logger.error(response.get("error"))

    def _send_message_in_chunks(
        self, channel: str, pretext: str, raw_data: list
    ) -> None:
        """
        Function to _send_message_with_pretext in chunks

        Parameters:
        :param: channel: `str` channel to send message to
        :param: pretext: `str` pretext to send
        :param: raw_data: `list` data to send
        """
        chunks = []
        start = 0
        end = 1

        # loop through the raw data, combine it with "\n"
        # until it's less than 7995 characters
        # then append it to chunks

        for index in range(1, len(raw_data) + 1):
            chunk = raw_data[start:end]

            if len("\n".join(chunk)) < self.MAX_LEN:
                end = index

                if end == len(raw_data):
                    chunks.append(raw_data[start:end])
            else:
                chunks.append(
                    raw_data[start : end - 1],
                )
                start = end - 1

        logger.info(f"Sending data in {len(chunks)} chunks")

        for chunk in chunks:
            text_data = "\n".join(chunk)
            self._send_message_with_pretext(channel, pretext, text_data)

    def post_long_message_to_slack(
        self,
        channel: str,
        aim: str,
        raw_data: list = [],
    ) -> None:
        """
        Function to send long message to Slack
        This function will decide whether to send message in chunks or not

        Parameters:
        :param: channel: `str` channel to send message to
        :param: aim: `str` aim to send message to
        :param: raw_data: `list` data to send
        """

        if self.env.ARCHIVE_DEBUG:
            channel: str = "#egg-test"

        logger.info(
            f"POST request to channel: {channel}",
        )

        if not raw_data:  # no data
            logger.info(
                f"No data to send to channel: {channel} for aim: {aim}"
            )
            return

        message: str = self.messages.get(aim)
        message += f"\nGoing to be archived on {self._get_archiving_date()}"

        text_data = "\n".join(raw_data)

        # number above 7,995 seems to get truncation
        if len(text_data) < self.MAX_LEN:
            self._send_message_with_pretext(channel, message, text_data)
        else:
            self._send_message_in_chunks(channel, message, raw_data)

    def _send_message_with_attachment(
        self,
        data: list,
        channel: str,
        message: str,
    ) -> None:
        """
        Function to send message with attachment to Slack

        Parameters:
        :param: data: `list` data to send
        :param: channel: `str` channel to send message to
        :param: message: `str` message to send
        """
        if self.env.ARCHIVE_DEBUG:
            channel: str = "#egg-test"

        with open("tar.txt", "w") as f:
            for line in data:
                txt = "\t".join(line)
                f.write(f"{txt}\n")

        response = self._http.post(
            "https://slack.com/api/files.upload",
            params={
                "token": self.env.SLACK_TOKEN,
                "channels": f"{channel}",
                "initial_comment": message,
                "filename": "tar.txt",
                "filetype": "txt",
            },
            files={"file": ("tar.txt", open("tar.txt", "rb"), "txt")},
        ).json()

        if response["ok"]:
            logger.info(f"POST request to {channel} successful")
        else:
            # slack api request failed
            logger.error(response.get("error"))

    def notify(self, aim_to_data: dict) -> None:
        """
        Function to notify on Slack based on aim:
        - tars, archived, precisions, directories, projects2, projects3

        Parameters:
        :param: aim_to_data: `dict` with aim as key and data as value
        """

        for aim, data in aim_to_data.items():
            if aim == "tars":
                if data:
                    self._send_message_with_attachment(
                        data,
                        "#egg-alerts",
                        f"automated-archiving: `tar.gz` in staging-52 not modified in the last {self.env.TAR_MONTH} months",
                    )
            else:
                if data:
                    self.post_long_message_to_slack(
                        "#egg-alerts",
                        aim,
                        data,
                    )
