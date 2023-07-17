import requests
import json
import datetime as dt

from requests.adapters import HTTPAdapter
from urllib3.util import Retry


from bin.helper import get_logger

logger = get_logger(__name__)


class SlackClass:
    def __init__(
        self,
        token: str,
        months: int,
        debug: bool,
    ) -> None:
        self.token = token
        self.months = months
        self.debug = debug

        self._http = requests.Session()
        self._retries = Retry(
            total=5,
            backoff_factor=10,
            method_whitelist=["POST"],
        )
        self._http.mount(
            "https://",
            HTTPAdapter(max_retries=self._retries),
        )

    def _fetch_messages(
        self,
        purpose: str,
        today: str,
        **kwargs,
    ) -> str:
        """
        Function to return the right message for the given purpose

        Parameters:
        :param: purpose: decide on which message to return e.g. 002, alert
        :param: today: date to display on Slack message
        :param: **kwarg: see below

        :Keyword Arguments:
            **days_till_archiving `int`
                only when sending Slack countdown
            **archiving_date `datetime`
                most messages
            **tar_period_start_date `str`
                earliest period of tar.gz
            **tar_period_end_date `str`
                latest period of tar.gz
            **dnanexus_error `str`
                error message from dnanexus login

        Return:
            Slack message based on :param: purpose
        """

        days_till_archiving: int = kwargs.get("days_till_archiving")
        archiving_date: dt.datetime = kwargs.get("archiving_date")
        tar_period_start_date: str = kwargs.get("tar_period_start_date")
        tar_period_end_date: str = kwargs.get("tar_period_end_date")
        dnanexus_error: str = kwargs.get("dnanexus_error")

        msgs: dict[str, str] = {
            "002": (
                f":bangbang: {today} *002 projects to be archived:*"
                "\n_Please tag `no-archive` or `never-archive` "
                "in project.Settings.Tags_"
                f"\n*Archive date: {archiving_date}*"
            ),
            "003": (
                f":bangbang: {today} *003 projects to be archived:*"
                "\n_Please tag `no-archive` or `never-archive` "
                "in project.Settings.Tags_"
                f"\n*Archive date: {archiving_date}*"
            ),
            "staging52": (
                f":bangbang: {today} "
                "*Directories in `staging52` to be archived:*"
                "\n_Please tag `no-archive` or `never-archive` "
                "in on any file within the directory_"
                f"\n*Archive date: {archiving_date}*"
            ),
            "special-notify": (
                f":warning: {today} "
                "*Inactive project or directory to be archived*"
                "\n_unless re-tag `no-archive`_"
                f"\n*Archive date: {archiving_date}*"
            ),
            "no-archive": (
                f":male-detective: {today} "
                "*Projects or directory tagged with `no-archive`:*"
                "\n_just for your information_"
            ),
            "never-archive": (
                f":female-detective: {today} "
                "*Projects or directory tagged with `never-archive`:*"
                "\n_just for your information_"
            ),
            "archived": ":closed_book: *Projects or directory archived:*",
            "countdown": (
                "automated-archiving: "
                f"{days_till_archiving} day till archiving on {archiving_date}"
            ),
            "alert": (
                "automated-archiving: Error with dxpy token! Error code:\n"
                f"`{dnanexus_error}`"
            ),
            "tar": (
                "automated-tar-notify: `tar.gz` not modified in the last"
                f" {self.months} month\nPeriod:"
                f" {tar_period_start_date} -"
                f" {tar_period_end_date}\n_Please find complete list of"
                " file-id below:_"
            ),
        }

        return msgs[purpose]

    def post_message_to_slack(
        self,
        channel: str,
        purpose: str,
        today: dt.datetime,
        data: list = [],
        **kwargs,
    ) -> None:
        """
        Request function for slack web api for:
        (1) send alert msg when dxpy auth failed (alert=True)
        (2) send to-be-archived notification (default alert=False)

        Parameters:
        :param: channel: e.g. egg-alerts, egg-logs
        :param: purpose: alert, countdown, tar
        :param: today: datetime to appear in Slack notification
        :param: data: list of projs or dirs to be archived
        :param: **kwarg: see below

        :Keyword Arguments:
            **days_till_archiving `int`
                only when sending Slack countdown
            **archiving_date `datetime`
                most messages
            **tar_period_start_date `str`
                earliest period of tar.gz
            **tar_period_end_date `str`
                latest period of tar.gz
            **dnanexus_error `str`
                error message from dnanexus login
        """

        if self.debug:
            channel: str = "#egg-test"

        logger.info(
            f"POST request to channel: {channel} with purpose {purpose}",
        )

        strtoday: str = today.strftime("%d/%m/%Y")

        message: str = self._fetch_messages(purpose, strtoday, **kwargs)

        try:
            if purpose in ["alert", "countdown"]:
                response = self._http.post(
                    "https://slack.com/api/chat.postMessage",
                    {
                        "token": self.token,
                        "channel": f"{channel}",
                        "text": message,
                    },
                ).json()
            elif purpose == "tar":
                # tar-notify requires making a txt file of file-id
                # then send file as attachment using an enctype
                # of multipart/form-data

                with open("tar.txt", "w") as f:
                    for line in data:
                        txt = "\t".join(line)
                        f.write(f"{txt}\n")

                tar_file = {"file": ("tar.txt", open("tar.txt", "rb"), "txt")}
                response = self._http.post(
                    "https://slack.com/api/files.upload",
                    params={
                        "token": self.token,
                        "channels": f"{channel}",
                        "initial_comment": message,
                        "filename": "tar.txt",
                        "filetype": "txt",
                    },
                    files=tar_file,
                ).json()
            else:
                # default notification which is an attachment rather than
                # text (as seen in alert / countdown above)
                text_data = "\n".join(data)

                # number above 7,995 seems to get truncation
                if len(text_data) < 7995:
                    response = self._http.post(
                        "https://slack.com/api/chat.postMessage",
                        {
                            "token": self.token,
                            "channel": f"{channel}",
                            "attachments": json.dumps(
                                [
                                    {"pretext": message, "text": text_data},
                                ]
                            ),
                        },
                    ).json()
                else:
                    # chunk data based on its length after '\n'.join()
                    # if > than 7,995 after join(), we append
                    # data[start:end-1] into chunks.
                    # start = end - 1 and repeat
                    chunks = []
                    start = 0
                    end = 1

                    for index in range(1, len(data) + 1):
                        chunk = data[start:end]

                        if len("\n".join(chunk)) < 7995:
                            end = index

                            if end == len(data):
                                chunks.append(data[start:end])
                        else:
                            chunks.append(
                                data[start : end - 1],
                            )
                            start = end - 1

                    logger.info(f"Sending data in {len(chunks)} chunks")

                    for chunk in chunks:
                        text_data = "\n".join(chunk)

                        response = self._http.post(
                            "https://slack.com/api/chat.postMessage",
                            {
                                "token": self.token,
                                "channel": f"{channel}",
                                "attachments": json.dumps(
                                    [{"pretext": message, "text": text_data}]
                                ),
                            },
                        ).json()

                        if not response["ok"]:
                            break

            if response["ok"]:
                logger.info(f"POST request to {channel} successful")
            else:
                # slack api request failed
                slack_error = response["error"]

                raise ValueError(
                    "Error with Slack API or incorrect channel input to"
                    f" Slack: {slack_error}"
                )

        except Exception as other_exceptions:
            # endpoint request fail from server-side
            logger.error(f"Error sending POST request to channel {channel}")

            raise ValueError(other_exceptions)
