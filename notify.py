from xmlrpc.client import DateTime
import requests
import json
import sys
import datetime as dt
from requests.adapters import HTTPAdapter
from urllib3.util import Retry


from helper import get_logger

logger = get_logger("main log")


class Slack():
    def __init__(self, token, months, debug):
        self.token = token
        self.months = months
        self.debug = debug

    def fetch_messages(
        self,
        purpose: str,
        today: str,
        day: tuple,
        error_msg: str
            ) -> str:
        """
        Function to return the right message for the give purpose

        Inputs:
            purpose: decide on which message to return (etc, 002_proj, alert..)
            today: today's date to display on Slack message
            day: tuple of dates (vary) depending on purpose
            error_msg: error message from if purpose == alert (dxpy fail)

        Return:
            string of message
        """

        msgs = {
            '002_proj':
                (
                    f':bangbang: {today} *002 projects to be archived:*'
                    '\n_Please tag `no-archive` or `never-archive`_'
                    f'\n*Archive date: {day[0]}*'
                ),
            '003_proj':
                (
                    f':bangbang: {today} *003 projects to be archived:*'
                    '\n_Please tag `no-archive` or `never-archive`_'
                    f'\n*Archive date: {day[0]}*'
                ),
            'staging_52':
                (
                    f':bangbang: {today} '
                    '*Directories in `staging52` to be archived:*'
                    '\n_Please tag `no-archive` or `never-archive`_'
                    f'\n*Archive date: {day[0]}*'
                ),
            'special_notify':
                (
                    f':warning: {today} '
                    '*Inactive project or directory to be archived*'
                    '\n_unless re-tag `no-archive` or `never-archive`_'
                    f'\n*Archive date: {day[0]}*'
                ),
            'no_archive':
                (
                    f':male-detective: {today} '
                    '*Projects or directory tagged with `no-archive`:*'
                    '\n_just for your information_'
                ),
            'never_archive':
                (
                    f':female-detective: {today} '
                    '*Projects or directory tagged with `never-archive`:*'
                    '\n_just for your information_'
                ),
            'archived':
                (
                    ':closed_book: *Projects or directory archived:*'
                ),
            'countdown':
                (
                    f'automated-archiving: '
                    f'{day[0]} day till archiving on {day[1]}'
                ),
            'alert':
                (
                    "automated-archiving: Error with dxpy token! Error code:\n"
                    f"`{error_msg}`"
                ),
            'tar_notify':
                (
                    'automated-tar-notify: '
                    f'`tar.gz` not modified in the last {self.months} month'
                    f'\nEarliest Date: {day[0]} -- Latest Date: {day[1]}'
                    '\n_Please find complete list of file-id below:_'
                )
            }

        return msgs[purpose]

    def post_message_to_slack(
        self,
        channel: str,
        purpose: str,
        today: DateTime,
        data: list = None,
        error: str = None,
        day: tuple = (None, None)
            ) -> None:

        """
        Request function for slack web api for:
        (1) send alert msg when dxpy auth failed (alert=True)
        (2) send to-be-archived notification (default alert=False)

        Inputs:
            channel: e.g. egg-alerts, egg-logs
            purpose: this decide what message to send
            data: list of projs / dirs to be archived
            error: (optional) (required only when dxpy failed) dxpy error msg
            day: (optional) tuple of (day till next date, next run date) depend
            on purpose

        Return:
            None
        """

        http = requests.Session()
        retries = Retry(total=5, backoff_factor=10, method_whitelist=['POST'])
        http.mount("https://", HTTPAdapter(max_retries=retries))

        logger.info(f'Posting data for: {purpose}')

        strtoday = today.strftime("%d/%m/%Y")
        message = self.fetch_messages(purpose, strtoday, day, error)

        if not self.debug:
            pass
        else:
            channel = 'egg-test'

        logger.info(f'Sending POST request to channel: #{channel}')

        try:
            if purpose in ['alert', 'countdown']:
                response = http.post(
                    'https://slack.com/api/chat.postMessage', {
                        'token': self.token,
                        'channel': f'#{channel}',
                        'text': message
                    }).json()
            elif purpose == 'tar_notify':

                # tar_notify requires making a txt file of file-id
                # then send file as attachment using an enctype
                # of multipart/form-data

                with open('tar.txt', 'w') as f:
                    for line in data:
                        txt = "\t".join(line)
                        f.write(f'{txt}\n')

                tar_file = {
                    'file': ('tar.txt', open('tar.txt', 'rb'), 'txt')
                    }
                response = http.post(
                    'https://slack.com/api/files.upload',
                    params={
                        'token': self.token,
                        'channels': f'#{channel}',
                        'initial_comment': message,
                        'filename': 'tar.txt',
                        'filetype': 'txt'
                    },
                    files=tar_file
                    ).json()
            else:
                # default notification which is an attachment rather than
                # text (as seen in alert / countdown above)
                text_data = '\n'.join(data)

                # number above 7,995 seems to get truncation
                if len(text_data) < 7995:

                    response = http.post(
                        'https://slack.com/api/chat.postMessage', {
                            'token': self.token,
                            'channel': f'#{channel}',
                            'attachments': json.dumps([{
                                "pretext": message,
                                "text": text_data}])
                        }).json()
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

                        if len('\n'.join(chunk)) < 7995:
                            end = index

                            if end == len(data):
                                chunks.append(data[start:end])
                        else:
                            chunks.append(data[start:end-1])
                            start = end - 1

                    logger.info(f'Sending data in {len(chunks)} chunks')

                    for chunk in chunks:
                        text_data = '\n'.join(chunk)

                        response = http.post(
                            'https://slack.com/api/chat.postMessage', {
                                'token': self.token,
                                'channel': f'#{channel}',
                                'attachments': json.dumps([{
                                    "pretext": message,
                                    "text": text_data}])
                            }).json()

                        if not response['ok']:
                            break

            if response['ok']:
                logger.info(f'POST request to channel #{channel} successful')
            else:
                # slack api request failed
                error_code = response['error']
                logger.error(f'Slack API error to #{channel}')
                logger.error(f'Error Code From Slack: {error_code}')

                sys.exit('End of script')

        except Exception as e:
            # endpoint request fail from server
            logger.error(f'Error sending POST request to channel #{channel}')
            logger.error(e)

            sys.exit('End of script')
