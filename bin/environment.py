import os
import pprint

from bin.helper import get_logger


logger = get_logger(__name__)


class EnvironmentVariableClass:
    """
    Class to store all configuration variables
    """

    def __init__(self):
        self.SLACK_TOKEN: str
        self.DNANEXUS_TOKEN: str
        self.PROJECT_52: str
        self.AUTOMATED_MONTH_002: int
        self.AUTOMATED_MONTH_003: int
        self.AUTOMATED_CEN_WES_MONTH: int
        self.TAR_MONTH: int
        self.ARCHIVE_MODIFIED_MONTH: int
        self.PRECISION_MONTH: int
        self.AUTOMATED_ARCHIVE_PICKLE_PATH: str
        self.ARCHIVE_DEBUG: bool
        self.AUTOMATED_REGEX_EXCLUDE: list[str]
        self.PRECISION_ARCHIVING: list[str]
        self.DNANEXUS_URL_PREFIX: str
        self.GUIDELINE_URL: str

        self.required_variables = {
            "SLACK_TOKEN": None,
            "DNANEXUS_TOKEN": None,
            "PROJECT_52": "project-FpVG0G84X7kzq58g19vF1YJQ",
            "AUTOMATED_MONTH_002": 3,
            "AUTOMATED_MONTH_003": 1,
            "AUTOMATED_CEN_WES_MONTH": 6,
            "TAR_MONTH": 3,
            "ARCHIVE_MODIFIED_MONTH": 1,
            "PRECISION_MONTH": 1,
            "AUTOMATED_ARCHIVE_PICKLE_PATH": "/monitoring/archive_dict.pickle",
            "ARCHIVE_DEBUG": False,
            "AUTOMATED_REGEX_EXCLUDE": None,
            "PRECISION_ARCHIVING": None,
            "DNANEXUS_URL_PREFIX": "https://platform.dnanexus.com/panx/projects",
            "GUIDELINE_URL": "https://cuhbioinformatics.atlassian.net/l/cp/Uh8PmK0T"
        }

    def load_configs(self):
        """
        Load environment variables into the class instance

        Raises:
            KeyError: If a required environment variable is missing
        """
        for variable_name, default_value in self.required_variables.items():
            value = os.getenv(variable_name)
            if value is None and default_value is None:
                raise KeyError(
                    f"Missing required environment variable: {variable_name}"
                )

            setattr(self, variable_name, value or default_value)

        self._correct_typing()
        self._process_precision_projects_variable()
        self._process_regex_exclude_variable()
        self._debug_variables()

        self._print_variables()

    def _correct_typing(self):
        """
        Correct the typing of the variables
        """
        for attr in [
            "AUTOMATED_MONTH_002",
            "AUTOMATED_MONTH_003",
            "AUTOMATED_CEN_WES_MONTH",
            "TAR_MONTH",
            "ARCHIVE_MODIFIED_MONTH",
            "PRECISION_MONTH",
        ]:
            setattr(self, attr, int(getattr(self, attr)))

        setattr(self, "ARCHIVE_DEBUG", bool(getattr(self, "ARCHIVE_DEBUG")))

    def _process_precision_projects_variable(self):
        """
        Process the precision archiving projects variable
        """
        self.PRECISION_ARCHIVING = (
            [
                project_id.strip()
                for project_id in self.PRECISION_ARCHIVING.split(",")
            ]
            if "," in self.PRECISION_ARCHIVING
            else []
        )

    def _process_regex_exclude_variable(self):
        """
        Process the regex exclude variable
        """
        self.AUTOMATED_REGEX_EXCLUDE = [
            text.strip()
            for text in self.AUTOMATED_REGEX_EXCLUDE.split(",")
            if text.strip()
        ]

    def _debug_variables(self):
        """
        Redefine env variables for debug / testing
        """
        if self.ARCHIVE_DEBUG:
            self.AUTOMATED_ARCHIVE_PICKLE_PATH = (
                "/monitoring/archive_dict.test.pickle"
            )

    def _print_variables(self):
        """
        Print all the variables except for sensitive ones (auth tokens)
        """
        logger.info(
            pprint.pformat(
                {
                    k: v
                    for k, v in vars(self).items()
                    if not k
                    in ["SLACK_TOKEN", "DNANEXUS_TOKEN", "required_variables"]
                }
            )
        )
