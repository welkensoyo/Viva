import os
import logging
import nfty.constants as constants

logger = logging.getLogger("AppLogger")


class EnvironmentError(Exception):
    """
    Custom Exception Subclass for Environment Error Encapsulation
    """

    def __init__(self, message, errors):
        super().__init__(
            message
        )  # Call the base class constructor with the parameters it needs

        self.errors = errors
        for error in self.errors:
            logger.error(f"Environment error detected: {error}")


class Environment:
    """
    Class which establishes if we are in a QA mode or environment or not.

    Args:
      - api_key: str
      - environment_label: str

    Returns:
      - bool
    """

    def __init__(self, api_key: str):
        self.errors = []
        self.api_key = api_key
        self.environment_label = os.getenv("dsn")
        self.non_prod_env = os.getenv("NON_PROD_ENV")
        self.is_non_production_environment = (
            self.non_prod_env and self.non_prod_env.lower() in ["true", "1"]
        )

        if not self.non_prod_env:
            self.errors.append("No NON_PROD_ENV environment variable identified.")

        if not self.environment_label:
            self.errors.append("No dsn environment variable identified.")

        if self.errors:
            raise EnvironmentError("Environment Error Exception Thrown.", self.errors)

    @property
    def is_qa(self) -> bool:
        """
        QA is more of a "mode" than an environment.

        This is because at any time in production we can pass in a `testapikey`, which will process in processor sandbox instances.

        Returns:
          - bool
        """
        is_production_environment = self.environment_label == constants.PRODUCTION

        if is_production_environment:
            if self.api_key == constants.API_KEY_TEST:
                return True
            else:
                return False
        else:
            return True
