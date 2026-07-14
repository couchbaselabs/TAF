"""
AWS IAM Library for TAF
Provides IAM role assumption (via STS) for AWS operations in the TAF framework.
"""

import datetime
import logging
import os
import threading

import boto3
from botocore.credentials import RefreshableCredentials
from botocore.exceptions import ClientError
from botocore.session import Session as BotocoreSession


class IAMLib:
    """
    IAM library: assumes an IAM role (e.g. jenkins-cp-cli) via STS and caches
    the resulting temporary credentials for use by other AWS clients or
    aws/kubectl-style CLI subprocess calls. Credentials are transparently
    re-assumed (same role/params) once they're at/near expiry, so long-lived
    test runs don't start failing partway through with expired-token errors.
    """

    DEFAULT_ROLE_SESSION_NAME = "taf-fusion-iam"
    DEFAULT_ROLE_DURATION_SECONDS = 3600
    # Refresh this many seconds before the cached credentials actually
    # expire, so in-flight aws/kubectl calls don't race the exact cutover.
    REFRESH_BUFFER_SECONDS = 60

    def __init__(self, access_key=None, secret_key=None, session_token=None, region=None):
        """
        Initialize the IAM/STS client.

        :param access_key: AWS access key for the base/source profile. Falls
                            back to the AWS_ACCESS_KEY_ID_004 env var (the
                            test-0004 account credentials) if not given.
        :param secret_key: AWS secret key for the base/source profile. Falls
                            back to the AWS_SECRET_ACCESS_KEY_004 env var if
                            not given.
        :param session_token: AWS session token for the base profile (optional)
        :param region: AWS region (optional, defaults to us-east-1)
        """
        logging.basicConfig()
        logging.getLogger('boto3').setLevel(logging.ERROR)
        logging.getLogger('botocore').setLevel(logging.ERROR)
        self.logger = logging.getLogger("AWS_IAM_Util")
        self.region = region or 'us-east-1'

        access_key = access_key or os.environ.get("AWS_ACCESS_KEY_ID_004")
        secret_key = secret_key or os.environ.get("AWS_SECRET_ACCESS_KEY_004")

        session_kwargs = {
            "aws_access_key_id": access_key,
            "aws_secret_access_key": secret_key,
            "region_name": self.region,
        }
        if session_token:
            session_kwargs["aws_session_token"] = session_token
        self.aws_session = boto3.Session(**session_kwargs)
        self.sts_client = self.aws_session.client('sts', region_name=self.region)

        self._lock = threading.Lock()
        self._assumed_role_credentials = None
        self._assume_role_kwargs = None

    def assume_role(self, role_arn: str, external_id: str = None,
                     role_session_name: str = None,
                     duration_seconds: int = None) -> bool:
        """
        Assume an IAM role (e.g. jenkins-cp-cli) and cache the resulting
        temporary credentials for use by get_env()/get_credentials(). Those
        same role/params are remembered so the credentials can be
        transparently re-assumed later once they're close to expiring.

        :param role_arn: ARN of the role to assume
        :param external_id: External ID required by the role's trust policy (optional)
        :param role_session_name: Session name to tag the assumed session with
        :param duration_seconds: Duration of the assumed session in seconds
                                  (max 3600 when role chaining is involved)
        :return: True on success, False otherwise
        """
        kwargs = {
            "RoleArn": role_arn,
            "RoleSessionName": role_session_name or self.DEFAULT_ROLE_SESSION_NAME,
            "DurationSeconds": duration_seconds or self.DEFAULT_ROLE_DURATION_SECONDS,
        }
        if external_id:
            kwargs["ExternalId"] = external_id
        return self._do_assume_role(kwargs)

    def _do_assume_role(self, kwargs: dict) -> bool:
        """Call STS AssumeRole with `kwargs` and cache the result plus `kwargs` itself (for later auto-refresh)."""
        try:
            response = self.sts_client.assume_role(**kwargs)
            creds = response["Credentials"]
            with self._lock:
                self._assumed_role_credentials = {
                    "AccessKeyId": creds["AccessKeyId"],
                    "SecretAccessKey": creds["SecretAccessKey"],
                    "SessionToken": creds["SessionToken"],
                    "Expiration": creds["Expiration"],
                }
                self._assume_role_kwargs = kwargs
            self.logger.info(f"Assumed role {kwargs['RoleArn']}, session expires at {creds['Expiration']}")
            return True
        except ClientError as e:
            self.logger.error(f"Error assuming role {kwargs['RoleArn']}: {e}")
            return False

    def _refresh_if_expiring(self) -> None:
        """Re-assume the same role (same ARN/external ID/session params) if the cached credentials are at/near expiry."""
        with self._lock:
            creds = self._assumed_role_credentials
            kwargs = self._assume_role_kwargs
            if not creds or not kwargs:
                return
            remaining = (creds["Expiration"] - datetime.datetime.now(datetime.timezone.utc)).total_seconds()
            if remaining > self.REFRESH_BUFFER_SECONDS:
                return
        self.logger.info(f"Assumed role {kwargs['RoleArn']} credentials expiring soon — refreshing")
        self._do_assume_role(kwargs)

    def get_credentials(self) -> dict[str, str]:
        """
        Return the cached STS credentials for the currently assumed role
        ({"AccessKeyId", "SecretAccessKey", "SessionToken", "Expiration"}),
        transparently refreshing them first if they're at/near expiry.
        None if assume_role() hasn't succeeded yet.
        """
        self._refresh_if_expiring()
        with self._lock:
            return dict(self._assumed_role_credentials) if self._assumed_role_credentials else {}

    def _refresh_metadata(self) -> dict:
        """
        botocore RefreshableCredentials callback: re-assume the same role
        (same ARN/external ID/session params) and return the refreshed
        credentials in the metadata shape botocore expects.
        """
        with self._lock:
            kwargs = self._assume_role_kwargs
        if not kwargs:
            raise RuntimeError("No assumed role to refresh — call assume_role() first")
        if not self._do_assume_role(kwargs):
            raise RuntimeError(f"Failed to refresh assumed role {kwargs['RoleArn']}")
        with self._lock:
            creds = self._assumed_role_credentials
        return {
            "access_key": creds["AccessKeyId"],
            "secret_key": creds["SecretAccessKey"],
            "token": creds["SessionToken"],
            "expiry_time": creds["Expiration"].isoformat(),
        }

    def get_boto3_session(self, region: str = None) -> boto3.Session:
        """
        Return a boto3.Session backed by auto-refreshing credentials for the
        currently assumed role. AWS clients/resources built from this session
        transparently re-assume the same role once the cached credentials are
        at/near expiry, instead of failing with ExpiredToken partway through
        a long-running test. assume_role() must have been called first.

        :param region: AWS region to configure on the session (optional)
        :return: a boto3.Session with auto-refreshing credentials
        """
        with self._lock:
            creds = self._assumed_role_credentials
        if not creds:
            raise RuntimeError("No assumed-role credentials — call assume_role() first")

        metadata = {
            "access_key": creds["AccessKeyId"],
            "secret_key": creds["SecretAccessKey"],
            "token": creds["SessionToken"],
            "expiry_time": creds["Expiration"].isoformat(),
        }
        refreshable_creds = RefreshableCredentials.create_from_metadata(
            metadata=metadata,
            refresh_using=self._refresh_metadata,
            method="sts-assume-role",
        )
        botocore_session = BotocoreSession()
        botocore_session._credentials = refreshable_creds
        if region:
            botocore_session.set_config_variable("region", region)
        return boto3.Session(botocore_session=botocore_session)

    def get_env(self) -> dict[str, str]:
        """
        Return an {AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_SESSION_TOKEN}
        dict for the currently assumed role, for use as subprocess env
        overrides (e.g. aws/kubectl CLI calls). Empty dict if no role has
        been assumed yet, so callers can safely `env.update(iam.get_env())`
        unconditionally and fall back to the base credentials.
        """
        creds = self.get_credentials()
        if not creds:
            return {}
        return {
            "AWS_ACCESS_KEY_ID": creds["AccessKeyId"],
            "AWS_SECRET_ACCESS_KEY": creds["SecretAccessKey"],
            "AWS_SESSION_TOKEN": creds["SessionToken"],
        }
