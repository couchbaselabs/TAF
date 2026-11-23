"""AWS access for the qe-7 cost report.

Base credentials come from the environment (Jenkins binds the
``jenkins-cp-cli-keys`` credential as AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY)
or from a named profile when run by hand. The report then assumes the
``jenkins-cp-cli`` role in the qe-7 account. Sessions last about an hour, so
:class:`QeSession` re-assumes the role before it expires; a CloudTrail
download for a busy day can take longer than that.
"""

import datetime as dt
import os
import threading

import boto3

# Override with QE7_COST_ROLE_ARN / QE7_COST_EXTERNAL_ID (e.g. another
# sandbox); the defaults are the qe-7 role and the external id TAF's fusion
# tests already use (pytests/aGoodDoctor/fusion/fusion_aws_util.py)
QE7_ACCOUNT_ID = "264138468394"
ROLE_NAME = "jenkins-cp-cli"
DEFAULT_ROLE_ARN = os.environ.get(
    "QE7_COST_ROLE_ARN",
    "arn:aws:iam::%s:role/%s" % (QE7_ACCOUNT_ID, ROLE_NAME))
DEFAULT_EXTERNAL_ID = os.environ.get(
    "QE7_COST_EXTERNAL_ID", "f7bdb290-7b15-4ab7-afbf-28f3464a6144")
REFRESH_MARGIN = dt.timedelta(minutes=10)


class QeSession(object):
    """boto3 clients for the qe-7 account, with automatic role refresh."""

    def __init__(self, base_profile=None, role_arn=None,
                 external_id=DEFAULT_EXTERNAL_ID,
                 session_name="qe7-daily-cost-report"):
        self._base = boto3.Session(profile_name=base_profile)
        self.role_arn = role_arn or DEFAULT_ROLE_ARN
        self.external_id = external_id
        self.session_name = session_name
        self._lock = threading.Lock()
        self._session = None
        self._expires = None

    def _refresh(self):
        creds = self._base.client("sts").assume_role(
            RoleArn=self.role_arn, ExternalId=self.external_id,
            RoleSessionName=self.session_name)["Credentials"]
        self._session = boto3.Session(
            aws_access_key_id=creds["AccessKeyId"],
            aws_secret_access_key=creds["SecretAccessKey"],
            aws_session_token=creds["SessionToken"])
        self._expires = creds["Expiration"]

    def session(self):
        with self._lock:
            now = dt.datetime.now(dt.timezone.utc)
            if self._session is None or self._expires - now < REFRESH_MARGIN:
                self._refresh()
            return self._session

    def client(self, service, region="us-east-1"):
        """A fresh client on the current (refreshed if needed) session.
        Long-running workers should call this per unit of work, not once."""
        return self.session().client(service, region_name=region)

    def identity(self):
        return self.client("sts").get_caller_identity()["Arn"]
