"""
EC2 Auto Scaling Group of Java DocLoader REST servers.

Each instance runs `jvms_per_instance` DocLoader JVMs (systemd template unit
docloader@<port>). Two ways to get instances:

* launch_template=<name|lt-id>: a pre-baked template whose AMI already has
  DocLoader (see bake.sh). A per-run template version reshapes the JVMs to
  this run's count/heap and stamps the run id as the /check-online name;
  the template's security group gets a per-run ingress rule for the TAF host.
* no template: ships this checkout's DocLoader/target build via a presigned
  S3 URL and creates a per-run security group and launch template.

Everything created per run is tagged taf-docloader-fleet=<run_id> and removed
by teardown(). Nothing here is product-specific: any test that can reach its
clusters from EC2 can use it.
"""

import os
import tarfile
import tempfile
import time
import uuid
from base64 import b64encode

import requests
from botocore.exceptions import ClientError

FLEET_TAG_KEY = "taf-docloader-fleet"
UBUNTU_AMI_SSM_PARAM = \
    "/aws/service/canonical/ubuntu/server/24.04/stable/current/{arch}/hvm/ebs-gp3/ami-id"

USER_DATA_TEMPLATE = """#!/bin/bash
set -euxo pipefail
exec > /var/log/docloader-bootstrap.log 2>&1
export DEBIAN_FRONTEND=noninteractive
apt-get update -y
apt-get install -y openjdk-17-jre-headless curl
mkdir -p /opt/docloader
curl -fsSL --retry 10 --retry-delay 5 -o /tmp/docloader.tgz '{artifact_url}'
tar -xzf /tmp/docloader.tgz -C /opt/docloader
cat > /etc/systemd/system/docloader@.service <<'EOF'
[Unit]
Description=TAF DocLoader REST server on port %i
After=network-online.target
[Service]
WorkingDirectory=/opt/docloader
ExecStart=/usr/bin/java -XX:MaxRAMPercentage={heap_pct} -cp /opt/docloader/magmadocloader/magmadocloader.jar RestServer.RestApplication --server.port=%i --server.name={server_name}
LimitNOFILE=1048576
Restart=on-failure
StandardOutput=append:/var/log/docloader-%i.log
StandardError=append:/var/log/docloader-%i.log
[Install]
WantedBy=multi-user.target
EOF
systemctl daemon-reload
for port in {ports}; do systemctl enable --now docloader@$port; done
"""


# User-data for a per-run version of a baked launch template (AMI built by
# bake.sh): run exactly the requested docloader@<port> units.
LT_RUN_USER_DATA = """#!/bin/bash
set -eux
sed -i 's/^SERVER_NAME=.*/SERVER_NAME={server_name}/; s/^HEAP_PCT=.*/HEAP_PCT={heap_pct}/' /etc/docloader/env
for unit in $(systemctl list-units --all --plain --no-legend 'docloader@*' | awk '{{print $1}}'); do
    systemctl disable --now "$unit"
done
for port in {ports}; do
    systemctl enable docloader@$port
    systemctl restart docloader@$port
done
"""


class DocLoaderFleet(object):
    def __init__(self, log, boto3_session, region, taf_path,
                 size=1, instance_type="c8g.8xlarge", jvms_per_instance=3,
                 base_port=8080, subnet_ids=None, ami_id=None, key_name=None,
                 ingress_cidrs=None, s3_bucket=None, root_volume_gb=30,
                 launch_timeout=1200, launch_template=None):
        self.log = log
        self.session = boto3_session
        self.region = region
        self.taf_path = taf_path
        self.size = int(size)
        self.instance_type = instance_type
        self.jvms_per_instance = int(jvms_per_instance)
        self.ports = [int(base_port) + i for i in range(self.jvms_per_instance)]
        self.subnet_ids = subnet_ids
        self.ami_id = ami_id
        self.key_name = key_name
        self.ingress_cidrs = ingress_cidrs
        self.s3_bucket = s3_bucket
        self.root_volume_gb = int(root_volume_gb)
        self.launch_timeout = int(launch_timeout)

        # Pre-baked launch template (name or lt- id) whose AMI already runs
        # docloader@<port> units. Its SG is reused and only gets a per-run
        # ingress rule; nothing else is created besides the ASG.
        self.launch_template = launch_template

        self.run_id = "taf-dl-%s" % uuid.uuid4().hex[:10]
        # Name reported by /check-online, so a stray DocLoader that happens to
        # answer on a recycled IP is never mistaken for one of ours. Baked
        # AMIs report the SERVER_NAME from their /etc/docloader/env.
        self.server_name = self.run_id
        self.ingress_rule_ids = list()
        # (LaunchTemplateId|LaunchTemplateName key, version) created per run
        self.lt_run_version = None
        self.ingress_sg_id = None
        self.ec2 = self.session.client("ec2", region_name=region)
        self.asg = self.session.client("autoscaling", region_name=region)
        self.s3 = self.session.client("s3", region_name=region)
        self.ssm = self.session.client("ssm", region_name=region)

        self.artifact_key = None
        self.sg_id = None
        self.launch_template_id = None
        self.asg_name = None
        self.instances = list()
        self.urls = list()

    # ---------------------------------------------------------------- launch
    def launch(self):
        """Create the fleet and block until every JVM answers /check-online.
        Returns the list of DocLoader base URLs (one per JVM)."""
        self.log.info("Launching DocLoader fleet %s: %s x %s, %s JVM(s) each, "
                      "region %s" % (self.run_id, self.size, self.instance_type,
                                     self.jvms_per_instance, self.region))
        if self.launch_template:
            lt_spec = self._use_launch_template()
            subnets = self.subnet_ids or self._default_subnets()
        else:
            artifact_url = self._upload_artifact()
            subnets = self.subnet_ids or self._default_subnets()
            vpc_id = self.ec2.describe_subnets(
                SubnetIds=subnets[:1])["Subnets"][0]["VpcId"]
            self.sg_id = self._create_security_group(vpc_id)
            self.launch_template_id = self._create_launch_template(artifact_url)
            lt_spec = {"LaunchTemplateId": self.launch_template_id,
                       "Version": "$Latest"}
        self._create_asg(subnets, lt_spec)
        self.instances = self._wait_for_asg_in_service()
        self.urls = ["http://%s:%s" % (inst["PublicIpAddress"], port)
                     for inst in self.instances for port in self.ports]
        self._wait_for_loaders_online()
        self.log.info("DocLoader fleet %s online: %s"
                      % (self.run_id, self.urls))
        return self.urls

    def _tags(self, name):
        return [{"Key": "Name", "Value": name},
                {"Key": FLEET_TAG_KEY, "Value": self.run_id}]

    def _account_id(self):
        return self.session.client("sts").get_caller_identity()["Account"]

    def _upload_artifact(self):
        build_dir = os.path.join(self.taf_path, "DocLoader", "target",
                                 "magmadocloader")
        jar = os.path.join(build_dir, "magmadocloader.jar")
        if not os.path.isfile(jar):
            raise Exception("DocLoader build not found at %s - run "
                            "'mvn install' in DocLoader first" % jar)
        self.log.info("Shipping DocLoader build %s (built %s)"
                      % (jar, time.ctime(os.path.getmtime(jar))))

        bucket = self.s3_bucket or "taf-docloader-fleet-%s-%s" % (
            self._account_id(), self.region)
        try:
            self.s3.head_bucket(Bucket=bucket)
        except ClientError:
            self.log.info("Creating S3 bucket %s for DocLoader artifacts"
                          % bucket)
            if self.region == "us-east-1":
                self.s3.create_bucket(Bucket=bucket)
            else:
                self.s3.create_bucket(Bucket=bucket, CreateBucketConfiguration={
                    "LocationConstraint": self.region})
        self.s3_bucket = bucket

        with tempfile.NamedTemporaryFile(suffix=".tgz") as tmp:
            with tarfile.open(tmp.name, "w:gz") as tar:
                tar.add(build_dir, arcname="magmadocloader")
            self.artifact_key = "%s/magmadocloader.tgz" % self.run_id
            self.s3.upload_file(tmp.name, bucket, self.artifact_key)
        # Presigned so instances need no IAM instance profile. Validity is
        # also capped by the signing credentials' own expiry; it only has to
        # outlive instance boot.
        return self.s3.generate_presigned_url(
            "get_object", Params={"Bucket": bucket, "Key": self.artifact_key},
            ExpiresIn=6 * 3600)

    def _default_subnets(self):
        # Newer types (e.g. c8g) are not offered in every AZ, and an ASG
        # subnet in such an AZ only produces failed launch activities.
        offered = {o["Location"] for o in self.ec2.describe_instance_type_offerings(
            LocationType="availability-zone",
            Filters=[{"Name": "instance-type",
                      "Values": [self.instance_type]}])["InstanceTypeOfferings"]}
        subnets = self.ec2.describe_subnets(Filters=[
            {"Name": "default-for-az", "Values": ["true"]}])["Subnets"]
        subnets = [s for s in subnets if s["AvailabilityZone"] in offered]
        if not subnets:
            raise Exception("No default subnets in %s offer %s; pass "
                            "subnet_ids"
                            % (self.region, self.instance_type))
        return [s["SubnetId"] for s in subnets]

    def _ingress_cidrs(self):
        if self.ingress_cidrs:
            return self.ingress_cidrs
        # DocLoader's REST API is unauthenticated (it even exposes /shutdown),
        # so it is only ever opened to the host driving the test.
        my_ip = requests.get("https://checkip.amazonaws.com",
                             timeout=10).text.strip()
        return ["%s/32" % my_ip]

    def _create_security_group(self, vpc_id):
        cidrs = self._ingress_cidrs()
        sg_id = self.ec2.create_security_group(
            GroupName=self.run_id, VpcId=vpc_id,
            Description="TAF DocLoader fleet %s" % self.run_id,
            TagSpecifications=[{"ResourceType": "security-group",
                                "Tags": self._tags(self.run_id)}])["GroupId"]
        ip_ranges = [{"CidrIp": cidr} for cidr in cidrs]
        permissions = [{"IpProtocol": "tcp", "FromPort": self.ports[0],
                        "ToPort": self.ports[-1], "IpRanges": ip_ranges}]
        if self.key_name:
            permissions.append({"IpProtocol": "tcp", "FromPort": 22,
                                "ToPort": 22, "IpRanges": ip_ranges})
        self.ec2.authorize_security_group_ingress(
            GroupId=sg_id, IpPermissions=permissions)
        self.log.info("Security group %s allows ports %s-%s from %s"
                      % (sg_id, self.ports[0], self.ports[-1], cidrs))
        return sg_id

    def _resolve_ami(self):
        if self.ami_id:
            return self.ami_id
        archs = self.ec2.describe_instance_types(
            InstanceTypes=[self.instance_type])["InstanceTypes"][0][
            "ProcessorInfo"]["SupportedArchitectures"]
        arch = "arm64" if "arm64" in archs else "amd64"
        return self.ssm.get_parameter(
            Name=UBUNTU_AMI_SSM_PARAM.format(arch=arch))["Parameter"]["Value"]

    def _create_launch_template(self, artifact_url):
        user_data = USER_DATA_TEMPLATE.format(
            artifact_url=artifact_url,
            heap_pct=max(10, 75 // self.jvms_per_instance),
            server_name=self.server_name,
            ports=" ".join(str(p) for p in self.ports))
        data = {
            "ImageId": self._resolve_ami(),
            "InstanceType": self.instance_type,
            "UserData": b64encode(user_data.encode()).decode(),
            "NetworkInterfaces": [{"DeviceIndex": 0,
                                   "AssociatePublicIpAddress": True,
                                   "Groups": [self.sg_id],
                                   "DeleteOnTermination": True}],
            "BlockDeviceMappings": [{"DeviceName": "/dev/sda1",
                                     "Ebs": {"VolumeSize": self.root_volume_gb,
                                             "VolumeType": "gp3",
                                             "DeleteOnTermination": True}}],
            "TagSpecifications": [{"ResourceType": "instance",
                                   "Tags": self._tags(self.run_id)},
                                  {"ResourceType": "volume",
                                   "Tags": self._tags(self.run_id)}],
        }
        if self.key_name:
            data["KeyName"] = self.key_name
        return self.ec2.create_launch_template(
            LaunchTemplateName=self.run_id, LaunchTemplateData=data,
            TagSpecifications=[{"ResourceType": "launch-template",
                                "Tags": self._tags(self.run_id)}],
        )["LaunchTemplate"]["LaunchTemplateId"]

    def _use_launch_template(self):
        key = "LaunchTemplateId" if self.launch_template.startswith("lt-") \
            else "LaunchTemplateName"
        version = self.ec2.describe_launch_template_versions(
            **{key: self.launch_template}, Versions=["$Default"])[
            "LaunchTemplateVersions"][0]
        data = version["LaunchTemplateData"]
        self.instance_type = data.get("InstanceType", self.instance_type)
        sg_ids = [g for nic in data.get("NetworkInterfaces", [])
                  for g in nic.get("Groups", [])] or data.get("SecurityGroupIds", [])
        if not sg_ids:
            raise Exception("Launch template %s has no security group"
                            % self.launch_template)
        cidrs = self._ingress_cidrs()
        try:
            rules = self.ec2.authorize_security_group_ingress(
                GroupId=sg_ids[0], IpPermissions=[{
                    "IpProtocol": "tcp", "FromPort": self.ports[0],
                    "ToPort": self.ports[-1],
                    "IpRanges": [{"CidrIp": cidr, "Description": self.run_id}
                                 for cidr in cidrs]}])["SecurityGroupRules"]
            self.ingress_rule_ids = [r["SecurityGroupRuleId"] for r in rules]
            self.ingress_sg_id = sg_ids[0]
        except ClientError as e:
            # Another run from this host already opened it; that run owns
            # (and will revoke) the rule.
            if "InvalidPermission.Duplicate" not in str(e):
                raise
        # The baked AMI starts its own set of JVMs; a per-run version of the
        # template carries user-data that reshapes them to this run's JVM
        # count/heap and stamps the run id as the /check-online name.
        user_data = LT_RUN_USER_DATA.format(
            server_name=self.server_name,
            heap_pct=max(10, 75 // self.jvms_per_instance),
            ports=" ".join(str(p) for p in self.ports))
        new_version = self.ec2.create_launch_template_version(
            **{key: self.launch_template},
            SourceVersion=str(version["VersionNumber"]),
            VersionDescription=self.run_id,
            LaunchTemplateData={
                "UserData": b64encode(user_data.encode()).decode()},
        )["LaunchTemplateVersion"]["VersionNumber"]
        self.lt_run_version = (key, new_version)
        self.log.info("Using launch template %s v%s (from v%s, %s), %s JVMs "
                      "on ports %s, SG %s open to %s"
                      % (self.launch_template, new_version,
                         version["VersionNumber"], data.get("ImageId"),
                         self.jvms_per_instance, self.ports, sg_ids[0], cidrs))
        return {key: self.launch_template, "Version": str(new_version)}

    def _create_asg(self, subnets, lt_spec):
        self.asg_name = self.run_id
        self.asg.create_auto_scaling_group(
            AutoScalingGroupName=self.asg_name,
            LaunchTemplate=lt_spec,
            MinSize=self.size, MaxSize=self.size,
            DesiredCapacity=self.size,
            VPCZoneIdentifier=",".join(subnets),
            HealthCheckType="EC2",
            Tags=[dict(tag, PropagateAtLaunch=True)
                  for tag in self._tags(self.run_id)])

    def _wait_for_asg_in_service(self):
        end_time = time.time() + self.launch_timeout
        while time.time() < end_time:
            group = self.asg.describe_auto_scaling_groups(
                AutoScalingGroupNames=[self.asg_name])["AutoScalingGroups"][0]
            ids = [i["InstanceId"] for i in group["Instances"]
                   if i["LifecycleState"] == "InService"]
            if len(ids) >= self.size:
                reservations = self.ec2.describe_instances(
                    InstanceIds=ids)["Reservations"]
                instances = [i for r in reservations for i in r["Instances"]
                             if i.get("PublicIpAddress")]
                if len(instances) >= self.size:
                    for inst in instances:
                        self.log.info("Loader instance %s %s %s"
                                      % (inst["InstanceId"],
                                         inst["Placement"]["AvailabilityZone"],
                                         inst["PublicIpAddress"]))
                    return instances
            time.sleep(15)
        activities = self.asg.describe_scaling_activities(
            AutoScalingGroupName=self.asg_name, MaxRecords=5)["Activities"]
        raise Exception("DocLoader ASG %s did not reach %s InService "
                        "instances in %ss. Recent activity: %s"
                        % (self.asg_name, self.size, self.launch_timeout,
                           [a.get("StatusMessage") or a["Description"]
                            for a in activities]))

    def _wait_for_loaders_online(self):
        pending = set(self.urls)
        end_time = time.time() + self.launch_timeout
        while pending and time.time() < end_time:
            for url in list(pending):
                if is_loader_online(url, self.server_name):
                    pending.discard(url)
            if pending:
                time.sleep(15)
        if pending:
            raise Exception("DocLoader JVMs never came online: %s. Check "
                            "/var/log/docloader-bootstrap.log and "
                            "/var/log/docloader-<port>.log on the instance"
                            % sorted(pending))

    # -------------------------------------------------------------- teardown
    def teardown(self):
        """Best effort: remove everything launch() created, in dependency
        order. Safe to call after a partial launch."""
        if self.asg_name:
            self._ignore(self.asg.delete_auto_scaling_group,
                         AutoScalingGroupName=self.asg_name, ForceDelete=True)
            self._wait_for_asg_deleted()
        if self.ingress_rule_ids:
            self._ignore(self.ec2.revoke_security_group_ingress,
                         GroupId=self.ingress_sg_id,
                         SecurityGroupRuleIds=self.ingress_rule_ids)
        if self.lt_run_version:
            key, version = self.lt_run_version
            self._ignore(self.ec2.delete_launch_template_versions,
                         **{key: self.launch_template},
                         Versions=[str(version)])
        if self.launch_template_id:
            self._ignore(self.ec2.delete_launch_template,
                         LaunchTemplateId=self.launch_template_id)
        if self.sg_id:
            # ENIs of terminating instances keep the group in use for a while
            for _ in range(30):
                try:
                    self.ec2.delete_security_group(GroupId=self.sg_id)
                    break
                except ClientError as e:
                    if "DependencyViolation" not in str(e):
                        self.log.warning("Delete SG %s: %s" % (self.sg_id, e))
                        break
                    time.sleep(20)
        if self.artifact_key:
            self._ignore(self.s3.delete_object, Bucket=self.s3_bucket,
                         Key=self.artifact_key)
        self.log.info("DocLoader fleet %s torn down" % self.run_id)

    def _wait_for_asg_deleted(self, timeout=900):
        end_time = time.time() + timeout
        while time.time() < end_time:
            try:
                groups = self.asg.describe_auto_scaling_groups(
                    AutoScalingGroupNames=[self.asg_name])["AutoScalingGroups"]
            except ClientError:
                return
            if not groups:
                return
            time.sleep(20)
        self.log.warning("ASG %s still present after %ss"
                         % (self.asg_name, timeout))

    def _ignore(self, func, **kwargs):
        try:
            func(**kwargs)
        except ClientError as e:
            self.log.warning("%s(%s): %s" % (func.__name__, kwargs, e))


def is_loader_online(url, server_name=None):
    try:
        resp = requests.get(url + "/check-online", timeout=5)
        if resp.status_code != 200:
            return False
        return server_name is None or resp.json().get("name") == server_name
    except Exception:
        return False
