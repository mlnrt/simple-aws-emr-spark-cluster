import string
import random
from s3path import S3Path
from aws_cdk import (core as cdk,
                     aws_ec2 as ec2,
                     aws_s3 as s3,
                     aws_s3_deployment as s3_deploy,
                     aws_emr as emr,
                     aws_iam as iam)

"""
This stack deploys the following:
- The VPC for the EMR cluster
- Gateway VPC endpoint to access the S3 service from the PRIVATE subnets
- Security Groups for the EMR cluster instances
- Interface endpoint to access the instances using SSM through the VPC subnet
- Two S3 Buckets. One for the data and one for the EMR logs
- IAM Roles for the cluster, its instances 
- The EMR Cluster itself
"""

class AwsEmrSparkStack(cdk.Stack):

    def __init__(self, scope: cdk.Construct, construct_id: str, **kwargs) -> None:
        self.cidr = kwargs.pop("cidr", "172.16.0.0/22")
        self.max_azs = kwargs.pop("max_azs", 1)
        self.naming_prefix = kwargs.pop("naming_prefix", "default")
        self.naming_suffix = ''.join(random.choice(string.ascii_lowercase + string.digits) for i in range(8))
        self.instance_type = kwargs.pop("instance_type", "m4.large")
        self.instance_consumption = kwargs.pop("instance_consumption", "SPOT")
        self.nb_core_instances = kwargs.pop("nb_core_instances", 2)
        self.existing_data_buckets = kwargs.pop("existing_data_buckets", [])
        super().__init__(scope, construct_id, **kwargs)

        #
        # VPC
        #
        self.vpc = ec2.Vpc(
            self,
            "EmrSparkVpc",
            cidr=self.cidr,
            max_azs=self.max_azs)

        # Add Gateway endpoint for S3
        self.vpc.add_gateway_endpoint(
            "S3Endpoint",
            service=ec2.GatewayVpcEndpointAwsService.S3,
            subnets=[ec2.SubnetSelection(subnet_type=ec2.SubnetType.PRIVATE, one_per_az=True)])

        # I manually create security croup because if we let the EMR cluster create defaults security group
        # the stack fails to delete
        self.sg_master_security_group = ec2.SecurityGroup(
            self,
            "EmrManagedMasterSecurityGroup",
            vpc=self.vpc,
            allow_all_outbound=True,
            security_group_name="EmrManagedMasterSecurityGroup")
        self.sg_slave_security_group = ec2.SecurityGroup(
            self,
            "EmrManagedSlaveSecurityGroup",
            vpc=self.vpc,
            allow_all_outbound=True,
            security_group_name="EmrManagedSlaveSecurityGroup")
        self.sg_service_access_security_group = ec2.SecurityGroup(
            self,
            "EmrServiceAccessSecurityGroup",
            vpc=self.vpc,
            allow_all_outbound=False,
            security_group_name="EmrServiceAccessSecurityGroup")
        self.sg_notebook_security_group = ec2.SecurityGroup(
            self,
            "EmrNotebookSecurityGroup",
            vpc=self.vpc,
            allow_all_outbound=False,
            security_group_name="EmrNotebookSecurityGroup")

        # Add the inbound and outbound rules to the security groups of the MASTER node
        self.sg_master_security_group.add_ingress_rule(
            peer=self.sg_service_access_security_group,
            connection=ec2.Port(
                protocol=ec2.Protocol.TCP,
                string_representation="Custom TCP",
                from_port=8443,
                to_port=8443))
        self.sg_master_security_group.add_ingress_rule(
            peer=self.sg_master_security_group,
            connection=ec2.Port(
                protocol=ec2.Protocol.TCP,
                string_representation="All TCP",
                from_port=0,
                to_port=65535))
        self.sg_master_security_group.add_ingress_rule(
            peer=self.sg_master_security_group,
            connection=ec2.Port(
                protocol=ec2.Protocol.UDP,
                string_representation="All UDP",
                from_port=0,
                to_port=65535))
        self.sg_master_security_group.add_ingress_rule(
            peer=self.sg_master_security_group,
            connection=ec2.Port(
                protocol=ec2.Protocol.ICMP,
                string_representation="All ICMP - IPv4",
                from_port=-1,
                to_port=-1))
        self.sg_master_security_group.add_ingress_rule(
            peer=self.sg_slave_security_group,
            connection=ec2.Port(
                protocol=ec2.Protocol.TCP,
                string_representation="All TCP",
                from_port=0,
                to_port=65535))
        self.sg_master_security_group.add_ingress_rule(
            peer=self.sg_slave_security_group,
            connection=ec2.Port(
                protocol=ec2.Protocol.UDP,
                string_representation="All UDP",
                from_port=0,
                to_port=65535))
        self.sg_master_security_group.add_ingress_rule(
            peer=self.sg_slave_security_group,
            connection=ec2.Port(
                protocol=ec2.Protocol.ICMP,
                string_representation="All ICMP - IPv4",
                from_port=-1,
                to_port=-1))
        self.sg_master_security_group.add_ingress_rule(
            peer=self.sg_notebook_security_group,
            connection=ec2.Port(
                protocol=ec2.Protocol.TCP,
                string_representation="All TCP",
                from_port=0,
                to_port=65535))
        # Add the inbound and outbound rules to the security groups of the CORE nodes
        self.sg_slave_security_group.add_ingress_rule(
            peer=self.sg_service_access_security_group,
            connection=ec2.Port(
                protocol=ec2.Protocol.TCP,
                string_representation="Custom TCP",
                from_port=8443,
                to_port=8443))
        self.sg_slave_security_group.add_ingress_rule(
            peer=self.sg_master_security_group,
            connection=ec2.Port(
                protocol=ec2.Protocol.TCP,
                string_representation="All TCP",
                from_port=0,
                to_port=65535))
        self.sg_slave_security_group.add_ingress_rule(
            peer=self.sg_master_security_group,
            connection=ec2.Port(
                protocol=ec2.Protocol.UDP,
                string_representation="All UDP",
                from_port=0,
                to_port=65535))
        self.sg_slave_security_group.add_ingress_rule(
            peer=self.sg_master_security_group,
            connection=ec2.Port(
                protocol=ec2.Protocol.ICMP,
                string_representation="All ICMP - IPv4",
                from_port=-1,
                to_port=-1))
        self.sg_slave_security_group.add_ingress_rule(
            peer=self.sg_slave_security_group,
            connection=ec2.Port(
                protocol=ec2.Protocol.TCP,
                string_representation="All TCP",
                from_port=0,
                to_port=65535))
        self.sg_slave_security_group.add_ingress_rule(
            peer=self.sg_slave_security_group,
            connection=ec2.Port(
                protocol=ec2.Protocol.UDP,
                string_representation="All UDP",
                from_port=0,
                to_port=65535))
        self.sg_slave_security_group.add_ingress_rule(
            peer=self.sg_slave_security_group,
            connection=ec2.Port(
                protocol=ec2.Protocol.ICMP,
                string_representation="All ICMP - IPv4",
                from_port=-1,
                to_port=-1))
        # Add the inbound and outbound rules to the security groups for the SERVICE ACCESS
        # This is only required when the cluster runs in a private subnet
        self.sg_service_access_security_group.add_ingress_rule(
            peer=self.sg_master_security_group,
            connection=ec2.Port(
                protocol=ec2.Protocol.TCP,
                string_representation="Custom TCP",
                from_port=9443,
                to_port=9443))
        self.sg_service_access_security_group.add_egress_rule(
            peer=self.sg_master_security_group,
            connection=ec2.Port(
                protocol=ec2.Protocol.TCP,
                string_representation="Custom TCP",
                from_port=8443,
                to_port=8443))
        self.sg_service_access_security_group.add_egress_rule(
            peer=self.sg_slave_security_group,
            connection=ec2.Port(
                protocol=ec2.Protocol.TCP,
                string_representation="Custom TCP",
                from_port=8443,
                to_port=8443))
        # Add the inbound and outbound rules to the security groups for the NOTEBOOK instance
        # No notebook instance is automatically provisioned but the stack creates the IAM Role
        # and NSG for it
        self.sg_notebook_security_group.add_egress_rule(
            peer=self.sg_master_security_group,
            connection=ec2.Port(
                protocol=ec2.Protocol.TCP,
                string_representation="All TCP",
                from_port=0,
                to_port=65535))
        self.sg_notebook_security_group.add_egress_rule(
            peer=self.sg_master_security_group,
            connection=ec2.Port(
                protocol=ec2.Protocol.UDP,
                string_representation="All UDP",
                from_port=0,
                to_port=65535))

        # Add Interface endpoint for SSM
        self.ssm_endpoint = self.vpc.add_interface_endpoint(
            "SSMEndpoint",
            service=ec2.InterfaceVpcEndpointAwsService.SSM,
            private_dns_enabled=True,
            security_groups=[self.sg_master_security_group,
                             self.sg_slave_security_group],
            subnets=ec2.SubnetSelection(subnet_type=ec2.SubnetType.PRIVATE, one_per_az=True))

        #
        # S3
        #
        self.data_bucket = s3.Bucket(
            self,
            "EmrSparkDataS3Bucket",
            bucket_name=self.naming_prefix + "-emr-spark-data-" + self.naming_suffix,
            encryption=s3.BucketEncryption("S3_MANAGED"),
            enforce_ssl=True,
            removal_policy=cdk.RemovalPolicy.DESTROY)
        self.data_bucket.apply_removal_policy(cdk.RemovalPolicy.RETAIN)

        s3_deploy.BucketDeployment(
            self,
            "EmrSparkS3BucketDeploymentForBootstrap",
            destination_bucket=self.data_bucket,
            sources=[s3_deploy.Source.asset("./ec2_bootstrap_scripts")],
            destination_key_prefix="ec2_bootstrap_scripts"
        )

        # transfer the code of the steps to S3
        s3_deploy.BucketDeployment(
            self,
            "EmrSparkS3BucketDeploymentForCode",
            destination_bucket=self.data_bucket,
            sources=[s3_deploy.Source.asset("./emr_steps/code")],
            destination_key_prefix="code"
        )

        # transfer the data of the steps to S3
        s3_deploy.BucketDeployment(
            self,
            "EmrSparkS3BucketDeploymentForData",
            destination_bucket=self.data_bucket,
            sources=[s3_deploy.Source.asset("./emr_steps/data")],
            destination_key_prefix="data"
        )

        self.logs_bucket = s3.Bucket(
            self,
            "EmrSparkLogsS3Bucket",
            bucket_name=self.naming_prefix + "-emr-spark-logs-" + self.naming_suffix,
            encryption=s3.BucketEncryption("S3_MANAGED"),
            enforce_ssl=True,
            removal_policy=cdk.RemovalPolicy.DESTROY)
        self.logs_bucket.apply_removal_policy(cdk.RemovalPolicy.RETAIN)

        #
        # IAM
        #
        if self.existing_data_buckets:
            resources=[]
            for bucket in self.existing_data_buckets:
                # strip potential ending backslash and "s3://" at the beginning
                bucket_arn = "arn:aws:s3:::" + bucket.strip("/")[5:]
                resources += [bucket_arn , bucket_arn +"/*"]

            self.emr_service_role = iam.Role(
                self,
                "emrServiceRole",
                assumed_by=iam.ServicePrincipal(service="elasticmapreduce.amazonaws.com"),
                inline_policies={
                    "AmazonElasticMapReduceRole_inline_policy": iam.PolicyDocument(
                        statements=[
                            iam.PolicyStatement(
                                sid="AllowAccessToExistingS3Buckets",
                                actions=[
                                    "s3:List*",
                                    "s3:Get*"
                                ],
                                effect=iam.Effect.ALLOW,
                                resources=resources)])},
                managed_policies=[
                    iam.ManagedPolicy.from_aws_managed_policy_name(
                        "service-role/AmazonElasticMapReduceRole")],
                role_name="emr_service_role_" + self.naming_suffix
            )

            self.emr_ec2_role = iam.Role(
                self,
                "emrEc2InstanceRole",
                assumed_by=iam.ServicePrincipal(service="ec2.amazonaws.com"),
                inline_policies={
                    "emrEc2InstanceRole_inline_policy": iam.PolicyDocument(
                        statements=[
                            iam.PolicyStatement(
                                sid="AllowAccessToExistingS3Buckets",
                                actions=["s3:*"],
                                effect=iam.Effect.ALLOW,
                                resources=resources)])},
                managed_policies=[
                    iam.ManagedPolicy.from_aws_managed_policy_name(
                        "service-role/AmazonElasticMapReduceforEC2Role"),
                    iam.ManagedPolicy.from_aws_managed_policy_name(
                        "AmazonSSMManagedInstanceCore")],
                role_name="emr_ec2_role_" + self.naming_suffix
            )
        else:
            self.emr_service_role = iam.Role(
                self,
                "emrServiceRole",
                assumed_by=iam.ServicePrincipal(service="elasticmapreduce.amazonaws.com"),
                managed_policies=[
                    iam.ManagedPolicy.from_aws_managed_policy_name(
                        "service-role/AmazonElasticMapReduceRole")],
                role_name="emr_service_role_" + self.naming_suffix
            )

            self.emr_ec2_role = iam.Role(
                self,
                "emrEc2InstanceRole",
                assumed_by=iam.ServicePrincipal(service="ec2.amazonaws.com"),
                managed_policies=[
                    iam.ManagedPolicy.from_aws_managed_policy_name(
                        "service-role/AmazonElasticMapReduceforEC2Role"),
                    iam.ManagedPolicy.from_aws_managed_policy_name(
                        "AmazonSSMManagedInstanceCore")],
                role_name="emr_ec2_role_" + self.naming_suffix
            )

        self.emr_ec2_profile = iam.CfnInstanceProfile(
            self,
            "emrEc2InstanceProfile",
            roles=[self.emr_ec2_role.role_name],
            instance_profile_name="emr_ec2_role_" + self.naming_suffix
        )

        self.emr_notebook_role = iam.Role(
            self,
            "emrStudioNotebookRole",
            assumed_by=iam.ServicePrincipal(service="elasticmapreduce.amazonaws.com"),
            inline_policies={
                "emrStudioNotebookRole_inline_policy": iam.PolicyDocument(
                    statements=[
                        iam.PolicyStatement(
                            sid="AllowToGetSecretsFromSecretsManager",
                            actions=["secretsmanager:GetSecretValue"],
                            effect=iam.Effect.ALLOW,
                            resources=["*"]),
                        iam.PolicyStatement(
                            sid="AllowAllToDataS3Bucket",
                            actions=["s3:*"],
                            effect=iam.Effect.ALLOW,
                            resources=[
                                self.data_bucket.bucket_arn,
                                self.data_bucket.bucket_arn+"/*"])])},
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AmazonElasticMapReduceEditorsRole")],
            role_name="emr_notebooks_role_" + self.naming_suffix
        )

        #
        # EMR
        #
        self.emr_cluster = emr.CfnCluster(
            self,
            "EmrSparkCluster",
            instances=emr.CfnCluster.JobFlowInstancesConfigProperty(
                ec2_subnet_ids=[self.vpc.private_subnets[0].subnet_id],
                emr_managed_master_security_group=self.sg_master_security_group.security_group_name,
                emr_managed_slave_security_group=self.sg_slave_security_group.security_group_name,
                master_instance_group=emr.CfnCluster.InstanceGroupConfigProperty(
                    instance_count=1,
                    instance_type=self.instance_type,
                    name=self.naming_prefix + "emr-master-" + self.naming_suffix),
                core_instance_group=emr.CfnCluster.InstanceGroupConfigProperty(
                    instance_count=self.nb_core_instances,
                    instance_type=self.instance_type,
                    name=self.naming_prefix + "emr-core-" + self.naming_suffix),
                service_access_security_group=self.sg_service_access_security_group.security_group_name),
            job_flow_role=self.emr_ec2_profile.ref,
            name=self.naming_prefix + "-cluster-" + self.naming_suffix,
            service_role=self.emr_service_role.role_name,
            applications=[
                emr.CfnCluster.ApplicationProperty(name="Hadoop"),
                emr.CfnCluster.ApplicationProperty(name="Spark"),
                emr.CfnCluster.ApplicationProperty(name="Zeppelin"),
                emr.CfnCluster.ApplicationProperty(name="Livy"),
                emr.CfnCluster.ApplicationProperty(name="JupyterEnterpriseGateway")],
            bootstrap_actions=[emr.CfnCluster.BootstrapActionConfigProperty(
                name="EmrInstancesBootstrapScript",
                script_bootstrap_action=emr.CfnCluster.ScriptBootstrapActionConfigProperty(
                    path=self.data_bucket.s3_url_for_object("ec2_bootstrap_scripts/bootstrap.sh")))],
            configurations=[emr.CfnCluster.ConfigurationProperty(
                classification="spark-env",
                configurations=[
                    emr.CfnCluster.ConfigurationProperty(
                        classification="export",
                        configuration_properties={
                            "PYSPARK_PYTHON": "/usr/bin/python3"})])],
            ebs_root_volume_size=10,
            log_uri=self.logs_bucket.s3_url_for_object(),
            release_label="emr-6.3.0",
            visible_to_all_users=True
        )

        #
        # Generate Outputs
        #
        cdk.CfnOutput(
            self,
            "VpcIdOutput",
            value=self.vpc.vpc_id,
            description="The ID of the stack's VPC",
            export_name="VpcId")
        for i, public_subnet in enumerate(self.vpc.public_subnets):
            cdk.CfnOutput(
                self,
                "PublicSubnetIdsOutput" + str(i),
                value=public_subnet.subnet_id,
                description="The ID of the public subnet " + str(i),
                export_name="PublicSubnetId" + str(i))
        for i, private_subnet in enumerate(self.vpc.private_subnets):
            cdk.CfnOutput(
                self,
                "PrivateSubnetIdsOutput" + str(i),
                value=private_subnet.subnet_id,
                description="The ID of the private subnet " + str(i),
                export_name="PrivateSubnetId" + str(i))
        cdk.CfnOutput(
            self,
            "DataS3BucketNameOutput",
            value=self.data_bucket.bucket_name,
            description="The name of the data S3 bucket",
            export_name="DataS3BucketName")
        cdk.CfnOutput(
            self,
            "DataS3BucketArnOutput",
            value=self.data_bucket.bucket_arn,
            description="The ARN of the data S3 bucket",
            export_name="DataS3BucketArn")
        cdk.CfnOutput(
            self,
            "LogsS3BucketNameOutput",
            value=self.logs_bucket.bucket_name,
            description="The name of the EMR logs S3 bucket",
            export_name="LogsS3BucketName")
        cdk.CfnOutput(
            self,
            "LogsS3BucketArnOutput",
            value=self.logs_bucket.bucket_arn,
            description="The ARN of the EMR logs S3 bucket",
            export_name="LogsS3BucketArn")
        cdk.CfnOutput(
            self,
            "EmrClusterIdOutput",
            value=self.emr_cluster.ref,
            description="The ID of the EMR Spark cluster",
            export_name="EmrClusterId")


    def create_spark_step(self, step_config: dict) -> None:
        sanetized_name = "".join(c for c in step_config["name"] if c.isalnum())
        args = ["spark-submit",
                "--deploy-mode",
                "cluster"]
        if "code_source_folder" in step_config and "data_source_folder" in step_config:
            code_source_folder = step_config["code_source_folder"] \
                if step_config["code_source_folder"][0] != "/" \
                else step_config["code_source_folder"][1:]
            code_source_uri = S3Path.from_uri(step_config["data_source_bucket"]) / \
                    code_source_folder / \
                    step_config["code_filename"]
            args.append(code_source_uri.as_uri())
        else:
            args.append(self.data_bucket.s3_url_for_object("code/" + step_config["code_filename"]))
        args.extend(["--output_uri", self.data_bucket.s3_url_for_object("outputs/" + sanetized_name)])
        if "data_source_folder" in step_config:
            data_source_folder = step_config["data_source_folder"] \
                if step_config["data_source_folder"][0] != "/" \
                else step_config["data_source_folder"][1:]
            # If no already existing data S3 bucket are specified, use the data bucket created with the stack
            if "data_source_bucket" in step_config:
                args.append("--data_source_folder")
                data_source_uri = S3Path.from_uri(step_config["data_source_bucket"]) / data_source_folder
                args.append(data_source_uri.as_uri())
            else:
                args.append("--data_source_folder")
                args.append(self.data_bucket.s3_url_for_object( "data/" + data_source_folder ))

        emr.CfnStep(
            self,
            "EmrSparkStep" + sanetized_name,
            action_on_failure="CONTINUE",
            hadoop_jar_step=emr.CfnStep.HadoopJarStepConfigProperty(
                args=args,
                jar="command-runner.jar"),
            job_flow_id=self.emr_cluster.ref,
            name=sanetized_name
        )