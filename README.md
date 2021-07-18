
# Deploy a simple AWS EMR Spark cluster with AWS CDK

I am studying at the __School of Information at the University of Michigan__, in the online Master of Applied Data Science.
During the __SIADS 516 course Big Data: Scalable Data Processing__ course we studied Spark 
while I was also studying at the same time for the AWS Certification __Data Analytics Specialty__.
I thus decided to crete a package with the AWS CDK to deploy my own AWS EMR Spark environment and try to run my homework
assignments on a full cluster instead of a single node.

## What does this package do?

It deploys a full test environment (VPC, S3 buckets, EMR cluster, etc) and creates and run the Spark tasks (EMR steps)
defined in the `./emr_steps` folder.

So despite it started with the MADS SIADS 516 course in mind, I tried to made this repository flexible 
so you can extend it to run your own Spark jobs.

The architecture diagram shows the components deployed in this stack:
* a decidated VPC, with public and private subnets, internet and nat gateways
* the Network Security Groups for the cluster
* the S3 buckets for the Spark logs and the data (code, input data files and jobs outputs)
* IAM Roles for the culster instances and EMR service
* the EMR cluster itself
* a VPC Endpoint for Systems Manager to access the instances if needed using Systems Manager instead of opening SSH ports
* a gateway VPC endpoint for the instances to access the S3 service directly

![](images/emr-stack-architecture.jpg)

The `cdk.json` file tells the CDK Toolkit how to execute your app.

This project is set up like a standard Python project.  The initialization
process also creates a virtualenv within this project, stored under the `.venv`
directory.  To create the virtualenv it assumes that there is a `python3`
(or `python` for Windows) executable in your path with access to the `venv`
package. If for any reason the automatic creation of the virtualenv fails,
you can create the virtualenv manually.

To manually create a virtualenv on MacOS and Linux:

```
$ python -m venv .venv
```

After the init process completes and the virtualenv is created, you can use the following
step to activate your virtualenv.

```
$ source .venv/bin/activate
```

If you are a Windows platform, you would activate the virtualenv like this:

```
% .venv\Scripts\activate.bat
```

Once the virtualenv is activated, you can install the required dependencies.

```
$ pip install -r requirements.txt
```

At this point you can now synthesize the CloudFormation template for this code.

```
$ cdk synth
```

To add additional dependencies, for example other CDK libraries, just add
them to your `setup.py` file and rerun the `pip install -r requirements.txt`
command.

## Useful commands

 * `cdk ls`          list all stacks in the app
 * `cdk synth`       emits the synthesized CloudFormation template
 * `cdk deploy`      deploy this stack to your default AWS account/region
 * `cdk diff`        compare deployed stack with current state
 * `cdk docs`        open CDK documentation

Enjoy!
