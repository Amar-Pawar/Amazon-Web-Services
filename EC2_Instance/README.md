# Creating EC2 Instances using boto3 library

### Install boto3 library and aws cli
$ pip3 install boto3
$ pip3 install awscli
### boto3 version: 1.18.32
### aws cli version : 1.20.32

### Create user with programatic access with your aws management console

### Write a function to create an instance
 try:
        logger.info("Creating EC2 instance")
        resource_ec2 = boto3.client("ec2")
        resource_ec2.run_instances(
            ImageId="ami-00399ec92321828f5",
            MinCount=1,
            MaxCount=1,
            InstanceType="t2.micro",
            KeyName="Amar-EC2-Service"
        )
    except Exception as e:
        logger.info(e)

### To stop instance
try:
        logger.info("Stop EC2 instance")
        instance_id = describe_ec2_instance()
        resource_ec2 = boto3.client("ec2")
        logger.info(resource_ec2.stop_instances(InstanceIds=[instance_id]))
    except Exception as e:
        logger.info(e)

### To start instance
try:
        logger.info("Start EC2 instance")
        instance_id = describe_ec2_instance()
        resource_ec2 = boto3.client("ec2")
        logger.info(resource_ec2.start_instances(InstanceIds=[instance_id]))
    except Exception as e:
        logger.info(e)

### to terminate instance
try:
        logger.info("Terminate EC2 instance")
        instance_id = describe_ec2_instance()
        resource_ec2 = boto3.client("ec2")
        logger.info(resource_ec2.terminate_instances(InstanceIds=[instance_id]))
    except Exception as e:
        logger.info(e)
