'''
/**********************************************************************************
@Author: Amar Pawar
@Date: 2021-09-01
@Last Modified by: Amar Pawar
@Last Modified time: 2021-09-01
@Title : Program to create, stop,strat and terminate EC2 instance with boto3 library
/**********************************************************************************
'''
import boto3
from logging_handler import logger

def create_ec2_instance():
    """
    Description:
        This function will create an EC2 instance
    MaxCount=1, # Keep the max count to 1, unless you have a requirement to increase it
    InstanceType="t2.micro", # Change it as per your need, But use the Free tier one
    KeyName="ec2-key" # Change it to the name of the key you have.
    :return: Creates the EC2 instance.
    """
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


def describe_ec2_instance():
    """
    Description:
        This function will describe an EC2 instance created.
    """
    try:
        logger.info("Describing EC2 instance")
        resource_ec2 = boto3.client("ec2")
        logger.info(resource_ec2.describe_instances()["Reservations"][0]["Instances"][0]["InstanceId"])
        return str(resource_ec2.describe_instances()["Reservations"][0]["Instances"][0]["InstanceId"])
    except Exception as e:
        logger.info(e)


def reboot_ec2_instance():
    """
    Description:
        This function will reboot EC2 instance.
    """
    try:
        logger.info("Reboot EC2 instance")
        instance_id = describe_ec2_instance()
        resource_ec2 = boto3.client("ec2")
        logger.info(resource_ec2.reboot_instances(InstanceIds=[instance_id]))
    except Exception as e:
        logger.info(e)


def stop_ec2_instance():
    """
    Description:
        This function will stop EC2 instance.
    """
    try:
        logger.info("Stop EC2 instance")
        instance_id = describe_ec2_instance()
        resource_ec2 = boto3.client("ec2")
        logger.info(resource_ec2.stop_instances(InstanceIds=[instance_id]))
    except Exception as e:
        logger.info(e)


def start_ec2_instance():
    """
    Description:
        This function will start EC2 instance.
    """
    try:
        logger.info("Start EC2 instance")
        instance_id = describe_ec2_instance()
        resource_ec2 = boto3.client("ec2")
        logger.info(resource_ec2.start_instances(InstanceIds=[instance_id]))
    except Exception as e:
        logger.info(e)


def terminate_ec2_instance():
    """
    Description:
        This function will terminate EC2 instance.
    """
    try:
        logger.info("Terminate EC2 instance")
        instance_id = describe_ec2_instance()
        resource_ec2 = boto3.client("ec2")
        logger.info(resource_ec2.terminate_instances(InstanceIds=[instance_id]))
    except Exception as e:
        logger.info(e)


create_ec2_instance()
describe_ec2_instance()
reboot_ec2_instance()
stop_ec2_instance()
start_ec2_instance()
terminate_ec2_instance()