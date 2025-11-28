#!/bin/bash

# Variables
ROLE_NAME="EC2SSMRole"
INSTANCE_PROFILE_NAME="EC2SSMProfile"
INSTANCE_ID="i-0abcdef1234567890" # REPLACE WITH YOUR INSTANCE ID

# 1. Create IAM Role
echo "Creating IAM Role..."
aws iam create-role --role-name $ROLE_NAME --assume-role-policy-document '{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": { "Service": "ec2.amazonaws.com" },
      "Action": "sts:AssumeRole"
    }
  ]
}'

# 2. Attach SSM Policy
echo "Attaching SSM Policy..."
aws iam attach-role-policy --role-name $ROLE_NAME --policy-arn arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore

# 3. Create Instance Profile
echo "Creating Instance Profile..."
aws iam create-instance-profile --instance-profile-name $INSTANCE_PROFILE_NAME
aws iam add-role-to-instance-profile --instance-profile-name $INSTANCE_PROFILE_NAME --role-name $ROLE_NAME

# 4. Attach to EC2
echo "Attaching to EC2 Instance $INSTANCE_ID..."
aws ec2 associate-iam-instance-profile --instance-id $INSTANCE_ID --iam-instance-profile Name=$INSTANCE_PROFILE_NAME

echo "Done! You may need to restart the SSM agent on the instance: sudo systemctl restart snap.amazon-ssm-agent.amazon-ssm-agent.service"
