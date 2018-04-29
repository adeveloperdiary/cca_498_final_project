### Manual Steps

Note, when rebooting, all you should have to do is the following:
sudo docker exec -t sandbox-hdp /root/start-sandbox-hdp.sh

I have had some issues with Kibana restarting gracefully, in the event that hapepns, execute the following commands.

### Step 1 - Setup AWS instance
Ec2 instance notes
OS: Ubuntu - ami-43a15f3e (Default Ubuntu)
Size: m5.2xlarge (if you are getting poor performance, use m5.4xlarge)
Configuration Details - Leave default
Storage - EBS: 100 GB EBS
Tags - Key:Name, Label:"cca-project"
Security Group
Type            | Protocol | Port Range | Source
---------------------------------------------------
Custom TCP Rule | TCP      | 8080       | Anywhere
HTTP            | TCP      | 80         | Anywhere
HTTPS           | TCP      | 443        | Anywhere
SSH             | TCP      | 22         | Anywhere
Custom TCP Rule | TCP      | 7474       | Anywhere
Custom TCP Rule | TCP      | 5601       | Anywhere
Custom TCP Rule | TCP      | 7687       | Anywhere
Custom TCP Rule | TCP      | 9090       | Anywhere
Custom TCP Rule | TCP      | 5000       | Anywhere

### Step 2 - Run setup-ec2-instance.sh script
ssh -i "aws-boinc-key.pem" ubuntu@ec2-34-203-224-198.compute-1.amazonaws.com
git clone https://github.com/adeveloperdiary/cca_498_final_project.git
cd cca_498_final_project/setup-ec2-instance
sudo ./setup-ec2-instance.sh

### Step 3 - Setup SSH Tunnel Based on instance
ssh -i "[aws-key-pair.pem]]" -N \
-L 8080:[ip.amazonaws.com]:8080 \
-L 7474:[ip.amazonaws.com]:7474 \
-L 7687:[ip.amazonaws.com]:7687 \
-L 9200:[ip.amazonaws.com]:9200 \
 ubuntu@[ip.amazonaws.com]

 Example

ssh -i "aws-boinc-key.pem" -N \
-L 8080:ec2-54-152-36-59.compute-1.amazonaws.com:8080 \
-L 7474:ec2-54-152-36-59.compute-1.amazonaws.com:7474 \
-L 7687:ec2-54-152-36-59.compute-1.amazonaws.com:7687 \
-L 9200:ec2-54-152-36-59.compute-1.amazonaws.com:9200 \
-L 9090:ec2-54-152-36-59.compute-1.amazonaws.com:9090 \
-L 5000:ec2-54-152-36-59.compute-1.amazonaws.com:5000 \
 ubuntu@ec2-54-152-36-59.compute-1.amazonaws.com

### Wait until you see Ambari is up and running on port 8080
### If you do it beforehand, it can cause issues with HDFS

### Step 4 - Set the password in ambari
### Default password is: hadoop
### Gets you into the sandbox-hdp docker
ssh root@127.0.0.1 -p 2222
### Resets ambari-admin-password
ambari-admin-password-reset

### Step 5 - Kick off the start all
sudo docker exec -t sandbox-hdp /root/start-sandbox-hdp.sh

### Step 6 - Go to localhost:8080 in browser
### Turn HBase service on

### Step 7 - Once all services are up and running, need to start hbase
ssh root@127.0.0.1 -p 2222
hbase thrift start &

### Step 8 - Test that hbase is working by running the below code in python3 shell
import happybase as hb
con = hb.Connection('hbase-docker', 9090)
cf = {
    'raw': dict(),
    'mod': dict()
}
con.create_table('test2', cf)

### Step 9 - In Amabari (localhost:8080) - Import files into HDFS using the files
### view.  You can access this by select the button to the left of hte Admin at the top right
### Put the files in the following directory
### Need to run preprocess scripts to stage Answers and Questions to get Answers_New.csv and Questions_New.csv

### Step 10 - Before running, need to source bash_profile
source ~/.bash_profile
