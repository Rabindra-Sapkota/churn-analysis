# ELT and churn analysis using big data tools
## Background
In financial industries, churn users are users who were active in past days but have stopped using service in recent days. Analysis of churn is very crutial in any organization since users were source of revenue and loosing then will cause in lost in revenue. Calculating churn in transactional database in time consuming and hampers transactional database. In this project, we extract data from transactional MySQL database, load it into hive table of HADOOP system. We calculate churn user in per month basis. Churn user are extracted if they have stopped doing overall payment or payment in particular service(useful analysis in utility payment). Data is then exposed with help of api and visualized in browser with help of angular.
## Languages Used
- Shell Script (sqoop utility)
- SQL
- Python
- HTML
- CSS
- Angular
## Tools and libraries Used
- Sqoop
- MySQL
- Hive
- Pandas
- PyHive
- mailerpy
- Flask
## Project Setup
### Download cloudera VM, unzip it and import in your virtual box
> Vmware Link: https://downloads.cloudera.com/demo_vm/vmware/cloudera-quickstart-vm-5.13.0-0-vmware.zip

> Vitrualbox Link: https://downloads.cloudera.com/demo_vm/virtualbox/cloudera-quickstart-vm-5.13.0-0-virtualbox.zip
### Enable repo to install dependencies
- cd /etc/yum.repos.d/
- cp CentOS-Base.repo CentOS-Base.repo.old
- vi CentOS-Base.repo looks
- Add below contents
[base]
name=CentOS-$releasever - Base
mirrorlist=http://mirrorlist.centos.org/?release=$releasever&arch=$basearch&repo=os
baseurl=http://vault.centos.org/6.9/os/$basearch
gpgcheck=1
gpgkey=http://mirror.centos.org/centos/RPM-GPG-KEY-CentOS-6
exclude=redhat-logos
- **Donot use yum update command since it causes to fail dependency in existing clodera setup**
```
yum clean all
yum install git # For cloning project later
```
### Configure hive setup to avoid printing logs
- vi /usr/lib/hive/bin/hive
- Comment section inside # add Spark assembly jar to the classpath
- **REFERENCE: https://cloudera.ericlin.me/2017/12/hive-cli-prints-slf4j-error-to-standard-output/**
### Clone Project and setup in local
- Fork project, add ssh key to git, **copy clone link in git section**
```
ssh -T git@github.com
git clone <copied_link>
cd chrun-analysis
chmod 751 initial_setup.sh
./initial_setup.sh
```
#### Running data injection
- Cronjob will be scheduled to run every month at day 1
- To load data without schedule for first time or at time of failure use code below
```
./data_injector.sh > log/SQOOP20210114020640.log 2>&1
```
- Programs will not wait to terminate and logs will be printed at file log/SQOOP20210114020640.log
- Use & at end of above command to stop waiting program to terminate
### Running api
```
./churn_api.py
```
Test functionality by going to browser at
> http://127.0.0.1:5000/churn_trend/all \
> http://127.0.0.1:5000/churn_distribution/2020/02
- **all** is product name for which churn trend is to be visualized
- **/2020/02** is year and month for which churn distribution is to be visualized among all
