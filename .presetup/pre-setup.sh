# Using this AMI: ami-08f40561 (Instance)
# Using this AMI: ami-cef405a7 (EBS)

gpg --keyserver pgp.mit.edu --recv-keys 2B5C1B00
# gpg --keyserver keyserver.ubuntu.com --recv-keys F758CE318D77295D
gpg --export --armor 2B5C1B00 | sudo apt-key add -
wget -O - http://opscenter.datastax.com/debian/repo_key | sudo apt-key add -
sudo echo "sun-java6-bin shared/accepted-sun-dlj-v1-1 boolean true" | sudo debconf-set-selections
echo "export JAVA_HOME=/usr/lib/jvm/java-6-sun" | sudo -E tee -a ~/.bashrc
echo "export BRISK_HOME=/home/ubuntu/brisk" | sudo -E tee -a ~/.bashrc
export JAVA_HOME=/usr/lib/jvm/java-6-sun
sudo add-apt-repository "deb http://archive.canonical.com/ lucid partner"
sudo apt-get -y --force-yes update
sudo apt-get -y --force-yes install git ant sun-java6-jdk

# Git these files on to the server's home directory
git config --global color.ui auto
git config --global color.diff auto
git config --global color.status auto
git clone git://github.com/riptano/BriskClusterAMI.git
mv BriskClusterAMI/* ./
mv BriskClusterAMI/.presetup ./
mv BriskClusterAMI/.*py ./
mv BriskClusterAMI/.git/ ./
rm -rf BriskClusterAMI/
# git checkout tags/$(head -n 1 .presetup/VERSION)
git clone git://github.com/riptano/brisk.git
cd brisk
git checkout tags/beta2
ant

sudo su
ln -s /home/ubuntu/.m2 /root/.m2
rm -rf /home/ubuntu/.ssh
sudo rm -rf /root/.ssh
exit
cd
history -c
sudo python .presetup/run.py && sudo chown -R ubuntu:ubuntu . && rm -rf ~/.bash_history && history -c




# git pull && rm -rf ~/.bash_history && history -c
