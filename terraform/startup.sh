#! /bin/bash

# install docker
sudo apt-get update
sudo apt-get install \
    ca-certificates \
    curl \
    gnupg \
    lsb-release
sudo mkdir -p /etc/apt/keyrings
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg
echo \
  "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu \
  $(lsb_release -cs) stable" | sudo tee /etc/apt/sources.list.d/docker.list > /dev/null
sudo apt-get update
sudo apt-get -y install docker-ce docker-ce-cli containerd.io

# install python and git
sudo apt-get -y install python3-pip
sudo apt-get -y install git-all

# install docker-compose
sudo curl -L "https://github.com/docker/compose/releases/download/v2.7.0/docker-compose-linux-x86_64" -o /usr/local/bin/docker-compose
sudo chmod +x /usr/local/bin/docker-compose

# Docker post-install steps: https://docs.docker.com/engine/install/linux-postinstall/
sudo usermod -aG docker $USER
sudo usermod -aG docker ${var.gce_ssh_user}
newgrp docker
sudo su $USER

# generate ssh key
ssh-keygen -t ed25519 -N '' -C "${var.gce_ssh_user}@gmail.com" -f ~/.ssh/id_ed25519 <<< y

# add ssh-key to ssh-agent
eval "$(ssh-agent -s)"
ssh-add ~/.ssh/id_ed25519

# manual steps to add ssh-key public file to github:
# gh auth login
# gh ssh-key add ~/.ssh/id_ed25519.pub