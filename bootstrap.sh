#!/bin/bash

GO_VERSION_LINK="https://go.dev/VERSION?m=text"
GO_DOWNLOAD_LINK="https://go.dev/dl/*.linux-amd64.tar.gz"
SCALABLE_REPO="https://github.com/JGCRI/scalable.git"



echo "Connection to HPC/Cloud..."
read -p "Hostname: " host
read -p "Username: " user
if [[ $* == *"-i"* ]]; then
    while getopts ":i:" flag; do
        case $flag in
        i)
            echo "Found Identity"
            alias ssh='ssh -i $OPTARG'
        ;;
        esac
    done
fi
GO_VERSION=$(ssh $user@$host "curl -s $GO_VERSION_LINK | head -n 1 | tr -d '\n'")
DOWNLOAD_LINK="${GO_DOWNLOAD_LINK//\*/$GO_VERSION}"
FILENAME=$(basename $DOWNLOAD_LINK)
read -p \
"Enter Work Directory Name (created in home directory of remote system or if it already exists): " work_dir
ssh $user@$host "mkdir -p $work_dir"
ssh $user@$host "wget $DOWNLOAD_LINK -P $work_dir && tar -C $work_dir -xzf $work_dir/$FILENAME"
ssh $user@$host "git clone $SCALABLE_REPO $work_dir/scalable"
GO_PATH=$(ssh $user@$host "cd $work_dir/go/bin/ && pwd")
GO_PATH="$GO_PATH/go"
ssh -t $user@$host "cd $work_dir/scalable/communicator && $GO_PATH mod init communicator && $GO_PATH build src/communicator.go"
ssh $user@$host "export PATH=\$PATH:~/$work_dir/scalable/communicator"
echo "Creating container directory locally..."
mkdir containers
echo "Available container targets: "
sed -n 's/^FROM[[:space:]]\+[^ ]\+[[:space:]]\+AS[[:space:]]\+\([^ ]\+\)$/\1/p' Dockerfile | sed '/build_env/d'
HTTPS_PROXY="http://proxy01.pnl.gov:3128"
NO_PROXY="*.pnl.gov,*.pnnl.gov,127.0.0.1"
echo "Please enter the containers you'd like to build and upload to the remote system (separated by spaces): "
read -r -a targets
targets+=('scalable')
for target in "${targets[@]}"
do
    docker build --target $target --build-arg https_proxy=$HTTPS_PROXY --build-arg no_proxy=$NO_PROXY -t $target\_container .
    IMAGE_ID=$(docker images | grep $target\_container | sed 's/[\t ][\t ]*/ /g' | cut -d ' ' -f 3)
    docker save $IMAGE_ID -o containers/$target\_container.tar
done
rsync -aP containers $user@$host:~/$work_dir
for target in "${targets[@]}"
do
    ssh -t $user@$host "cd $work_dir/containers && apptainer build $target\_container.sif docker-archive:$target\_container.tar"
done
ssh -t $user@$host "communicator -s > $work_dir/communicator_logs.txt & module load apptainer && apptainer exec --bind $work_dir:/$work_dir $work_dir/containers/scalable_container.sif python3"