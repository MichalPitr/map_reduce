# get ip address of the nfs docker by running docker inspect <container-name>
# the mount path for nfs4 is relative to the exported root folder. Since we export /data,
# the path here :/ just means it's inside of /data.
sudo mount -v -t nfs4 172.17.0.2:/ /mnt

# This might also work
# sudo mount -v -t nfs4 localhost:/ /mnt