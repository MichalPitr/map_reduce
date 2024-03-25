docker run -itd --privileged \
  --restart unless-stopped \
  -e SHARED_DIRECTORY=/data \
  -v /home/michal/code/map_reduce/nfs/nfs-storage:/data \
  -p 2049:2049 \
  --name nfs \
  itsthenetwork/nfs-server-alpine:12