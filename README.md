# Map Reduce

## Usage

Run master:

first build and push docker image. Mapper and Reducer jobs rely on pulling this image from container registry. Make sure to update the script to push to your own registry.

```
sh build_image.sh
```

then,

```
go run main.go --mode master --image <image> --input-dir /mnt/input/ --nfs-path /mnt/nfs/
```

For debugging, you can run mapper and reducer locally:

```
go run main.go --mode=mapper --input-dir /mnt/input --output-dir /mnt/job-test/ --file-range book-0-40
```

```
go run main.go --mode=reducer --reducer-id <id> --job-id <job-id> --nfs-path <nfs-mount-folder>
```

