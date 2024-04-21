# Map Reduce

## Usage

Run master:

first build and push docker image. Mapper and Reducer jobs rely on pulling this image from container registry.
```
sh build_image.sh
```
then,
```
run main.go --mode=master --input-dir /mnt/input/ --nfs-path /mnt/nfs/
```

Call mapper locally:

```
go run main.go --mode=mapper --input-dir /mnt/input --output-dir /mnt/job-test/ --file-range book-0-40
```

Call Reducer locally:

```
go run main.go --mode=reducer --reducer-id <id> --job-id <job-id> --nfs-path <nfs-mount-folder>
```

