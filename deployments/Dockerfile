# Use an official Go runtime as a parent image
FROM golang:1.22 as builder

# Set the working directory outside $GOPATH to enable Go modules
WORKDIR /app

# Copy the local package files to the container's workspace
ADD . /app

# Build the Go app
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o mapreduce .


# Use a smaller image to run the binary
FROM alpine:latest  
RUN apk --no-cache add ca-certificates

WORKDIR /root/
# Copy the binary from the builder stage
COPY --from=builder /app/mapreduce .

# Command to run the binary
CMD ["./mapreduce"]
