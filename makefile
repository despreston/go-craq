client-rpi:
	env GOOS=linux GOARCH=arm GOARM=7 go build -o bin/rpi/client cmd/client/client.go
node-rpi:
	env GOOS=linux GOARCH=arm GOARM=7 go build -o bin/rpi/node cmd/node/node.go
coordinator-rpi:
	env GOOS=linux GOARCH=arm GOARM=7 go build -o bin/rpi/coordinator cmd/coordinator/coordinator.go

