module github.com/NARUBROWN/spine-nats-platform/examples/worker

go 1.26.0

require (
	github.com/NARUBROWN/spine v0.7.0-preview.2
	github.com/NARUBROWN/spine-nats-platform v0.1.0-preview.1
)

require (
	github.com/klauspost/compress v1.20.0 // indirect
	github.com/nats-io/nats.go v1.55.0 // indirect
	github.com/nats-io/nkeys v0.4.16 // indirect
	github.com/nats-io/nuid v1.0.1 // indirect
	golang.org/x/crypto v0.57.0 // indirect
	golang.org/x/sys v0.48.0 // indirect
)

replace github.com/NARUBROWN/spine-nats-platform => ../..
