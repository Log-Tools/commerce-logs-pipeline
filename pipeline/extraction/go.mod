module pipeline/extraction

go 1.23.0

toolchain go1.23.10

require (
	github.com/confluentinc/confluent-kafka-go/v2 v2.10.1
	github.com/stretchr/testify v1.9.0
	gopkg.in/yaml.v3 v3.0.1
	pipeline/events v0.0.0-00010101000000-000000000000
)

require (
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/stretchr/objx v0.5.2 // indirect
)

replace pipeline/events => ../events
