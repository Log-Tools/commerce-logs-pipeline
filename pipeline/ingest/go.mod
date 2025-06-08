module github.com/Log-Tools/commerce-logs-pipeline/ingest

go 1.23.0

toolchain go1.23.10

require (
	github.com/Azure/azure-sdk-for-go/sdk/storage/azblob v1.6.1
	github.com/Log-Tools/commerce-logs-pipeline/config v0.0.0
	github.com/confluentinc/confluent-kafka-go/v2 v2.10.0
)

require (
	github.com/Azure/azure-sdk-for-go/sdk/azcore v1.18.0 // indirect
	github.com/Azure/azure-sdk-for-go/sdk/internal v1.11.1 // indirect
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	golang.org/x/net v0.39.0 // indirect
	golang.org/x/text v0.24.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

// Temporary replace directive for local development
// Remove this once the repository is published on GitHub at github.com/Log-Tools/commerce-logs-pipeline
replace github.com/Log-Tools/commerce-logs-pipeline/config => ../config
