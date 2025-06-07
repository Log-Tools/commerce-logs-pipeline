module github.com/Log-Tools/commerce-logs-pipeline/ingest

go 1.21

require (
	github.com/Azure/azure-sdk-for-go/sdk/storage/azblob v1.5.0
	github.com/Log-Tools/commerce-logs-pipeline/config v0.0.0
	github.com/confluentinc/confluent-kafka-go/v2 v2.6.1
)

require (
	github.com/Azure/azure-sdk-for-go/sdk/azcore v1.16.0 // indirect
	github.com/Azure/azure-sdk-for-go/sdk/internal v1.10.0 // indirect
	golang.org/x/net v0.29.0 // indirect
	golang.org/x/text v0.19.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

// Temporary replace directive for local development
// Remove this once the repository is published on GitHub at github.com/Log-Tools/commerce-logs-pipeline
replace github.com/Log-Tools/commerce-logs-pipeline/config => ../config
