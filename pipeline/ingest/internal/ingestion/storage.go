package ingestion

import (
	"context"
	"fmt"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Log-Tools/commerce-logs-pipeline/config"
)

// azureStorageClientFactory implements StorageClientFactory interface
type azureStorageClientFactory struct {
	storageConfig *config.Config
}

// NewAzureStorageClientFactory creates a new Azure storage client factory
func NewAzureStorageClientFactory() (StorageClientFactory, error) {
	// Load storage configuration from main config module
	storageConfig, err := config.LoadConfig()
	if err != nil {
		return nil, fmt.Errorf("failed to load storage configuration: %w", err)
	}

	return &azureStorageClientFactory{
		storageConfig: storageConfig,
	}, nil
}

// CreateBlobClient creates a blob client for the specified container and blob
// The subscription and environment are determined from the blob processing context
func (f *azureStorageClientFactory) CreateBlobClient(subscription, environment, containerName, blobName string) (BlobClient, error) {
	// Get storage account for this environment
	storageAccount, err := f.storageConfig.GetStorageAccount(subscription, environment)
	if err != nil {
		return nil, fmt.Errorf("failed to get storage account for %s/%s: %w", subscription, environment, err)
	}

	// Initialize Azure Blob Client
	cred, err := azblob.NewSharedKeyCredential(storageAccount.AccountName, storageAccount.AccessKey)
	if err != nil {
		return nil, fmt.Errorf("failed to create Azure shared key credential: %w", err)
	}

	serviceURL := fmt.Sprintf("https://%s.blob.core.windows.net/", storageAccount.AccountName)
	client, err := azblob.NewClientWithSharedKeyCredential(serviceURL, cred, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create Azure Blob client: %w", err)
	}

	containerClient := client.ServiceClient().NewContainerClient(containerName)
	blobClient := containerClient.NewBlobClient(blobName)

	return &azureBlobClientWrapper{
		client: blobClient,
	}, nil
}

// azureBlobClientWrapper wraps Azure blob client to implement BlobClient interface
type azureBlobClientWrapper struct {
	client *blob.Client
}

// GetProperties gets blob properties
func (c *azureBlobClientWrapper) GetProperties(ctx context.Context, options *blob.GetPropertiesOptions) (blob.GetPropertiesResponse, error) {
	return c.client.GetProperties(ctx, options)
}

// DownloadStream downloads blob stream
func (c *azureBlobClientWrapper) DownloadStream(ctx context.Context, options *blob.DownloadStreamOptions) (blob.DownloadStreamResponse, error) {
	return c.client.DownloadStream(ctx, options)
}
