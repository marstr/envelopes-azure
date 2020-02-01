package envelopes_azure

import (
	"bytes"
	"context"
	"fmt"
	"net/url"

	"github.com/Azure/azure-pipeline-go/pipeline"
	"github.com/Azure/azure-storage-blob-go/azblob"
	"github.com/marstr/envelopes"
)

type BlobClient struct {
	RepositoryAddress azblob.ContainerURL
	Pipeline pipeline.Pipeline
}

const blobUrlSeparator = "/"
const blobObjectsfolder = "objects"

func NewBlobClientFromSharedKey(accountName, accountKey, repositoryName string) (*BlobClient, error) {
	var err error
	var credential azblob.Credential
	var baseUrl *url.URL
	repositoryName = url.PathEscape(repositoryName)
	retval := BlobClient{}

	credential, err = azblob.NewSharedKeyCredential(accountName, accountKey)
	if err != nil {
		return nil, err
	}

	retval.Pipeline = azblob.NewPipeline(credential, azblob.PipelineOptions{})

	baseUrl, err = url.Parse(fmt.Sprintf("https://%s.blob.core.windows.net", accountName))
	if err != nil {
		return nil, err
	}

	accountUrl := azblob.NewServiceURL(*baseUrl, retval.Pipeline)
	retval.RepositoryAddress = accountUrl.NewContainerURL(repositoryName)

	return &retval, nil
}

func (azclient BlobClient) blobUrl(id envelopes.ID) azblob.BlockBlobURL {
	return azclient.RepositoryAddress.NewBlockBlobURL(fmt.Sprintf("%s/%s.json", blobObjectsfolder, id))
}

// Stash uploads a blob to the appropriate location in Azure Blob Storage.
func (azclient *BlobClient) Stash(ctx context.Context, id envelopes.ID, payload []byte) (err error) {
	blobLocation := azclient.blobUrl(id)

	_, err = blobLocation.Upload(
		ctx,
		bytes.NewReader(payload),
		azblob.BlobHTTPHeaders{ContentType: "text/plain"},
		azblob.Metadata{},
		azblob.BlobAccessConditions{})

	return
}

// Fetch downloads a blob from the appropriate location in Azure Blob Storage.
func (azclient *BlobClient) Fetch(ctx context.Context, id envelopes.ID) ([]byte, error) {
	blobLocation := azclient.blobUrl(id)

	get, err := blobLocation.Download(ctx, 0, 0, azblob.BlobAccessConditions{}, false)
	if err != nil {
		return nil, err
	}
	reader := get.Body(azblob.RetryReaderOptions{})
	defer reader.Close()

	downloadedData := &bytes.Buffer{}
	downloadedData.ReadFrom(reader)

	return downloadedData.Bytes(), nil
}

func (azclient *BlobClient) Current(ctx context.Context) (envelopes.ID, error) {
	panic("Not implemented")
}
