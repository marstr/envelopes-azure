package envelopes_azure_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/marstr/envelopes"
	"github.com/marstr/envelopes/persist"

	az "github.com/marstr/envelopes-azure"
)

var overallCtx, _ = context.WithTimeout(context.Background(), time.Minute * 10)

var TestAccountName string
var TestAccountKey string
var TestRepoName string

func init(){
	TestAccountName = os.Getenv("AZ_ACCOUNT_NAME")
	TestAccountKey = os.Getenv("AZ_ACCOUNT_KEY")
	TestRepoName = os.Getenv("REPO_NAME")
}

func TestAzureClient_roundTrip(t *testing.T) {
	ctx, cancel := context.WithTimeout(overallCtx, 2 * time.Minute)
	defer cancel()

	subject, err := az.NewBlobClientFromSharedKey(TestAccountName, TestAccountKey, TestRepoName)
	if err != nil {
		t.Error(err)
		return
	}

	writer := persist.DefaultWriter{
		Stasher: subject,
	}

	reader := persist.DefaultLoader{
		Fetcher: subject,
	}

	expected := envelopes.Budget{
		Balance: envelopes.Balance(9987),
	}

	err = writer.Write(ctx, expected)
	if err == context.DeadlineExceeded || err == context.Canceled {
		t.Skip(err)
		return
	} else if err != nil {
		t.Error(err)
	}

	var got envelopes.Budget
	err = reader.Load(ctx, expected.ID(), &got)
	if err == context.DeadlineExceeded || err == context.Canceled {
		t.Skip(err)
	} else if err != nil {
		t.Error(err)
	}

	if !expected.Equal(got) {
		t.Logf("\tgot:  %s\n\twant: %s", got, expected)
		t.Fail()
	}
}