package main_test

import (
	"context"
	"testing"

	"cloud.google.com/go/bigtable"
	"github.com/stretchr/testify/suite"
	"golang.org/x/oauth2"
	"google.golang.org/api/option"
)

type testTokenSource struct{}

func (testTokenSource) Token() (*oauth2.Token, error) {
	return new(oauth2.Token), nil
}

type BigTableConnectivityTest struct {
	suite.Suite
}

func TestBigTableConnectivity(t *testing.T) {
	suite.Run(t, new(BigTableConnectivityTest))
}

func (assert *BigTableConnectivityTest) TestCanConnect() {
	ctx := context.Background()
	btClient, err := bigtable.NewAdminClient(ctx, "dev", "dev", option.WithTokenSource(&testTokenSource{}))
	assert.Nil(err)
	assert.NotNil(btClient)

	tables, err := btClient.Tables(ctx)
	assert.Nil(err)
	assert.NotNil(tables)

	btClient.Close()
}
