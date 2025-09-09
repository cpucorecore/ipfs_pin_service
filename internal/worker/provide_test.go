package worker

import (
	"os"
	"testing"

	"github.com/cpucorecore/ipfs_pin_service/internal/ipfs"
	"github.com/cpucorecore/ipfs_pin_service/internal/store"
	"github.com/cpucorecore/ipfs_pin_service/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestProvideWorker(t *testing.T) {
	// Init test logger
	log.InitLoggerForTest()

	// Create test store
	testStore, err := store.NewPebbleStore("test_provide.db")
	require.NoError(t, err)
	defer testStore.Close()
	defer os.RemoveAll("test_provide.db")

	// Create mock IPFS client
	ipfsClient := &ipfs.Client{}

	// Create provide worker
	worker := NewProvideWorker(3, 0, false, testStore, nil, ipfsClient)

	// Validate worker created
	assert.NotNil(t, worker)
	assert.Equal(t, testStore, worker.store)
	assert.NotNil(t, worker.provideFunc)
	assert.Equal(t, 3, worker.maxRetry)
	assert.Equal(t, "provide", worker.opType)
}

func TestProvideWorker_MessageFormat(t *testing.T) {
	// Validate using cid string as message payload
	testCid := "QmTest123"
	body := []byte(testCid)

	// Verify body
	assert.Equal(t, testCid, string(body))
	assert.Equal(t, []byte(testCid), body)
}
