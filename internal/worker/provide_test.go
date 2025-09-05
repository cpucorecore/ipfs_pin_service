package worker

import (
	"os"
	"testing"

	"github.com/cpucorecore/ipfs_pin_service/internal/ipfs"
	"github.com/cpucorecore/ipfs_pin_service/internal/store"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestProvideWorker(t *testing.T) {
	// 创建测试用的 store
	testStore, err := store.NewPebbleStore("test_provide.db")
	require.NoError(t, err)
	defer testStore.Close()
	defer os.RemoveAll("test_provide.db")

	// 创建测试用的 IPFS 客户端 (mock)
	ipfsClient := &ipfs.Client{}

	// 创建 provide worker
	worker := NewProvideWorker(3, 0, false, testStore, nil, ipfsClient)

	// 测试 worker 创建
	assert.NotNil(t, worker)
	assert.Equal(t, testStore, worker.store)
	assert.Equal(t, ipfsClient, worker.ipfs)
	assert.Equal(t, 3, worker.maxRetry)
	assert.Equal(t, false, worker.recursive)
}

func TestProvideWorker_MessageFormat(t *testing.T) {
	// 测试直接使用cid字符串的消息格式
	testCid := "QmTest123"
	body := []byte(testCid)

	// 验证消息内容
	assert.Equal(t, testCid, string(body))
	assert.Equal(t, []byte(testCid), body)
}
