package ipfs

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"time"

	ipfspath "github.com/ipfs/boxo/path"
	ipfscid "github.com/ipfs/go-cid"
	"github.com/ipfs/kubo/client/rpc"
	"github.com/ipfs/kubo/core/coreiface/options"

	"github.com/cpucorecore/ipfs_pin_service/internal/config"
	"github.com/cpucorecore/ipfs_pin_service/log"
)

type RepoStat struct {
	RepoSize   int64  `json:"RepoSize"`
	StorageMax int64  `json:"StorageMax"`
	NumObjects int64  `json:"NumObjects"`
	RepoPath   string `json:"RepoPath"`
	Version    string `json:"Version"`
}

type Client struct {
	ipfsCli *rpc.HttpApi
}

func NewClientWithConfig(url string, cfg *config.Config) *Client {
	dialTimeout := 30 * time.Second
	if cfg != nil && cfg.IPFS.DialTimeout > 0 {
		dialTimeout = cfg.IPFS.DialTimeout
	}

	httpTimeout := 30 * time.Second
	if cfg != nil && cfg.IPFS.HTTPTimeout > 0 {
		httpTimeout = cfg.IPFS.HTTPTimeout
	}

	httpClient := &http.Client{
		Transport: &http.Transport{
			Proxy: http.ProxyFromEnvironment,
			DialContext: (&net.Dialer{
				Timeout:   dialTimeout,
				KeepAlive: 30 * time.Second,
			}).DialContext,
			ForceAttemptHTTP2:     true,
			MaxIdleConns:          100,
			IdleConnTimeout:       90 * time.Second,
			TLSHandshakeTimeout:   10 * time.Second,
			ExpectContinueTimeout: 1 * time.Second,
			DisableKeepAlives:     false,
			DisableCompression:    false,
		},
		Timeout: httpTimeout,
	}

	ipfsCli, err := rpc.NewURLApiWithClient(url, httpClient)
	if err != nil {
		log.Log.Sugar().Fatalf("create ipfs client error: %v", err)
	}

	ipfsCli.Request("")
	return &Client{ipfsCli: ipfsCli}
}

func getCidPath(cidStr string) ipfspath.Path {
	cid, err := ipfscid.Decode(cidStr)
	if err != nil {
		log.Log.Sugar().Fatalf("decode cid error: %v", err) // already precheck request
	}
	return ipfspath.FromCid(cid)
}

func (c *Client) PinAdd(ctx context.Context, cidStr string) error {
	p := getCidPath(cidStr)
	return c.ipfsCli.Pin().Add(ctx, p)
}

func (c *Client) PinRm(ctx context.Context, cidStr string) error {
	p := getCidPath(cidStr)
	return c.ipfsCli.Pin().Rm(ctx, p)
}

func (c *Client) RepoGC(ctx context.Context) error {
	return c.ipfsCli.Request("repo/gc").Option("quiet", true).Exec(ctx, nil)
}

func (c *Client) RepoStat(ctx context.Context) (*RepoStat, error) {
	repoStat := &RepoStat{}
	err := c.ipfsCli.Request("repo/stat").Exec(ctx, repoStat)
	if err != nil {
		return nil, err
	}
	return repoStat, nil
}

// 定义 Wantlist 项目类型
type WantlistItem struct {
	CID string `json:"/"`
}

type BitswapStat struct {
	WantlistItems    []WantlistItem `json:"Wantlist"`
	Peers            []string       `json:"Peers"`
	BlocksReceived   uint64         `json:"BlocksReceived"`
	DataReceived     uint64         `json:"DataReceived"`
	DupBlksReceived  uint64         `json:"DupBlksReceived"`
	DupDataReceived  uint64         `json:"DupDataReceived"`
	MessagesReceived uint64         `json:"MessagesReceived"`
	BlocksSent       uint64         `json:"BlocksSent"`
	DataSent         uint64         `json:"DataSent"`
}

// 获取 Wantlist CID 列表
func (bs *BitswapStat) GetWantlist() []string {
	result := make([]string, len(bs.WantlistItems))
	for i, item := range bs.WantlistItems {
		result[i] = item.CID
	}
	return result
}

func (c *Client) BitswapStat(ctx context.Context) (*BitswapStat, error) {
	bitswapStat := &BitswapStat{}
	var rawMessage json.RawMessage
	err := c.ipfsCli.Request("bitswap/stat").Exec(ctx, &rawMessage)
	if err != nil {
		return nil, err
	}

	err = json.Unmarshal(rawMessage, bitswapStat)
	if err != nil {
		log.Log.Sugar().Infof("bitswap stat: %s", rawMessage)
		return nil, err
	}
	return bitswapStat, nil
}

func (c *Client) Provide(ctx context.Context, cidStr string) error {
	p := getCidPath(cidStr)
	return c.ipfsCli.Routing().Provide(ctx, p)
}

func (c *Client) ProvideRecursive(ctx context.Context, cidStr string) error {
	p := getCidPath(cidStr)
	return c.ipfsCli.Routing().Provide(ctx, p, options.Routing.Recursive(true))
}
