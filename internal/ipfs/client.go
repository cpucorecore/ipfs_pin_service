package ipfs

import (
	"context"
	"log"
	"net"
	"net/http"
	"time"

	ipfspath "github.com/ipfs/boxo/path"
	ipfscid "github.com/ipfs/go-cid"
	"github.com/ipfs/kubo/client/rpc"
)

type GCReport struct {
	KeysRemoved int64
}

type RepoStat struct {
	StorageMax int64
	RepoSize   int64
}

type Client struct {
	ipfsCli *rpc.HttpApi
}

func NewClient(url string) *Client {
	httpClient := &http.Client{
		Transport: &http.Transport{
			Proxy: http.ProxyFromEnvironment,
			DialContext: (&net.Dialer{
				Timeout:   30 * time.Second,
				KeepAlive: 30 * time.Second,
			}).DialContext,
			ForceAttemptHTTP2:     true,
			MaxIdleConns:          100,
			IdleConnTimeout:       90 * time.Second,
			TLSHandshakeTimeout:   10 * time.Second,
			ExpectContinueTimeout: 1 * time.Second,
		},
		Timeout: 30 * time.Second,
	}

	ipfsCli, err := rpc.NewURLApiWithClient(url, httpClient)
	if err != nil {
		log.Fatal(err)
	}

	return &Client{ipfsCli: ipfsCli}
}

func getCidPath(cidStr string) ipfspath.Path {
	cid, err := ipfscid.Decode(cidStr)
	if err != nil {
		log.Fatal(err) // already precheck request
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

func (c *Client) RepoGC(ctx context.Context) (GCReport, error) {
	var gcResp string
	err := c.ipfsCli.Request("repo/gc").Exec(ctx, &gcResp)
	log.Println("gcResp:", gcResp)
	if err != nil {
		return GCReport{}, err
	}
	return GCReport{}, nil
}

func (c *Client) RepoStat(ctx context.Context) (*RepoStat, error) {
	repoStat := &RepoStat{}
	err := c.ipfsCli.Request("repo/stat").Exec(ctx, repoStat)
	if err != nil {
		return nil, err
	}
	return repoStat, nil
}
