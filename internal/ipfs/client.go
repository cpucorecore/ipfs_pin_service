package ipfs

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"time"

	"github.com/cenkalti/backoff/v4"
	shell "github.com/ipfs/go-ipfs-api"
)

type GCReport struct {
	KeysRemoved int64
}

type RepoStat struct {
	StorageMax int64
	RepoSize   int64
}

type Client struct {
	sh *shell.Shell
}

func (c *Client) withRetry(ctx context.Context, operation string, fn func() error) error {
	b := backoff.NewExponentialBackOff()
	b.MaxElapsedTime = 2 * time.Minute
	b.InitialInterval = 1 * time.Second
	b.MaxInterval = 10 * time.Second

	return backoff.RetryNotify(func() error {
		if ctx.Err() != nil {
			return backoff.Permanent(ctx.Err())
		}
		err := fn()
		if err != nil {
			if netErr, ok := err.(net.Error); ok && (netErr.Temporary() || netErr.Timeout()) {
				return err // 可以重试
			}
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				return err // 可以重试
			}
			return backoff.Permanent(err) // 其他错误不重试
		}
		return nil
	}, b, func(err error, duration time.Duration) {
		if ctx.Err() == nil { // 只在上下文未取消时记录
			log.Printf("Retrying %s after %v due to error: %v", operation, duration, err)
		}
	})
}

func NewClient(apiAddr string) *Client {
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

	sh := shell.NewShellWithClient(apiAddr, httpClient)

	return &Client{
		sh: sh,
	}
}

func (c *Client) PinAdd(ctx context.Context, cid string) error {
	var response struct {
		Pins     []string `json:"pins"`
		Progress int      `json:"progress"`
	}

	err := c.sh.Request("pin/add", cid).
		Option("recursive", true).
		Exec(ctx, &response)

	if err != nil {
		return fmt.Errorf("pin add %s failed: %w", cid, err)
	}

	if len(response.Pins) == 0 {
		return fmt.Errorf("pin add %s: no pins in response", cid)
	}

	return nil
}

func (c *Client) PinRm(ctx context.Context, cid string) error {
	return c.sh.Request("pin/rm", cid).
		Option("recursive", true).
		Exec(ctx, nil)
}

func (c *Client) RepoGC(ctx context.Context) (GCReport, error) {
	var report GCReport

	err := c.withRetry(ctx, "repo gc", func() error {
		var totalKeysRemoved int64

		// Use streaming response to read GC results
		resp, err := c.sh.Request("repo/gc").Send(ctx)
		if err != nil {
			return fmt.Errorf("gc request failed: %w", err)
		}
		defer resp.Close()

		dec := json.NewDecoder(resp.Output)
		for {
			var result struct {
				Key   string `json:"Key"`
				Error string `json:"Error"`
			}
			if err := dec.Decode(&result); err != nil {
				if err == io.EOF {
					break
				}
				return fmt.Errorf("gc decode failed: %w", err)
			}
			if result.Error != "" {
				log.Printf("GC warning for key %s: %s", result.Key, result.Error)
				continue
			}
			if result.Key != "" {
				totalKeysRemoved++
			}
		}

		report = GCReport{KeysRemoved: totalKeysRemoved}
		return nil
	})

	if err != nil {
		return GCReport{}, err
	}

	return report, nil
}

func (c *Client) RepoStat(ctx context.Context) (RepoStat, error) {
	var result RepoStat

	err := c.withRetry(ctx, "repo stat", func() error {
		var stat struct {
			StorageMax int64 `json:"storage_max"`
			RepoSize   int64 `json:"repo_size"`
		}
		err := c.sh.Request("repo/stat").Exec(ctx, &stat)
		if err != nil {
			return fmt.Errorf("stat: %w", err)
		}
		result = RepoStat{
			StorageMax: stat.StorageMax,
			RepoSize:   stat.RepoSize,
		}
		return nil
	})

	if err != nil {
		return RepoStat{}, err
	}

	return result, nil
}
