package req_pre_check

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/cpucorecore/ipfs_pin_service/log"
	"github.com/gin-gonic/gin"
)

func TestMain(m *testing.M) {
	log.InitLoggerForTest()
	os.Exit(m.Run())
}

func TestCheckReq(t *testing.T) {
	// invalid cid
	if err, _, _ := CheckReq("invalid", 0); err == nil {
		t.Fatalf("expected error for invalid cid")
	}

	// negative size
	if err, _, _ := CheckReq("bafkreigh2akiscaildcw453xpy5z2wqzvxwp6j2qaf2t7qsc5f2q4k3w5e", -1); err == nil {
		t.Fatalf("expected error for negative size")
	}

	// valid
	expectedCid := "bafkreigh2akiscaildcw453xpy5z2wqzvxwp6j2qaf2t7qsc5f2q4k3w5e"
	expectedSize := int64(123)
	err, cid, size := CheckReq(expectedCid, expectedSize)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cid != expectedCid {
		t.Fatalf("cid mismatch: got=%s want=%s", cid, expectedCid)
	}
	if size != expectedSize {
		t.Fatalf("size mismatch: got=%d want=%d", size, expectedSize)
	}
}

func TestCheckJsonReq(t *testing.T) {
	// invalid json
	if err, _, _ := CheckJsonReq([]byte("{")); err == nil {
		t.Fatalf("expected error for invalid json")
	}

	// invalid cid
	body, _ := json.Marshal(map[string]any{"cid": "bad", "size": 1})
	if err, _, _ := CheckJsonReq(body); err == nil {
		t.Fatalf("expected error for invalid cid in json")
	}

	// valid
	expectedCid := "bafkreigh2akiscaildcw453xpy5z2wqzvxwp6j2qaf2t7qsc5f2q4k3w5e"
	expectedSize := int64(0)
	body, _ = json.Marshal(map[string]any{"cid": expectedCid, "size": expectedSize})
	if err, cid, size := CheckJsonReq(body); err != nil {
		t.Fatalf("unexpected error: %v", err)
	} else {
		if cid != expectedCid {
			t.Fatalf("cid mismatch: got=%s want=%s", cid, expectedCid)
		}
		if size != expectedSize {
			t.Fatalf("size mismatch: got=%d want=%d", size, expectedSize)
		}
	}
}

func TestCheckHttpReq(t *testing.T) {
	gin.SetMode(gin.TestMode)

	// missing cid
	{
		r := gin.Default()
		r.PUT("/pins/:cid", func(c *gin.Context) {
			err, _, _ := CheckHttpReq(c)
			if err == nil {
				t.Fatalf("expected error for empty cid")
			}
			c.Status(http.StatusOK)
		})
		req := httptest.NewRequest(http.MethodPut, "/pins/", nil)
		w := httptest.NewRecorder()
		r.ServeHTTP(w, req)
	}

	// invalid cid
	{
		r := gin.Default()
		r.PUT("/pins/:cid", func(c *gin.Context) {
			err, _, _ := CheckHttpReq(c)
			if err == nil {
				t.Fatalf("expected error for invalid cid")
			}
			c.Status(http.StatusOK)
		})
		req := httptest.NewRequest(http.MethodPut, "/pins/invalid?size=1", nil)
		w := httptest.NewRecorder()
		r.ServeHTTP(w, req)
	}

	// invalid size
	{
		r := gin.Default()
		r.PUT("/pins/:cid", func(c *gin.Context) {
			err, _, _ := CheckHttpReq(c)
			if err == nil {
				t.Fatalf("expected error for invalid size")
			}
			c.Status(http.StatusOK)
		})
		req := httptest.NewRequest(http.MethodPut, "/pins/bafkreigh2akiscaildcw453xpy5z2wqzvxwp6j2qaf2t7qsc5f2q4k3w5e?size=notnum", nil)
		w := httptest.NewRecorder()
		r.ServeHTTP(w, req)
	}

	// negative size
	{
		r := gin.Default()
		r.PUT("/pins/:cid", func(c *gin.Context) {
			err, _, _ := CheckHttpReq(c)
			if err == nil {
				t.Fatalf("expected error for negative size")
			}
			c.Status(http.StatusOK)
		})
		req := httptest.NewRequest(http.MethodPut, "/pins/bafkreigh2akiscaildcw453xpy5z2wqzvxwp6j2qaf2t7qsc5f2q4k3w5e?size=-1", nil)
		w := httptest.NewRecorder()
		r.ServeHTTP(w, req)
	}

	// valid without size (defaults to 0)
	{
		r := gin.Default()
		r.PUT("/pins/:cid", func(c *gin.Context) {
			err, cid, size := CheckHttpReq(c)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			expectedCid := "bafkreigh2akiscaildcw453xpy5z2wqzvxwp6j2qaf2t7qsc5f2q4k3w5e"
			if cid != expectedCid {
				t.Fatalf("cid mismatch: got=%s want=%s", cid, expectedCid)
			}
			if size != 0 {
				t.Fatalf("size mismatch: got=%d want=%d", size, 0)
			}
			c.Status(http.StatusOK)
		})
		req := httptest.NewRequest(http.MethodPut, "/pins/bafkreigh2akiscaildcw453xpy5z2wqzvxwp6j2qaf2t7qsc5f2q4k3w5e", nil)
		w := httptest.NewRecorder()
		r.ServeHTTP(w, req)
	}

	// valid with size
	{
		r := gin.Default()
		r.PUT("/pins/:cid", func(c *gin.Context) {
			err, cid, size := CheckHttpReq(c)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			expectedCid := "bafkreigh2akiscaildcw453xpy5z2wqzvxwp6j2qaf2t7qsc5f2q4k3w5e"
			if cid != expectedCid {
				t.Fatalf("cid mismatch: got=%s want=%s", cid, expectedCid)
			}
			if size != 42 {
				t.Fatalf("size mismatch: got=%d want=%d", size, 42)
			}
			c.Status(http.StatusOK)
		})
		req := httptest.NewRequest(http.MethodPut, "/pins/bafkreigh2akiscaildcw453xpy5z2wqzvxwp6j2qaf2t7qsc5f2q4k3w5e?size=42", nil)
		w := httptest.NewRecorder()
		r.ServeHTTP(w, req)
	}
}
