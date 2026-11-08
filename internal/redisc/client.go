// Package redisc is a deliberately small Redis client (RESP2 over TCP) with a
// connection pool and EVALSHA support.
//
// Why not go-redis? The build sandbox this project was first written in could
// not download modules, so the gateway depends only on the Go standard
// library. Swapping in go-redis later only touches this package: the rest of
// the code uses the Do and Script APIs below.
package redisc

import (
	"bufio"
	"context"
	"crypto/sha1"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"time"
)

// ErrNil is returned for a nil bulk reply.
var ErrNil = errors.New("redisc: nil reply")

// ServerError is an error reply from Redis (a "-ERR ..." line).
type ServerError string

func (e ServerError) Error() string { return string(e) }

type conn struct {
	nc net.Conn
	br *bufio.Reader
	bw *bufio.Writer
}

// Client is safe for concurrent use.
type Client struct {
	addr        string
	dialTimeout time.Duration
	pool        chan *conn // idle connections
	sem         chan struct{}
}

// Options configure a Client.
type Options struct {
	Addr        string
	PoolSize    int           // max concurrent connections (default 64)
	DialTimeout time.Duration // default 100ms
}

// New creates a client. No connection is made until the first command.
func New(o Options) *Client {
	if o.PoolSize <= 0 {
		o.PoolSize = 64
	}
	if o.DialTimeout <= 0 {
		o.DialTimeout = 100 * time.Millisecond
	}
	return &Client{
		addr:        o.Addr,
		dialTimeout: o.DialTimeout,
		pool:        make(chan *conn, o.PoolSize),
		sem:         make(chan struct{}, o.PoolSize),
	}
}

// Close closes idle connections.
func (c *Client) Close() {
	for {
		select {
		case cn := <-c.pool:
			cn.nc.Close()
		default:
			return
		}
	}
}

func (c *Client) get(ctx context.Context) (*conn, error) {
	select {
	case c.sem <- struct{}{}:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	select {
	case cn := <-c.pool:
		return cn, nil
	default:
	}
	d := net.Dialer{Timeout: c.dialTimeout}
	nc, err := d.DialContext(ctx, "tcp", c.addr)
	if err != nil {
		<-c.sem
		return nil, err
	}
	if tc, ok := nc.(*net.TCPConn); ok {
		_ = tc.SetNoDelay(true)
	}
	return &conn{nc: nc, br: bufio.NewReaderSize(nc, 4096), bw: bufio.NewWriterSize(nc, 4096)}, nil
}

// put returns a healthy connection; broken ones are closed.
func (c *Client) put(cn *conn, healthy bool) {
	if healthy {
		select {
		case c.pool <- cn:
		default:
			cn.nc.Close()
		}
	} else {
		cn.nc.Close()
	}
	<-c.sem
}

// Do runs one command. The context deadline bounds the whole round trip.
func (c *Client) Do(ctx context.Context, args ...any) (any, error) {
	cn, err := c.get(ctx)
	if err != nil {
		return nil, err
	}
	if dl, ok := ctx.Deadline(); ok {
		_ = cn.nc.SetDeadline(dl)
	} else {
		_ = cn.nc.SetDeadline(time.Now().Add(2 * time.Second))
	}
	if err := writeCommand(cn.bw, args); err != nil {
		c.put(cn, false)
		return nil, err
	}
	reply, err := readReply(cn.br)
	var se ServerError
	// A server error reply leaves the connection in a clean state; I/O errors do not.
	c.put(cn, err == nil || errors.As(err, &se))
	return reply, err
}

func writeCommand(w *bufio.Writer, args []any) error {
	fmt.Fprintf(w, "*%d\r\n", len(args))
	for _, a := range args {
		var s string
		switch v := a.(type) {
		case string:
			s = v
		case []byte:
			s = string(v)
		case int:
			s = strconv.Itoa(v)
		case int64:
			s = strconv.FormatInt(v, 10)
		case float64:
			s = strconv.FormatFloat(v, 'f', -1, 64)
		default:
			s = fmt.Sprint(v)
		}
		fmt.Fprintf(w, "$%d\r\n%s\r\n", len(s), s)
	}
	return w.Flush()
}

func readReply(r *bufio.Reader) (any, error) {
	line, err := r.ReadString('\n')
	if err != nil {
		return nil, err
	}
	if len(line) < 3 {
		return nil, fmt.Errorf("redisc: short reply %q", line)
	}
	body := strings.TrimSuffix(line[1:], "\r\n")
	switch line[0] {
	case '+':
		return body, nil
	case '-':
		return nil, ServerError(body)
	case ':':
		return strconv.ParseInt(body, 10, 64)
	case '$':
		n, err := strconv.Atoi(body)
		if err != nil {
			return nil, err
		}
		if n < 0 {
			return nil, nil
		}
		buf := make([]byte, n+2)
		if _, err := io.ReadFull(r, buf); err != nil {
			return nil, err
		}
		return string(buf[:n]), nil
	case '*':
		n, err := strconv.Atoi(body)
		if err != nil {
			return nil, err
		}
		if n < 0 {
			return nil, nil
		}
		out := make([]any, n)
		for i := range out {
			v, err := readReply(r)
			if err != nil {
				var se ServerError
				if errors.As(err, &se) { // nested error element: keep reading the array
					out[i] = se
					continue
				}
				return nil, err
			}
			out[i] = v
		}
		return out, nil
	}
	return nil, fmt.Errorf("redisc: unknown reply type %q", line[0])
}

// Script is a Lua script invoked with EVALSHA, loading itself on NOSCRIPT.
type Script struct {
	src string
	sha string
}

// NewScript computes the SHA1 of src up front.
func NewScript(src string) *Script {
	h := sha1.Sum([]byte(src))
	return &Script{src: src, sha: hex.EncodeToString(h[:])}
}

// Run executes the script. EVALSHA keeps the payload tiny; if the server
// restarted or failed over and lost its script cache we fall back to EVAL once,
// which also repopulates the cache.
func (s *Script) Run(ctx context.Context, c *Client, keys []string, args ...any) (any, error) {
	cmd := make([]any, 0, 3+len(keys)+len(args))
	cmd = append(cmd, "EVALSHA", s.sha, len(keys))
	for _, k := range keys {
		cmd = append(cmd, k)
	}
	cmd = append(cmd, args...)
	reply, err := c.Do(ctx, cmd...)
	var se ServerError
	if errors.As(err, &se) && strings.HasPrefix(string(se), "NOSCRIPT") {
		cmd[0], cmd[1] = "EVAL", s.src
		return c.Do(ctx, cmd...)
	}
	return reply, err
}

// Ints converts an array reply of integers.
func Ints(reply any, err error) ([]int64, error) {
	if err != nil {
		return nil, err
	}
	arr, ok := reply.([]any)
	if !ok {
		return nil, fmt.Errorf("redisc: expected array, got %T", reply)
	}
	out := make([]int64, len(arr))
	for i, v := range arr {
		n, ok := v.(int64)
		if !ok {
			return nil, fmt.Errorf("redisc: element %d is %T, want integer", i, v)
		}
		out[i] = n
	}
	return out, nil
}
