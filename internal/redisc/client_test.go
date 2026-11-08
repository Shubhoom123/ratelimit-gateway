package redisc

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// fakeServer is an in-process RESP server so these tests need no real Redis.
// handler receives each parsed command and returns the raw RESP reply to send;
// returning "" closes the connection without replying.
type fakeServer struct {
	ln       net.Listener
	conns    atomic.Int64 // connections accepted
	commands atomic.Int64
	handler  func(args []string) string
}

func newFakeServer(t *testing.T, h func(args []string) string) *fakeServer {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	s := &fakeServer{ln: ln, handler: h}
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			s.conns.Add(1)
			go s.serve(c)
		}
	}()
	t.Cleanup(func() { ln.Close() })
	return s
}

func (s *fakeServer) addr() string { return s.ln.Addr().String() }

func (s *fakeServer) serve(c net.Conn) {
	defer c.Close()
	br := bufio.NewReader(c)
	for {
		args, err := readCommand(br)
		if err != nil {
			return
		}
		s.commands.Add(1)
		reply := s.handler(args)
		if reply == "" {
			return
		}
		if _, err := io.WriteString(c, reply); err != nil {
			return
		}
	}
}

func readCommand(br *bufio.Reader) ([]string, error) {
	line, err := br.ReadString('\n')
	if err != nil {
		return nil, err
	}
	n, err := strconv.Atoi(strings.TrimSpace(line[1:]))
	if err != nil {
		return nil, err
	}
	args := make([]string, n)
	for i := range args {
		hdr, err := br.ReadString('\n')
		if err != nil {
			return nil, err
		}
		l, _ := strconv.Atoi(strings.TrimSpace(hdr[1:]))
		buf := make([]byte, l+2)
		if _, err := io.ReadFull(br, buf); err != nil {
			return nil, err
		}
		args[i] = string(buf[:l])
	}
	return args, nil
}

func ctx500() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), 500*time.Millisecond)
}

func TestDoReplyTypes(t *testing.T) {
	srv := newFakeServer(t, func(a []string) string {
		switch a[0] {
		case "SIMPLE":
			return "+OK\r\n"
		case "INT":
			return ":42\r\n"
		case "NEGINT":
			return ":-7\r\n"
		case "BULK":
			return "$5\r\nhello\r\n"
		case "EMPTYBULK":
			return "$0\r\n\r\n"
		case "NILBULK":
			return "$-1\r\n"
		case "NILARRAY":
			return "*-1\r\n"
		case "ARRAY":
			return "*3\r\n:1\r\n$2\r\nhi\r\n+OK\r\n"
		case "NESTEDERR":
			return "*2\r\n:1\r\n-ERR boom\r\n"
		case "ERR":
			return "-ERR unknown command\r\n"
		}
		return "-ERR nope\r\n"
	})
	c := New(Options{Addr: srv.addr()})
	defer c.Close()
	ctx, cancel := ctx500()
	defer cancel()

	check := func(cmd string, want any) {
		t.Helper()
		got, err := c.Do(ctx, cmd)
		if err != nil {
			t.Fatalf("%s: %v", cmd, err)
		}
		if fmt.Sprint(got) != fmt.Sprint(want) {
			t.Errorf("%s = %#v, want %#v", cmd, got, want)
		}
	}
	check("SIMPLE", "OK")
	check("INT", int64(42))
	check("NEGINT", int64(-7))
	check("BULK", "hello")
	check("EMPTYBULK", "")
	check("ARRAY", []any{int64(1), "hi", "OK"})

	if got, err := c.Do(ctx, "NILBULK"); err != nil || got != nil {
		t.Errorf("nil bulk = %v, %v; want nil, nil", got, err)
	}
	if got, err := c.Do(ctx, "NILARRAY"); err != nil || got != nil {
		t.Errorf("nil array = %v, %v; want nil, nil", got, err)
	}

	// An error element inside an array must not abort the read of the array.
	got, err := c.Do(ctx, "NESTEDERR")
	if err != nil {
		t.Fatalf("nested error: %v", err)
	}
	arr := got.([]any)
	if arr[0] != int64(1) || arr[1].(ServerError) != "ERR boom" {
		t.Errorf("nested = %#v", arr)
	}

	// Top-level server error is a ServerError.
	_, err = c.Do(ctx, "ERR")
	var se ServerError
	if !errors.As(err, &se) || !strings.HasPrefix(string(se), "ERR unknown") {
		t.Errorf("err = %v, want ServerError", err)
	}
}

func TestWriteCommandEncodesArgTypes(t *testing.T) {
	got := make(chan []string, 1)
	srv := newFakeServer(t, func(a []string) string { got <- a; return "+OK\r\n" })
	c := New(Options{Addr: srv.addr()})
	defer c.Close()
	ctx, cancel := ctx500()
	defer cancel()
	if _, err := c.Do(ctx, "SET", "k", []byte("bytes"), 7, int64(-9), 1.5, true); err != nil {
		t.Fatal(err)
	}
	want := []string{"SET", "k", "bytes", "7", "-9", "1.5", "true"}
	if g := <-got; strings.Join(g, "|") != strings.Join(want, "|") {
		t.Errorf("server saw %v, want %v", g, want)
	}
}

func TestValuesWithCRLFAndBinaryRoundTrip(t *testing.T) {
	srv := newFakeServer(t, func(a []string) string {
		v := a[1]
		return fmt.Sprintf("$%d\r\n%s\r\n", len(v), v)
	})
	c := New(Options{Addr: srv.addr()})
	defer c.Close()
	ctx, cancel := ctx500()
	defer cancel()
	for _, v := range []string{"a\r\nb", "", "héllo wörld", strings.Repeat("x", 10000)} {
		got, err := c.Do(ctx, "ECHO", v)
		if err != nil || got != v {
			t.Errorf("echo %q: got %q, %v", v[:min(len(v), 20)], got, err)
		}
	}
}

func TestConnectionsAreReused(t *testing.T) {
	srv := newFakeServer(t, func([]string) string { return "+PONG\r\n" })
	c := New(Options{Addr: srv.addr(), PoolSize: 4})
	defer c.Close()
	ctx, cancel := ctx500()
	defer cancel()
	for i := 0; i < 20; i++ {
		if _, err := c.Do(ctx, "PING"); err != nil {
			t.Fatal(err)
		}
	}
	if n := srv.conns.Load(); n != 1 {
		t.Fatalf("20 sequential commands opened %d connections, want 1", n)
	}
}

func TestServerErrorKeepsConnectionButIOErrorDropsIt(t *testing.T) {
	srv := newFakeServer(t, func(a []string) string {
		switch a[0] {
		case "BAD":
			return "-ERR bad\r\n"
		case "HANGUP":
			return "" // close without replying
		}
		return "+OK\r\n"
	})
	c := New(Options{Addr: srv.addr(), PoolSize: 2})
	defer c.Close()
	ctx, cancel := ctx500()
	defer cancel()

	c.Do(ctx, "PING")
	c.Do(ctx, "BAD") // server error: connection is still clean and reusable
	c.Do(ctx, "PING")
	if n := srv.conns.Load(); n != 1 {
		t.Fatalf("server error should not drop the connection; conns = %d", n)
	}

	if _, err := c.Do(ctx, "HANGUP"); err == nil {
		t.Fatal("expected I/O error when server hangs up")
	}
	if _, err := c.Do(ctx, "PING"); err != nil {
		t.Fatalf("client must recover with a fresh connection: %v", err)
	}
	if n := srv.conns.Load(); n != 2 {
		t.Fatalf("broken connection should be replaced; conns = %d, want 2", n)
	}
}

func TestContextDeadlineBoundsRoundTrip(t *testing.T) {
	srv := newFakeServer(t, func([]string) string {
		time.Sleep(500 * time.Millisecond)
		return "+SLOW\r\n"
	})
	c := New(Options{Addr: srv.addr()})
	defer c.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, err := c.Do(ctx, "PING")
	if err == nil {
		t.Fatal("expected timeout")
	}
	if el := time.Since(start); el > 300*time.Millisecond {
		t.Fatalf("Do took %v; deadline was ignored", el)
	}
	var ne net.Error
	if !errors.As(err, &ne) || !ne.Timeout() {
		t.Errorf("err = %v, want a timeout", err)
	}
}

func TestDialFailureReleasesPoolSlot(t *testing.T) {
	ln, _ := net.Listen("tcp", "127.0.0.1:0")
	addr := ln.Addr().String()
	ln.Close() // nothing listens here now

	c := New(Options{Addr: addr, PoolSize: 1, DialTimeout: 50 * time.Millisecond})
	for i := 0; i < 5; i++ { // would deadlock on the 2nd call if a slot leaked
		ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
		if _, err := c.Do(ctx, "PING"); err == nil {
			t.Fatal("expected dial error")
		}
		cancel()
	}
}

func TestPoolSizeCapsConcurrentConnections(t *testing.T) {
	var inFlight, peak atomic.Int64
	srv := newFakeServer(t, func([]string) string {
		n := inFlight.Add(1)
		for {
			p := peak.Load()
			if n <= p || peak.CompareAndSwap(p, n) {
				break
			}
		}
		time.Sleep(20 * time.Millisecond)
		inFlight.Add(-1)
		return "+OK\r\n"
	})
	c := New(Options{Addr: srv.addr(), PoolSize: 3})
	defer c.Close()

	var wg sync.WaitGroup
	for i := 0; i < 30; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if _, err := c.Do(ctx, "PING"); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	if p := peak.Load(); p > 3 {
		t.Fatalf("peak concurrent server-side commands = %d, pool size is 3", p)
	}
	if n := srv.conns.Load(); n > 3 {
		t.Fatalf("opened %d connections, pool size is 3", n)
	}
}

func TestWaitingForPoolSlotHonoursContext(t *testing.T) {
	release := make(chan struct{})
	srv := newFakeServer(t, func([]string) string { <-release; return "+OK\r\n" })
	defer close(release)
	c := New(Options{Addr: srv.addr(), PoolSize: 1})
	defer c.Close()

	go func() { // occupies the only slot
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		c.Do(ctx, "PING")
	}()
	time.Sleep(50 * time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, err := c.Do(ctx, "PING")
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, want context.DeadlineExceeded", err)
	}
	if time.Since(start) > 300*time.Millisecond {
		t.Fatal("blocked on the pool past the context deadline")
	}
}

func TestScriptUsesEvalshaThenFallsBackOnNoscript(t *testing.T) {
	const src = "return redis.call('GET', KEYS[1])"
	var cmds []string
	var mu sync.Mutex
	loaded := false
	srv := newFakeServer(t, func(a []string) string {
		mu.Lock()
		defer mu.Unlock()
		cmds = append(cmds, a[0])
		switch a[0] {
		case "EVALSHA":
			if !loaded {
				return "-NOSCRIPT No matching script. Please use EVAL.\r\n"
			}
			return ":1\r\n"
		case "EVAL":
			loaded = true
			if a[1] != src {
				return "-ERR wrong source\r\n"
			}
			return ":1\r\n"
		}
		return "-ERR\r\n"
	})
	c := New(Options{Addr: srv.addr()})
	defer c.Close()
	s := NewScript(src)
	ctx, cancel := ctx500()
	defer cancel()

	for i := 0; i < 2; i++ {
		got, err := s.Run(ctx, c, []string{"k1"}, "a", 2)
		if err != nil || got != int64(1) {
			t.Fatalf("run %d = %v, %v", i, got, err)
		}
	}
	// First call: EVALSHA -> NOSCRIPT -> EVAL. Second call: EVALSHA only.
	if strings.Join(cmds, ",") != "EVALSHA,EVAL,EVALSHA" {
		t.Fatalf("command sequence = %v", cmds)
	}
}

func TestScriptSendsKeysAndArgsInOrder(t *testing.T) {
	got := make(chan []string, 1)
	srv := newFakeServer(t, func(a []string) string { got <- a; return ":0\r\n" })
	c := New(Options{Addr: srv.addr()})
	defer c.Close()
	ctx, cancel := ctx500()
	defer cancel()
	s := NewScript("return 0")
	if _, err := s.Run(ctx, c, []string{"k1", "k2"}, "a1", 5); err != nil {
		t.Fatal(err)
	}
	a := <-got
	if a[0] != "EVALSHA" || a[1] != s.sha || a[2] != "2" || strings.Join(a[3:], ",") != "k1,k2,a1,5" {
		t.Fatalf("wire command = %v", a)
	}
}

func TestScriptSHAMatchesSHA1OfSource(t *testing.T) {
	// Known vector: SHA1("return 1") — what Redis's SCRIPT LOAD would report.
	if got := NewScript("return 1").sha; got != "e0e1f9fabfc9d4800c877a703b823ac0578ff8db" {
		t.Fatalf("sha = %s", got)
	}
}

func TestScriptOtherServerErrorsAreNotRetried(t *testing.T) {
	srv := newFakeServer(t, func(a []string) string { return "-ERR Error running script\r\n" })
	c := New(Options{Addr: srv.addr()})
	defer c.Close()
	ctx, cancel := ctx500()
	defer cancel()
	if _, err := NewScript("x").Run(ctx, c, nil); err == nil {
		t.Fatal("expected error")
	}
	if n := srv.commands.Load(); n != 1 {
		t.Fatalf("non-NOSCRIPT error was retried (%d commands)", n)
	}
}

func TestInts(t *testing.T) {
	got, err := Ints([]any{int64(1), int64(0), int64(-3)}, nil)
	if err != nil || len(got) != 3 || got[0] != 1 || got[2] != -3 {
		t.Fatalf("Ints = %v, %v", got, err)
	}
	boom := errors.New("boom")
	if _, err := Ints(nil, boom); !errors.Is(err, boom) {
		t.Errorf("error should pass through, got %v", err)
	}
	if _, err := Ints("not an array", nil); err == nil {
		t.Error("expected error for non-array reply")
	}
	if _, err := Ints([]any{int64(1), "x"}, nil); err == nil {
		t.Error("expected error for non-integer element")
	}
	if _, err := Ints(nil, nil); err == nil {
		t.Error("nil reply is not an array; expected error")
	}
}

func TestReadReplyRejectsMalformedInput(t *testing.T) {
	for _, in := range []string{"?weird\r\n", "$abc\r\n", ":xyz\r\n", "*x\r\n", "+\n", "$5\r\nab"} {
		if _, err := readReply(bufio.NewReader(strings.NewReader(in))); err == nil {
			t.Errorf("readReply(%q) should fail", in)
		}
	}
}

func TestCloseDropsIdleConnections(t *testing.T) {
	srv := newFakeServer(t, func([]string) string { return "+OK\r\n" })
	c := New(Options{Addr: srv.addr()})
	ctx, cancel := ctx500()
	defer cancel()
	c.Do(ctx, "PING")
	c.Close()
	c.Do(ctx, "PING")
	if n := srv.conns.Load(); n != 2 {
		t.Fatalf("conns = %d; Close should have discarded the idle connection", n)
	}
}

func TestConcurrentUseIsRaceFree(t *testing.T) {
	srv := newFakeServer(t, func(a []string) string {
		return fmt.Sprintf("$%d\r\n%s\r\n", len(a[1]), a[1])
	})
	c := New(Options{Addr: srv.addr(), PoolSize: 8})
	defer c.Close()
	var wg sync.WaitGroup
	for g := 0; g < 16; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 50; i++ {
				ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
				want := fmt.Sprintf("g%d-i%d", g, i)
				got, err := c.Do(ctx, "ECHO", want)
				cancel()
				if err != nil || got != want { // a mixed-up reply would show here
					t.Errorf("got %v, %v; want %s", got, err, want)
					return
				}
			}
		}(g)
	}
	wg.Wait()
}
