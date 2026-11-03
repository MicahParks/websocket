//go:build !js

package websocket

import (
	"bufio"
	"bytes"
	"compress/flate"
	"context"
	"io"
	"net"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket/internal/test/assert"
	"github.com/coder/websocket/internal/test/xrand"
)

func Test_slidingWindow(t *testing.T) {
	t.Parallel()

	const testCount = 99
	const maxWindow = 99999
	for range testCount {
		t.Run("", func(t *testing.T) {
			t.Parallel()

			input := xrand.String(maxWindow)
			windowLength := xrand.Int(maxWindow)
			var sw slidingWindow
			sw.init(windowLength)
			sw.write([]byte(input))

			assert.Equal(t, "window length", windowLength, cap(sw.buf))
			if !strings.HasSuffix(input, string(sw.buf)) {
				t.Fatalf("r.buf is not a suffix of input: %q and %q", input, sw.buf)
			}
		})
	}
}

func BenchmarkFlateWriter(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		w, _ := flate.NewWriter(io.Discard, flate.BestSpeed)
		// We have to write a byte to get the writer to allocate to its full extent.
		w.Write([]byte{'a'})
		w.Flush()
	}
}

func BenchmarkFlateReader(b *testing.B) {
	b.ReportAllocs()

	var buf bytes.Buffer
	w, _ := flate.NewWriter(&buf, flate.BestSpeed)
	w.Write([]byte{'a'})
	w.Flush()

	for i := 0; i < b.N; i++ {
		r := flate.NewReader(bytes.NewReader(buf.Bytes()))
		io.ReadAll(r)
	}
}

// TestWriteSingleFrameCompressed verifies that Conn.Write sends compressed
// messages in a single frame instead of multiple frames, and that messages
// below the flateThreshold are sent uncompressed.
// This is a regression test for https://github.com/coder/websocket/issues/435
func TestWriteSingleFrameCompressed(t *testing.T) {
	t.Parallel()

	var (
		flateThreshold = 64

		largeMsg = []byte(strings.Repeat("hello world ", 100))
		smallMsg = []byte("small message")
	)

	testCases := []struct {
		name     string
		mode     CompressionMode
		msg      []byte
		wantRsv1 bool // true = compressed, false = uncompressed
	}{
		{"ContextTakeover/AboveThreshold", CompressionContextTakeover, largeMsg, true},
		{"NoContextTakeover/AboveThreshold", CompressionNoContextTakeover, largeMsg, true},
		{"ContextTakeover/BelowThreshold", CompressionContextTakeover, smallMsg, false},
		{"NoContextTakeover/BelowThreshold", CompressionNoContextTakeover, smallMsg, false},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			clientConn, serverConn := net.Pipe()
			defer clientConn.Close()
			defer serverConn.Close()

			c := newConn(connConfig{
				rwc:            clientConn,
				client:         true,
				copts:          tc.mode.opts(),
				flateThreshold: flateThreshold,
				br:             bufio.NewReader(clientConn),
				bw:             bufio.NewWriterSize(clientConn, 4096),
			})

			ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*100)
			defer cancel()

			writeDone := make(chan error, 1)
			go func() {
				writeDone <- c.Write(ctx, MessageText, tc.msg)
			}()

			reader := bufio.NewReader(serverConn)
			readBuf := make([]byte, 8)

			h, err := readFrameHeader(reader, readBuf)
			assert.Success(t, err)

			_, err = io.CopyN(io.Discard, reader, h.payloadLength)
			assert.Success(t, err)

			assert.Equal(t, "opcode", opText, h.opcode)
			assert.Equal(t, "rsv1 (compressed)", tc.wantRsv1, h.rsv1)
			assert.Equal(t, "fin", true, h.fin)

			err = <-writeDone
			assert.Success(t, err)
		})
	}
}

// TestWriteThenWriterContextTakeover verifies that using Conn.Write followed by
// Conn.Writer works correctly with context takeover enabled. This tests that
// the flateWriter destination is properly restored after Conn.Write redirects
// it to a temporary buffer.
func TestWriteThenWriterContextTakeover(t *testing.T) {
	t.Parallel()

	clientConn, serverConn := net.Pipe()
	defer clientConn.Close()
	defer serverConn.Close()

	client := newConn(connConfig{
		rwc:            clientConn,
		client:         true,
		copts:          CompressionContextTakeover.opts(),
		flateThreshold: 64,
		br:             bufio.NewReader(clientConn),
		bw:             bufio.NewWriterSize(clientConn, 4096),
	})

	server := newConn(connConfig{
		rwc:            serverConn,
		client:         false,
		copts:          CompressionContextTakeover.opts(),
		flateThreshold: 64,
		br:             bufio.NewReader(serverConn),
		bw:             bufio.NewWriterSize(serverConn, 4096),
	})

	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*500)
	defer cancel()

	msg1 := []byte(strings.Repeat("first message ", 100))
	msg2 := []byte(strings.Repeat("second message ", 100))

	type readResult struct {
		typ MessageType
		p   []byte
		err error
	}
	readCh := make(chan readResult, 3)
	go func() {
		for range 3 {
			typ, p, err := server.Read(ctx)
			readCh <- readResult{typ, p, err}
		}
	}()

	// We want to verify mixing `Write` and `Writer` usages still work.
	//
	// To this end, we call them in this order:
	// - `Write`
	// - `Writer`
	// - `Write`
	//
	// This verifies that it works for a `Write` followed by a `Writer`
	// as well as a `Writer` followed by a `Write`.

	// 1. `Write` API
	err := client.Write(ctx, MessageText, msg1)
	assert.Success(t, err)

	r := <-readCh
	assert.Success(t, r.err)
	assert.Equal(t, "Write type", MessageText, r.typ)
	assert.Equal(t, "Write content", string(msg1), string(r.p))

	// 2. `Writer` API
	w, err := client.Writer(ctx, MessageBinary)
	assert.Success(t, err)
	_, err = w.Write(msg2)
	assert.Success(t, err)
	assert.Success(t, w.Close())

	r = <-readCh
	assert.Success(t, r.err)
	assert.Equal(t, "Writer type", MessageBinary, r.typ)
	assert.Equal(t, "Writer content", string(msg2), string(r.p))

	// 3. `Write` API again
	err = client.Write(ctx, MessageText, msg1)
	assert.Success(t, err)

	r = <-readCh
	assert.Success(t, r.err)
	assert.Equal(t, "Write type", MessageText, r.typ)
	assert.Equal(t, "Write content", string(msg1), string(r.p))
}

// TestCompressionDictionaryPreserved verifies that context takeover mode
// preserves the compression dictionary across Conn.Write calls, resulting
// in better compression for consecutive similar messages.
func TestCompressionDictionaryPreserved(t *testing.T) {
	t.Parallel()

	msg := []byte(strings.Repeat(`{"type":"event","data":"value"}`, 50))

	takeoverClient, takeoverServer := net.Pipe()
	defer takeoverClient.Close()
	defer takeoverServer.Close()

	withTakeover := newConn(connConfig{
		rwc:            takeoverClient,
		client:         true,
		copts:          CompressionContextTakeover.opts(),
		flateThreshold: 64,
		br:             bufio.NewReader(takeoverClient),
		bw:             bufio.NewWriterSize(takeoverClient, 4096),
	})

	noTakeoverClient, noTakeoverServer := net.Pipe()
	defer noTakeoverClient.Close()
	defer noTakeoverServer.Close()

	withoutTakeover := newConn(connConfig{
		rwc:            noTakeoverClient,
		client:         true,
		copts:          CompressionNoContextTakeover.opts(),
		flateThreshold: 64,
		br:             bufio.NewReader(noTakeoverClient),
		bw:             bufio.NewWriterSize(noTakeoverClient, 4096),
	})

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	// Capture compressed sizes for both modes
	var withTakeoverSizes, withoutTakeoverSizes []int64

	reader1 := bufio.NewReader(takeoverServer)
	reader2 := bufio.NewReader(noTakeoverServer)
	readBuf := make([]byte, 8)

	// Send 3 identical messages each
	for range 3 {
		// With context takeover
		writeDone1 := make(chan error, 1)
		go func() {
			writeDone1 <- withTakeover.Write(ctx, MessageText, msg)
		}()

		h1, err := readFrameHeader(reader1, readBuf)
		assert.Success(t, err)

		_, err = io.CopyN(io.Discard, reader1, h1.payloadLength)
		assert.Success(t, err)

		withTakeoverSizes = append(withTakeoverSizes, h1.payloadLength)
		assert.Success(t, <-writeDone1)

		// Without context takeover
		writeDone2 := make(chan error, 1)
		go func() {
			writeDone2 <- withoutTakeover.Write(ctx, MessageText, msg)
		}()

		h2, err := readFrameHeader(reader2, readBuf)
		assert.Success(t, err)

		_, err = io.CopyN(io.Discard, reader2, h2.payloadLength)
		assert.Success(t, err)

		withoutTakeoverSizes = append(withoutTakeoverSizes, h2.payloadLength)
		assert.Success(t, <-writeDone2)
	}

	// With context takeover, the 2nd and 3rd messages should be smaller than
	// without context takeover (dictionary helps compress repeated patterns).
	// The first message will be similar size for both modes since there's no
	// prior dictionary. But subsequent messages benefit from context takeover.
	if withTakeoverSizes[2] >= withoutTakeoverSizes[2] {
		t.Errorf("context takeover should compress better: with=%d, without=%d",
			withTakeoverSizes[2], withoutTakeoverSizes[2])
	}
}

// deflateSync compresses p as a DEFLATE stream ending in a sync flush, as a
// caller of Conn.WriteDeflated produces one.
func deflateSync(t testing.TB, p []byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	fw, err := flate.NewWriter(&buf, flate.BestSpeed)
	assert.Success(t, err)
	_, err = fw.Write(p)
	assert.Success(t, err)
	assert.Success(t, fw.Flush())
	return buf.Bytes()
}

// pipeConns returns a client and a server joined by net.Pipe, both with
// copts, closed when the test ends.
func pipeConns(t testing.TB, copts *compressionOptions) (client, server *Conn) {
	t.Helper()
	clientConn, serverConn := net.Pipe()
	t.Cleanup(func() {
		clientConn.Close()
		serverConn.Close()
	})
	client = newConn(connConfig{
		rwc:            clientConn,
		client:         true,
		copts:          copts,
		flateThreshold: 64,
		br:             bufio.NewReader(clientConn),
		bw:             bufio.NewWriterSize(clientConn, 4096),
	})
	server = newConn(connConfig{
		rwc:            serverConn,
		client:         false,
		copts:          copts,
		flateThreshold: 64,
		br:             bufio.NewReader(serverConn),
		bw:             bufio.NewWriterSize(serverConn, 4096),
	})
	return client, server
}

// TestWriteDeflated verifies that the peer reads back what the caller
// compressed, from either side, in one part, as a prefix compressed once with
// a suffix per message, with the sync flush tail split across parts, and
// followed by a regular Write.
func TestWriteDeflated(t *testing.T) {
	t.Parallel()

	msg := []byte(strings.Repeat("hello world ", 100))
	shared := []byte(strings.Repeat("shared state ", 100))
	suffixes := [][]byte{
		[]byte(strings.Repeat("first ", 20)),
		[]byte(strings.Repeat("second ", 20)),
	}

	testCases := []struct {
		name  string
		typ   MessageType
		write func(t *testing.T, ctx context.Context, c *Conn)
		want  [][]byte
	}{
		{"OnePart", MessageBinary, func(t *testing.T, ctx context.Context, c *Conn) {
			assert.Success(t, c.WriteDeflated(ctx, MessageBinary, deflateSync(t, msg)))
		}, [][]byte{msg}},
		{"SharedPrefix", MessageText, func(t *testing.T, ctx context.Context, c *Conn) {
			prefix := deflateSync(t, shared)
			for _, suffix := range suffixes {
				assert.Success(t, c.WriteDeflated(ctx, MessageText, prefix, deflateSync(t, suffix)))
			}
			// The parts are the caller's to reuse.
			assert.Equal(t, "prefix", string(deflateSync(t, shared)), string(prefix))
		}, [][]byte{slices.Concat(shared, suffixes[0]), slices.Concat(shared, suffixes[1])}},
		{"TailSplit", MessageBinary, func(t *testing.T, ctx context.Context, c *Conn) {
			stream := deflateSync(t, msg)
			n := len(stream)
			assert.Success(t, c.WriteDeflated(ctx, MessageBinary, stream[:n-2], nil, stream[n-2:n-1], stream[n-1:]))
		}, [][]byte{msg}},
		{"ThenWrite", MessageBinary, func(t *testing.T, ctx context.Context, c *Conn) {
			assert.Success(t, c.WriteDeflated(ctx, MessageBinary, deflateSync(t, msg)))
			assert.Success(t, c.Write(ctx, MessageBinary, msg))
		}, [][]byte{msg, msg}},
	}

	for _, side := range []string{"server", "client"} {
		for _, tc := range testCases {
			t.Run(side+"/"+tc.name, func(t *testing.T) {
				t.Parallel()

				client, server := pipeConns(t, CompressionNoContextTakeover.opts())
				w, r := server, client
				if side == "client" {
					w, r = client, server
				}

				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				defer cancel()

				type readResult struct {
					typ MessageType
					p   []byte
					err error
				}
				readCh := make(chan readResult, len(tc.want))
				go func() {
					for range tc.want {
						typ, p, err := r.Read(ctx)
						readCh <- readResult{typ, p, err}
					}
				}()

				tc.write(t, ctx, w)

				for _, want := range tc.want {
					got := <-readCh
					assert.Success(t, got.err)
					assert.Equal(t, "type", tc.typ, got.typ)
					if !bytes.Equal(want, got.p) {
						t.Fatalf("read %d bytes, want %d", len(got.p), len(want))
					}
				}
			})
		}
	}
}

func TestWriteDeflatedRefused(t *testing.T) {
	t.Parallel()

	stream := deflateSync(t, []byte(strings.Repeat("hello world ", 100)))

	testCases := []struct {
		name  string
		copts *compressionOptions
		parts [][]byte
		want  string
	}{
		{"CompressionDisabled", nil, [][]byte{stream}, "without context takeover"},
		{"ContextTakeover", CompressionContextTakeover.opts(), [][]byte{stream}, "without context takeover"},
		{"NoTail", CompressionNoContextTakeover.opts(), [][]byte{stream[:len(stream)-4]}, "sync flush"},
		{"Short", CompressionNoContextTakeover.opts(), [][]byte{stream[:2], nil}, "sync flush"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			_, server := pipeConns(t, tc.copts)

			ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*100)
			defer cancel()

			err := server.WriteDeflated(ctx, MessageBinary, tc.parts...)
			assert.Error(t, err)
			assert.Contains(t, err, tc.want)
		})
	}
}

// TestWriteDeflatedFrame verifies the frame on the wire: one final frame with
// rsv1 set, the tail removed, and on a client the masking key carried across
// parts whose lengths are not multiples of four.
func TestWriteDeflatedFrame(t *testing.T) {
	t.Parallel()

	stream := deflateSync(t, []byte(strings.Repeat("hello world ", 100)))
	payload := stream[:len(stream)-4]

	for _, client := range []bool{false, true} {
		name := "server"
		if client {
			name = "client"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			writerConn, peerConn := net.Pipe()
			defer writerConn.Close()
			defer peerConn.Close()

			c := newConn(connConfig{
				rwc:    writerConn,
				client: client,
				copts:  CompressionNoContextTakeover.opts(),
				br:     bufio.NewReader(writerConn),
				bw:     bufio.NewWriterSize(writerConn, 4096),
			})

			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()

			writeDone := make(chan error, 1)
			go func() {
				writeDone <- c.WriteDeflated(ctx, MessageText, stream[:5], stream[5:11], stream[11:])
			}()

			reader := bufio.NewReader(peerConn)
			h, err := readFrameHeader(reader, make([]byte, 8))
			assert.Success(t, err)
			assert.Equal(t, "fin", true, h.fin)
			assert.Equal(t, "rsv1", true, h.rsv1)
			assert.Equal(t, "opcode", opText, h.opcode)
			assert.Equal(t, "masked", client, h.masked)
			assert.Equal(t, "payload length", int64(len(payload)), h.payloadLength)

			got := make([]byte, h.payloadLength)
			_, err = io.ReadFull(reader, got)
			assert.Success(t, err)
			if h.masked {
				mask(got, h.maskKey)
			}
			if !bytes.Equal(payload, got) {
				t.Fatal("payload differs from the stream without its tail")
			}

			assert.Success(t, <-writeDone)
		})
	}
}

func TestWriteCompression(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name           string
		copts          *compressionOptions
		client, server CompressionMode
	}{
		{"Disabled", nil, CompressionDisabled, CompressionDisabled},
		{"ContextTakeover", CompressionContextTakeover.opts(), CompressionContextTakeover, CompressionContextTakeover},
		{"NoContextTakeover", CompressionNoContextTakeover.opts(), CompressionNoContextTakeover, CompressionNoContextTakeover},
		{"ClientNoContextTakeover", &compressionOptions{clientNoContextTakeover: true}, CompressionNoContextTakeover, CompressionContextTakeover},
		{"ServerNoContextTakeover", &compressionOptions{serverNoContextTakeover: true}, CompressionContextTakeover, CompressionNoContextTakeover},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			client, server := pipeConns(t, tc.copts)
			assert.Equal(t, "client", tc.client, client.WriteCompression())
			assert.Equal(t, "server", tc.server, server.WriteCompression())
		})
	}
}

// BenchmarkWriteDeflated broadcasts a shared prefix with a suffix per
// connection to 16 server connections compressing without context takeover.
// Write compresses prefix and suffix per connection; WriteDeflated compresses
// the prefix once and the suffix per connection.
func BenchmarkWriteDeflated(b *testing.B) {
	const conns = 16

	prefix := []byte(strings.Repeat("shared state ", 80))
	suffix := []byte(strings.Repeat("mine ", 16))

	ctx := context.Background()
	writers := make([]*Conn, conns)
	for i := range writers {
		clientConn, serverConn := net.Pipe()
		b.Cleanup(func() {
			clientConn.Close()
			serverConn.Close()
		})
		writers[i] = newConn(connConfig{
			rwc:   serverConn,
			copts: CompressionNoContextTakeover.opts(),
			br:    bufio.NewReader(serverConn),
			bw:    bufio.NewWriterSize(io.Discard, 4096),
		})
	}

	b.Run("write", func(b *testing.B) {
		msg := make([]byte, 0, len(prefix)+len(suffix))
		b.SetBytes(int64((len(prefix) + len(suffix)) * conns))
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			msg = append(append(msg[:0], prefix...), suffix...)
			for _, c := range writers {
				err := c.Write(ctx, MessageBinary, msg)
				if err != nil {
					b.Fatal(err)
				}
			}
		}
	})

	b.Run("writeDeflated", func(b *testing.B) {
		var prefixBuf, suffixBuf bytes.Buffer
		fw, err := flate.NewWriter(io.Discard, flate.BestSpeed)
		if err != nil {
			b.Fatal(err)
		}
		deflate := func(buf *bytes.Buffer, p []byte) []byte {
			buf.Reset()
			fw.Reset(buf)
			fw.Write(p)
			fw.Flush()
			return buf.Bytes()
		}
		b.SetBytes(int64((len(prefix) + len(suffix)) * conns))
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			shared := deflate(&prefixBuf, prefix)
			for _, c := range writers {
				err := c.WriteDeflated(ctx, MessageBinary, shared, deflate(&suffixBuf, suffix))
				if err != nil {
					b.Fatal(err)
				}
			}
		}
	})
}
