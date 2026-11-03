//go:build !js

package websocket

import (
	"bufio"
	"compress/flate"
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"time"

	"github.com/coder/websocket/internal/bpool"
	"github.com/coder/websocket/internal/errd"
	"github.com/coder/websocket/internal/util"
)

// Writer returns a writer bounded by the context that will write
// a WebSocket message of type dataType to the connection.
//
// You must close the writer once you have written the entire message.
//
// Only one writer can be open at a time, multiple calls will block until the previous writer
// is closed.
func (c *Conn) Writer(ctx context.Context, typ MessageType) (io.WriteCloser, error) {
	w, err := c.writer(ctx, typ)
	if err != nil {
		return nil, fmt.Errorf("failed to get writer: %w", err)
	}
	return w, nil
}

// Write writes a message to the connection.
//
// See the Writer method if you want to stream a message.
//
// If compression is disabled or the compression threshold is not met, then it
// will write the message in a single frame.
func (c *Conn) Write(ctx context.Context, typ MessageType, p []byte) error {
	_, err := c.write(ctx, typ, p)
	if err != nil {
		return fmt.Errorf("failed to write msg: %w", err)
	}
	return nil
}

// WriteCompression returns the compression mode of the messages this side
// writes: CompressionDisabled when the peer negotiated no compression,
// otherwise the mode of this direction alone, as the peer may negotiate no
// context takeover for one direction only.
func (c *Conn) WriteCompression() CompressionMode {
	if !c.flate() {
		return CompressionDisabled
	}
	if c.msgWriter.flateContextTakeover() {
		return CompressionContextTakeover
	}
	return CompressionNoContextTakeover
}

// WriteDeflated writes a message the caller compressed, such as a broadcast
// compressed once for every connection, or a prefix compressed once with a
// suffix compressed per connection. The parts joined must be a raw DEFLATE
// stream ending in a sync flush, as flate.Writer then Flush produce. It is
// written as one frame with the trailing 0x00 0x00 0xff 0xff removed, per
// RFC 7692 section 7.2.1, without copying the parts together. Only that tail
// is checked; a malformed stream fails on the peer.
//
// It errors unless WriteCompression is CompressionNoContextTakeover: with
// context takeover the peer inflates against the window of this connection's
// own compression, and without compression the peer cannot inflate.
func (c *Conn) WriteDeflated(ctx context.Context, typ MessageType, parts ...[]byte) error {
	err := c.writeDeflated(ctx, typ, parts)
	if err != nil {
		return fmt.Errorf("failed to write deflated msg: %w", err)
	}
	return nil
}

func (c *Conn) writeDeflated(ctx context.Context, typ MessageType, parts [][]byte) error {
	if c.WriteCompression() != CompressionNoContextTakeover {
		return errors.New("writes are not compressed without context takeover")
	}
	if !endsInDeflateTail(parts) {
		return errors.New("deflated message does not end in a sync flush")
	}
	n := -len(deflateMessageTail)
	for _, p := range parts {
		n += len(p)
	}

	err := c.msgWriter.reset(ctx, typ)
	if err != nil {
		return err
	}
	defer c.msgWriter.mu.unlock()

	_, err = c.writeFrameParts(ctx, true, true, c.msgWriter.opcode, n, parts...)
	return err
}

type msgWriter struct {
	c *Conn

	mu      *mu
	writeMu *mu
	closed  bool

	ctx    context.Context
	opcode opcode
	flate  bool

	trimWriter  *trimLastFourBytesWriter
	flateWriter *flate.Writer
}

func newMsgWriter(c *Conn) *msgWriter {
	mw := &msgWriter{
		c:       c,
		mu:      newMu(c),
		writeMu: newMu(c),
	}
	return mw
}

func (mw *msgWriter) ensureFlate() {
	if mw.trimWriter == nil {
		mw.trimWriter = &trimLastFourBytesWriter{
			w: util.WriterFunc(mw.write),
		}
	}

	if mw.flateWriter == nil {
		mw.flateWriter = getFlateWriter(mw.trimWriter)
	}
	mw.flate = true
}

func (mw *msgWriter) flateContextTakeover() bool {
	if mw.c.client {
		return !mw.c.copts.clientNoContextTakeover
	}
	return !mw.c.copts.serverNoContextTakeover
}

func (c *Conn) writer(ctx context.Context, typ MessageType) (io.WriteCloser, error) {
	err := c.msgWriter.reset(ctx, typ)
	if err != nil {
		return nil, err
	}
	return c.msgWriter, nil
}

func (c *Conn) write(ctx context.Context, typ MessageType, p []byte) (int, error) {
	err := c.msgWriter.reset(ctx, typ)
	if err != nil {
		return 0, err
	}
	defer c.msgWriter.mu.unlock()

	if !c.flate() || len(p) < c.flateThreshold {
		return c.writeFrame(ctx, true, false, c.msgWriter.opcode, p)
	}

	return c.msgWriter.writeCompressedFrame(ctx, p)
}

func (mw *msgWriter) reset(ctx context.Context, typ MessageType) error {
	err := mw.mu.lock(ctx)
	if err != nil {
		return err
	}

	mw.ctx = ctx
	mw.opcode = opcode(typ)
	mw.flate = false
	mw.closed = false

	mw.trimWriter.reset()

	return nil
}

func (mw *msgWriter) putFlateWriter() {
	if mw.flateWriter != nil {
		putFlateWriter(mw.flateWriter)
		mw.flateWriter = nil
	}
}

// writeCompressedFrame compresses and writes p as a single frame.
func (mw *msgWriter) writeCompressedFrame(ctx context.Context, p []byte) (int, error) {
	err := mw.writeMu.lock(mw.ctx)
	if err != nil {
		return 0, fmt.Errorf("failed to write: %w", err)
	}
	defer mw.writeMu.unlock()

	if mw.closed {
		return 0, errors.New("cannot use closed writer")
	}

	mw.ensureFlate()

	buf := bpool.Get()
	defer bpool.Put(buf)

	// Buffer compressed output so we can write as
	// a single frame instead of chunked frames.
	origWriter := mw.trimWriter.w
	mw.trimWriter.w = buf
	defer func() {
		mw.trimWriter.w = origWriter
	}()

	_, err = mw.flateWriter.Write(p)
	if err != nil {
		return 0, fmt.Errorf("failed to compress: %w", err)
	}

	err = mw.flateWriter.Flush()
	if err != nil {
		return 0, fmt.Errorf("failed to flush compression: %w", err)
	}

	mw.trimWriter.reset()

	if !mw.flateContextTakeover() {
		mw.putFlateWriter()
	}

	mw.closed = true

	_, err = mw.c.writeFrame(ctx, true, true, mw.opcode, buf.Bytes())
	if err != nil {
		return 0, err
	}
	return len(p), nil
}

// Write writes the given bytes to the WebSocket connection.
func (mw *msgWriter) Write(p []byte) (_ int, err error) {
	err = mw.writeMu.lock(mw.ctx)
	if err != nil {
		return 0, fmt.Errorf("failed to write: %w", err)
	}
	defer mw.writeMu.unlock()

	if mw.closed {
		return 0, errors.New("cannot use closed writer")
	}

	defer func() {
		if err != nil {
			err = fmt.Errorf("failed to write: %w", err)
		}
	}()

	if mw.c.flate() {
		// Only enables flate if the length crosses the
		// threshold on the first frame
		if mw.opcode != opContinuation && len(p) >= mw.c.flateThreshold {
			mw.ensureFlate()
		}
	}

	if mw.flate {
		return mw.flateWriter.Write(p)
	}

	return mw.write(p)
}

func (mw *msgWriter) write(p []byte) (int, error) {
	n, err := mw.c.writeFrame(mw.ctx, false, mw.flate, mw.opcode, p)
	if err != nil {
		return n, fmt.Errorf("failed to write data frame: %w", err)
	}
	mw.opcode = opContinuation
	return n, nil
}

// Close flushes the frame to the connection.
func (mw *msgWriter) Close() (err error) {
	defer errd.Wrap(&err, "failed to close writer")

	err = mw.writeMu.lock(mw.ctx)
	if err != nil {
		return err
	}
	defer mw.writeMu.unlock()

	if mw.closed {
		return errors.New("writer already closed")
	}
	mw.closed = true

	if mw.flate {
		err = mw.flateWriter.Flush()
		if err != nil {
			return fmt.Errorf("failed to flush flate: %w", err)
		}
	}

	_, err = mw.c.writeFrame(mw.ctx, true, mw.flate, mw.opcode, nil)
	if err != nil {
		return fmt.Errorf("failed to write fin frame: %w", err)
	}

	if mw.flate && !mw.flateContextTakeover() {
		mw.putFlateWriter()
	}
	mw.mu.unlock()
	return nil
}

func (mw *msgWriter) close() {
	if mw.c.client {
		mw.c.writeFrameMu.forceLock()
		putBufioWriter(mw.c.bw)
	}

	mw.writeMu.forceLock()
	mw.putFlateWriter()
}

func (c *Conn) writeControl(ctx context.Context, opcode opcode, p []byte) error {
	ctx, cancel := context.WithTimeout(ctx, time.Second*5)
	defer cancel()

	_, err := c.writeFrame(ctx, true, false, opcode, p)
	if err != nil {
		return fmt.Errorf("failed to write control frame %v: %w", opcode, err)
	}
	return nil
}

func (c *Conn) writeFrame(ctx context.Context, fin bool, flate bool, opcode opcode, p []byte) (int, error) {
	return c.writeFrameParts(ctx, fin, flate, opcode, len(p), p)
}

// writeFrameParts handles all writes to the connection: one frame whose
// payload is the first n bytes of parts joined, so bytes shared between
// connections are never copied.
func (c *Conn) writeFrameParts(ctx context.Context, fin bool, flate bool, opcode opcode, n int, parts ...[]byte) (_ int, err error) {
	err = c.writeFrameMu.lock(ctx)
	if err != nil {
		return 0, err
	}
	defer c.writeFrameMu.unlock()

	defer func() {
		if c.isClosed() && opcode == opClose {
			err = nil
		}
		if err != nil {
			if ctx.Err() != nil {
				err = ctx.Err()
			} else if c.isClosed() {
				err = net.ErrClosed
			}
			err = fmt.Errorf("failed to write frame: %w", err)
		}
	}()

	c.closeStateMu.Lock()
	closeSentErr := c.closeSentErr
	c.closeStateMu.Unlock()
	if closeSentErr != nil {
		return 0, net.ErrClosed
	}

	select {
	case <-c.closed:
		return 0, net.ErrClosed
	default:
	}
	if c.setupWriteTimeout(ctx) {
		defer c.clearWriteTimeout()
	}

	c.writeHeader.fin = fin
	c.writeHeader.opcode = opcode
	c.writeHeader.payloadLength = int64(n)

	if c.client {
		c.writeHeader.masked = true
		_, err = io.ReadFull(rand.Reader, c.writeHeaderBuf[:4])
		if err != nil {
			return 0, fmt.Errorf("failed to generate masking key: %w", err)
		}
		c.writeHeader.maskKey = binary.LittleEndian.Uint32(c.writeHeaderBuf[:])
	}

	c.writeHeader.rsv1 = false
	if flate && (opcode == opText || opcode == opBinary) {
		c.writeHeader.rsv1 = true
	}

	err = writeFrameHeader(c.writeHeader, c.bw, c.writeHeaderBuf[:])
	if err != nil {
		return 0, err
	}

	written, maskKey := 0, c.writeHeader.maskKey
	for _, p := range parts {
		p = p[:min(len(p), n-written)]
		var m int
		m, maskKey, err = c.writeFramePayload(p, maskKey)
		written += m
		if err != nil {
			return written, err
		}
	}

	if c.writeHeader.fin {
		err = c.bw.Flush()
		if err != nil {
			return written, fmt.Errorf("failed to flush: %w", err)
		}
	}

	if opcode == opClose {
		c.closeStateMu.Lock()
		c.closeSentErr = fmt.Errorf("sent close frame: %w", net.ErrClosed)
		closeReceived := c.closeReceivedErr != nil
		c.closeStateMu.Unlock()

		if closeReceived && !c.casClosing() {
			c.writeFrameMu.unlock()
			_ = c.close()
		}
	}

	return written, nil
}

// writeFramePayload writes p, masked with maskKey on a masked frame, and
// returns the key for the bytes that follow p in the frame.
func (c *Conn) writeFramePayload(p []byte, maskKey uint32) (n int, _ uint32, err error) {
	defer errd.Wrap(&err, "failed to write frame payload")

	if !c.writeHeader.masked {
		n, err = c.bw.Write(p)
		return n, maskKey, err
	}

	for len(p) > 0 {
		// If the buffer is full, we need to flush.
		if c.bw.Available() == 0 {
			err = c.bw.Flush()
			if err != nil {
				return n, maskKey, err
			}
		}

		// Start of next write in the buffer.
		i := c.bw.Buffered()

		j := min(len(p), c.bw.Available())

		_, err := c.bw.Write(p[:j])
		if err != nil {
			return n, maskKey, err
		}

		maskKey = mask(c.writeBuf[i:c.bw.Buffered()], maskKey)

		p = p[j:]
		n += j
	}

	return n, maskKey, nil
}

// extractBufioWriterBuf grabs the []byte backing a *bufio.Writer
// and returns it.
func extractBufioWriterBuf(bw *bufio.Writer, w io.Writer) []byte {
	var writeBuf []byte
	bw.Reset(util.WriterFunc(func(p2 []byte) (int, error) {
		writeBuf = p2[:cap(p2)]
		return len(p2), nil
	}))

	bw.WriteByte(0)
	bw.Flush()

	bw.Reset(w)

	return writeBuf
}

func (c *Conn) writeError(code StatusCode, err error) {
	c.writeClose(code, err.Error())
}
