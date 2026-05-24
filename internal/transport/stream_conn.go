package transport

import (
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"time"
)

// ErrStreamClosed is returned when an operation is attempted on a closed stream.
type ErrStreamClosed struct{}

func (ErrStreamClosed) Error() string   { return "stream closed" }
func (ErrStreamClosed) Timeout() bool   { return false }
func (ErrStreamClosed) Temporary() bool { return false }

var errStreamClosed error = ErrStreamClosed{}

type timeoutError struct {
	op string
}

func (e timeoutError) Error() string {
	if e.op == "" {
		return "i/o timeout"
	}

	return e.op + ": i/o timeout"
}

func (e timeoutError) Timeout() bool {
	return true
}

func (e timeoutError) Temporary() bool {
	return true
}

type streamAddr struct {
	network string
	address string
}

func (a streamAddr) Network() string {
	return a.network
}

func (a streamAddr) String() string {
	return a.address
}

// streamConn adapts exec/WebSocket streams to net.Conn.
//
// Writes are serialized through writePump. Reads are provided by the remote
// stream reader through enqueueRead.
type streamConn struct {
	ctx    context.Context
	cancel context.CancelFunc

	localAddr  net.Addr
	remoteAddr net.Addr

	send    func([]byte) error
	closeFn func() error

	readCh  chan []byte
	writeCh chan []byte

	// readMu serializes concurrent Read calls and protects pending.
	readMu  sync.Mutex
	pending []byte

	localDone  chan struct{}
	remoteDone chan struct{}

	closeOnce       sync.Once
	remoteCloseOnce sync.Once

	remoteErrMu sync.Mutex
	remoteErr   error

	// readClosed guards against send-on-closed-channel in enqueueRead.
	readClosedMu sync.Mutex
	readClosed   bool

	// Deadline state: instead of spawning a goroutine per SetXxxDeadline call,
	// we store the deadline time and a change-notification channel. Pending
	// Read/Write operations re-evaluate the timer whenever the channel fires.
	deadlineMu           sync.RWMutex
	readDeadline         time.Time
	writeDeadline        time.Time
	readDeadlineChanged  chan struct{}
	writeDeadlineChanged chan struct{}
}

func newStreamConn(
	parent context.Context,
	localAddr net.Addr,
	remoteAddr net.Addr,
	send func([]byte) error,
	closeFn func() error,
) *streamConn {
	ctx, cancel := context.WithCancel(parent)

	c := &streamConn{
		ctx:                  ctx,
		cancel:               cancel,
		localAddr:            localAddr,
		remoteAddr:           remoteAddr,
		send:                 send,
		closeFn:              closeFn,
		readCh:               make(chan []byte, 64),
		writeCh:              make(chan []byte, 64),
		localDone:            make(chan struct{}),
		remoteDone:           make(chan struct{}),
		readDeadlineChanged:  make(chan struct{}),
		writeDeadlineChanged: make(chan struct{}),
	}

	go c.writePump()

	return c
}

// deadlineTimer returns a channel that fires when deadline t is reached, plus a
// stop function. If t is zero, returns nil (no deadline). If t is already past,
// returns a pre-closed channel. The caller must call stop() when done.
func deadlineTimer(t time.Time) (<-chan time.Time, func()) {
	if t.IsZero() {
		return nil, func() {}
	}
	d := time.Until(t)
	if d <= 0 {
		expired := make(chan time.Time)
		close(expired)
		return expired, func() {}
	}
	timer := time.NewTimer(d)
	return timer.C, func() {
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
	}
}

func (c *streamConn) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}

	c.readMu.Lock()
	defer c.readMu.Unlock()

	if len(c.pending) > 0 {
		n := copy(p, c.pending)
		c.pending = c.pending[n:]
		return n, nil
	}

	// Loop so that a SetReadDeadline call while blocked re-arms the timer.
	for {
		c.deadlineMu.RLock()
		dl := c.readDeadline
		changed := c.readDeadlineChanged
		c.deadlineMu.RUnlock()

		timerC, stopTimer := deadlineTimer(dl)

		select {
		case b, ok := <-c.readCh:
			stopTimer()
			if !ok {
				return 0, c.remoteReadErr()
			}
			n := copy(p, b)
			if n < len(b) {
				c.pending = append(c.pending[:0], b[n:]...)
			}
			return n, nil

		case <-timerC:
			stopTimer()
			return 0, timeoutError{op: "read"}

		case <-changed:
			// Deadline was updated; re-evaluate.
			stopTimer()

		case <-c.localDone:
			stopTimer()
			return 0, net.ErrClosed

		case <-c.ctx.Done():
			stopTimer()
			return 0, net.ErrClosed
		}
	}
}

func (c *streamConn) Write(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}

	b := make([]byte, len(p))
	copy(b, p)

	// Loop so that a SetWriteDeadline call while blocked re-arms the timer.
	for {
		c.deadlineMu.RLock()
		dl := c.writeDeadline
		changed := c.writeDeadlineChanged
		c.deadlineMu.RUnlock()

		timerC, stopTimer := deadlineTimer(dl)

		select {
		case c.writeCh <- b:
			stopTimer()
			return len(p), nil

		case <-timerC:
			stopTimer()
			return 0, timeoutError{op: "write"}

		case <-changed:
			// Deadline was updated; re-evaluate.
			stopTimer()

		case <-c.localDone:
			stopTimer()
			return 0, net.ErrClosed

		case <-c.remoteDone:
			stopTimer()
			return 0, errStreamClosed

		case <-c.ctx.Done():
			stopTimer()
			return 0, net.ErrClosed
		}
	}
}

func (c *streamConn) Close() error {
	c.closeOnce.Do(func() {
		c.cancel()

		if c.closeFn != nil {
			_ = c.closeFn()
		}

		close(c.localDone)
	})

	return nil
}

func (c *streamConn) LocalAddr() net.Addr {
	return c.localAddr
}

func (c *streamConn) RemoteAddr() net.Addr {
	return c.remoteAddr
}

func (c *streamConn) SetDeadline(t time.Time) error {
	_ = c.SetReadDeadline(t)
	_ = c.SetWriteDeadline(t)
	return nil
}

// SetReadDeadline implements net.Conn. Setting a past time unblocks any
// blocked Read immediately; setting zero removes the deadline.
// No goroutines are spawned: the deadline is stored and a change-notification
// channel is closed so pending Read calls re-evaluate the timer.
func (c *streamConn) SetReadDeadline(t time.Time) error {
	c.deadlineMu.Lock()
	c.readDeadline = t
	old := c.readDeadlineChanged
	c.readDeadlineChanged = make(chan struct{})
	c.deadlineMu.Unlock()
	close(old) // wake any blocked Read so it re-evaluates the deadline
	return nil
}

// SetWriteDeadline implements net.Conn. Setting a past time unblocks any
// blocked Write immediately; setting zero removes the deadline.
// No goroutines are spawned: the deadline is stored and a change-notification
// channel is closed so pending Write calls re-evaluate the timer.
func (c *streamConn) SetWriteDeadline(t time.Time) error {
	c.deadlineMu.Lock()
	c.writeDeadline = t
	old := c.writeDeadlineChanged
	c.writeDeadlineChanged = make(chan struct{})
	c.deadlineMu.Unlock()
	close(old) // wake any blocked Write so it re-evaluates the deadline
	return nil
}

func (c *streamConn) writePump() {
	for {
		select {
		case b := <-c.writeCh:
			if len(b) == 0 {
				continue
			}

			if c.send == nil {
				c.finishRemote(errStreamClosed)
				return
			}

			if err := c.send(b); err != nil {
				c.finishRemote(err)
				return
			}

		case <-c.localDone:
			return

		case <-c.remoteDone:
			return

		case <-c.ctx.Done():
			return
		}
	}
}

func (c *streamConn) enqueueRead(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}

	b := make([]byte, len(p))
	copy(b, p)

	// Check the closed flag without holding the lock during the blocking send.
	// If remoteDone is closed we know the channel is closed; use select on
	// remoteDone rather than readClosedMu to avoid holding the lock while
	// blocking — finishRemote needs the same lock to close readCh.
	c.readClosedMu.Lock()
	closed := c.readClosed
	c.readClosedMu.Unlock()
	if closed {
		return 0, net.ErrClosed
	}

	select {
	case c.readCh <- b:
		return len(p), nil

	case <-c.remoteDone:
		return 0, net.ErrClosed

	case <-c.localDone:
		return 0, net.ErrClosed

	case <-c.ctx.Done():
		return 0, net.ErrClosed
	}
}

func (c *streamConn) finishRemote(err error) {
	c.remoteCloseOnce.Do(func() {
		c.remoteErrMu.Lock()
		c.remoteErr = err
		c.remoteErrMu.Unlock()

		close(c.remoteDone)

		// Guard readCh close to prevent send-on-closed-channel in enqueueRead.
		c.readClosedMu.Lock()
		c.readClosed = true
		close(c.readCh)
		c.readClosedMu.Unlock()

		if c.closeFn != nil {
			_ = c.closeFn()
		}

		c.cancel()
	})
}

func (c *streamConn) remoteReadErr() error {
	c.remoteErrMu.Lock()
	err := c.remoteErr
	c.remoteErrMu.Unlock()

	if err == nil || errors.Is(err, context.Canceled) {
		return io.EOF
	}

	return err
}

type remoteStdoutWriter struct {
	conn *streamConn
}

func (w remoteStdoutWriter) Write(p []byte) (int, error) {
	return w.conn.enqueueRead(p)
}

// limitedBuffer captures bounded stderr output from remote commands.
type limitedBuffer struct {
	mu    sync.Mutex
	limit int
	buf   []byte
}

func newLimitedBuffer(limit int) *limitedBuffer {
	return &limitedBuffer{
		limit: limit,
	}
}

func (b *limitedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.limit <= 0 {
		return len(p), nil
	}

	remaining := b.limit - len(b.buf)
	if remaining <= 0 {
		return len(p), nil
	}

	if len(p) > remaining {
		b.buf = append(b.buf, p[:remaining]...)
	} else {
		b.buf = append(b.buf, p...)
	}

	return len(p), nil
}

func (b *limitedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()

	return string(b.buf)
}
