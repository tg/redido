package redido

import (
	"io"
	"net"
	"sync"
	"time"

	"github.com/garyburd/redigo/redis"
)

// Conn iterface exposes Do and Close. It's a subset of redigo.Conn interface.
type Conn interface {
	// Do sends a command to the server and returns the received reply.
	Do(commandName string, args ...interface{}) (reply interface{}, err error)

	// Close closes the connection.
	Close() error
}

// ConnFactory is a function returning new connections
type ConnFactory func() (Conn, error)

// Doer does the Do, reconnecting when necessary
type Doer struct {
	newConn ConnFactory
	c       Conn
	lock    sync.Mutex
}

// New returns new Doer which will use passed connection factory to estabilish
// new connections.
func New(cf ConnFactory) *Doer {
	return &Doer{newConn: cf}
}

// NewDialTimeout will dial using redis.DialTimeout with passed parameters.
// No connections is being estabilished by calling this function.
func NewDialTimeout(network, address string, connectTimeout, readTimeout, writeTimeout time.Duration) *Doer {
	return New(func() (Conn, error) {
		return redis.DialTimeout(network, address, connectTimeout, readTimeout, writeTimeout)
	})
}

// Do does the command with arguments. Retries on network error.
func (d *Doer) Do(cmd string, args ...interface{}) (reply interface{}, err error) {
	for n := 0; n < 5; n++ {
		reply, err = d.do(cmd, args...)
		if d.c != nil {
			return
		}
		time.Sleep(time.Duration(n) * time.Second)
	}
	return
}

// do does the command with arguments without any retries.
// Connections will be estabilished if doesn't exist.
func (d *Doer) do(cmd string, args ...interface{}) (resp interface{}, err error) {
	d.lock.Lock()
	defer d.lock.Unlock()

	if d.c == nil {
		d.c, err = d.newConn()
		if err != nil {
			return
		}
	}
	r, err := d.c.Do(cmd, args...)
	if _, ok := err.(net.Error); ok || err == io.EOF {
		d.Close()
	}
	return r, err
}

// Close closes the underlying connection.
func (d *Doer) Close() {
	if d.c != nil {
		d.c.Close()
		d.c = nil
	}
}
