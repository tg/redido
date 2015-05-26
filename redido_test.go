package redido

import (
	"errors"
	"fmt"
	"io"
	"net"
	"testing"
)

func cmdArgsToString(cmd string, args ...interface{}) string {
	return fmt.Sprintf("%s%v;", cmd, args)
}

type logConnFactory struct {
	done     int
	lastDone string
	created  int
	closed   int

	err  error
	conn *logConn
}

func (f *logConnFactory) New() (Conn, error) {
	if f.conn != nil {
		panic("already has a connection")
	}

	// Pop error
	if f.err != nil {
		err := f.err
		f.err = nil
		return nil, err
	}

	f.created++
	f.conn = &logConn{factory: f}
	return f.conn, nil
}

type logConn struct {
	factory *logConnFactory
}

func (c *logConn) Do(cmd string, args ...interface{}) (interface{}, error) {
	if c.factory.err == nil {
		c.factory.lastDone = cmdArgsToString(cmd, args)
		c.factory.done++
	}
	return c.factory.done, c.factory.err
}

func (c *logConn) Close() error {
	if c.factory.conn != c {
		panic("wrong connection in factory")
	}
	c.factory.closed++
	c.factory.conn = nil
	c.factory.err = nil
	return nil
}

func TestDoConnError(t *testing.T) {
	f := logConnFactory{}
	d := New(f.New)

	tasks := []struct {
		err  error
		cmd  string
		args []interface{}
	}{
		{io.EOF, "TEST1", []interface{}{1, 1}},
		{nil, "TEST2", []interface{}{2}},
		{&net.DNSError{}, "TEST3", []interface{}{"three", 3}},
		{io.EOF, "TEST4", []interface{}{4}},
		{nil, "TEST5", []interface{}{5, "fünf"}},
	}

	errors := 0
	for n, task := range tasks {
		if task.err != nil {
			f.err = task.err
			errors++
		}

		if r, err := d.Do(task.cmd, task.args...); err != nil {
			t.Error("Do returned error:", err)
		} else if r != n+1 {
			t.Error("Do returned", r, "expected", n+1)
		}

		if e := cmdArgsToString(task.cmd, task.args); f.lastDone != e {
			t.Errorf("last done is %q, expected %q", f.lastDone, e)
		}
	}

	d.Close()

	if f.created != f.closed {
		t.Error("created", f.created, "closed", f.closed)
	}
	if f.created != errors+1 {
		t.Error("created", f.created, "expected", errors+1)
	}
}

func TestDoCustomError(t *testing.T) {
	f := logConnFactory{}
	d := New(f.New)

	_, err := d.Do("TEST", 1)
	if err != nil {
		t.Error("do returned error:", err)
	}

	// Set custom error
	cerr := errors.New("custom error")
	f.err = cerr

	n, err := d.Do("TEST", 2)
	if err != cerr {
		t.Error("expected custom error, got", err)
	}
	if n != 1 {
		t.Error("Do returned", n, "expected 1")
	}
}
