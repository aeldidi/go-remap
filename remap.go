// remap implements an out-of-process key-value store. Each string key maps
// to a JSON value.
package remap

import (
	"encoding/json"
	"errors"
	"fmt"
	"sync"
)

var ErrDup = errors.New("the key name given already exists in the store")

func New(driver string, dataSourceName string) (*Map, error) {
	driversMu.RLock()
	defer driversMu.RUnlock()

	conn, err := drivers[driver].Open(dataSourceName)
	if err != nil {
		return nil, fmt.Errorf("couldn't initialize driver: %w", err)
	}

	return &Map{
		c: conn,
	}, nil
}

var ErrNotSupported = errors.New("not supported")

func From(other Conn) (*Map, error) {
	conn, err := other.Clone()
	if err != nil {
		return nil, fmt.Errorf("couldn't initialize driver: %w", err)
	}

	return &Map{
		c: conn,
	}, nil
}

type Field struct {
	Name  string
	Value string
}

type Index struct {
	Index uint32
	Value string
}

type Driver interface {
	Open(dataSourceName string) (Conn, error)
}

type Conn interface {
	Clone() (Conn, error)

	SetIfNotExists(key string, value string) (bool, error)
	SetString(key string, value string) error
	// Returns the JSON representation of the type.
	GetString(key string) (string, error)
	DelString(key string) error
}

var drivers = make(map[string]Driver)
var driversMu sync.RWMutex

// Register makes a remap driver available by the provided name.
//
// If Register is called twice with the same name or if driver is nil, it
// panics.
func Register(name string, driver Driver) {
	driversMu.Lock()
	defer driversMu.Unlock()
	if driver == nil {
		panic("remap: Register driver is nil")
	}

	if _, dup := drivers[name]; dup {
		panic("remap: Register called twice for driver " + name)
	}
	drivers[name] = driver
}

type Map struct {
	c Conn
}

var ErrInvalidType = errors.New("value type cannot be set atomically")

// SetIfNotExists sets the given key to a value and returns `true` if the key
// could be set, or `false` if the key already exists.
//
// Also returns an error if the value could not be marshalled into JSON, or if
// the backing store returned an error.
func (m *Map) SetIfNotExists(key string, value any) (bool, error) {
	bytes, err := json.Marshal(value)
	if err != nil {
		return false, fmt.Errorf("error marshalling value to JSON: %w", err)
	}

	return m.c.SetIfNotExists(key, string(bytes))
}

func (m *Map) Set(key string, value any) error {
	bytes, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("error marshalling value to JSON: %w", err)
	}

	if err = m.c.SetString(key, string(bytes)); err != nil {
		return fmt.Errorf("error setting value: %w", err)
	}

	return nil
}

func (m *Map) Del(key string) error {
	if err := m.c.DelString(key); err != nil {
		return fmt.Errorf("error deleting key: %w", err)
	}

	return nil
}

var ErrNotFound = errors.New("the requested key was not present")

// Returns [ErrNotFound] if the key is not set.
func (m *Map) Get(key string, value any) error {
	s, err := m.c.GetString(key)
	if err != nil {
		return fmt.Errorf("error getting value: %w", err)
	}

	if err = json.Unmarshal([]byte(s), value); err != nil {
		return fmt.Errorf("error unmarshalling response: %w", err)
	}

	return nil
}
