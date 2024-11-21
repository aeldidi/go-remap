package remap_test

import (
	"database/sql"
	"fmt"
	"testing"

	_ "embed"

	"go.eldidi.org/remap"
	remap_sqlite "go.eldidi.org/remap/sqlite"
)

func TestBasic(t *testing.T) {
	m := newMap("TestBasic", t)
	if err := m.Set("cool", "beans"); err != nil {
		t.Fatal(err)
	}

	var value string
	if err := m.Get("cool", &value); err != nil {
		t.Fatal(err)
	}

	if value != "beans" {
		t.Fatalf(`expected "value", got "%v"`, value)
	}

	if err := m.Set("cool", "guy"); err != nil {
		t.Fatal(err)
	}

	value = ""
	if err := m.Get("cool", &value); err != nil {
		t.Fatal(err)
	}

	if value != "guy" {
		t.Fatalf(`expected "guy", got "%v"`, value)
	}
}

func TestDel(t *testing.T) {
	m := newMap("TestDel", t)
	if err := m.Set("cool", "beans"); err != nil {
		t.Fatal(err)
	}

	var value string
	if err := m.Get("cool", &value); err != nil {
		t.Fatal(err)
	}

	if err := m.Del("cool"); err != nil {
		t.Fatal(err)
	}

	if err := m.Get("cool", &value); err == nil {
		t.Fatal("deleted value still remains")
	}
}

func TestSetIfNotExist(t *testing.T) {
	m := newMap("TestSetIfNotExist", t)
	ok, err := m.SetIfNotExists("cool", "beans")
	if err != nil {
		t.Fatal(err)
	}

	if !ok {
		t.Fatal("already exist")
	}
}

//go:embed schema.sqlite.sql
var schema string

func newMap(name string, t *testing.T) *remap.Map {
	db, err := sql.Open(
		"sqlite3",
		fmt.Sprintf("file:%v?mode=memory&cache=shared", name),
	)
	if err != nil {
		t.Fatal(err)
	}

	if _, err = db.Exec(schema); err != nil {
		t.Fatal(err)
	}

	m, err := remap.From(remap_sqlite.DB(db))
	if err != nil {
		t.Fatal(err)
	}

	return m
}
