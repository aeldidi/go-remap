package remap_sqlite

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"

	"github.com/mattn/go-sqlite3"
	"go.eldidi.org/remap"
)

func init() {
	remap.Register("sqlite", &SQLiteDriver{})
}

// Creates a remap connection from an existing SQLite database connection.
// The `*sql.DB` provided MUST be one initialized from
// https://github.com/mattn/go-sqlite3.
func DB(db *sql.DB) remap.Conn {
	return &SQLiteConn{
		db: db,
	}
}

type SQLiteDriver struct {
}

func (SQLiteDriver) Open(dataSourceName string) (remap.Conn, error) {
	db, err := sql.Open("sqlite3", dataSourceName)
	if err != nil {
		return nil, fmt.Errorf("error opening database: %w", err)
	}

	return &SQLiteConn{
		db: db,
	}, nil
}

type SQLiteConn struct {
	db *sql.DB
}

const (
	typeString = 1
	typeObject = 2
	typeArray  = 3
)

func (c *SQLiteConn) Clone() (remap.Conn, error) {
	return &SQLiteConn{
		db: c.db,
	}, nil
}

func (d *SQLiteConn) SetIfNotExists(key string, value string) (bool, error) {
	var success bool
	err := d.doTransaction(func(conn *sqlite3.SQLiteConn) error {
		result, err := conn.Exec(`
		INSERT INTO remap_keys (maptype, mapkey) VALUES (?, ?);
		`, []driver.Value{int64(typeString), key})
		if errors.Is(err, sqlite3.ErrConstraintUnique) {
			success = false
			return remap.ErrDup
		} else if err != nil {
			return fmt.Errorf("error inserting values: %w", err)
		}

		id, err := result.LastInsertId()
		if err != nil {
			return fmt.Errorf("error retrieving last insert id: %w", err)
		}

		_, err = conn.Exec(`
		INSERT INTO remap_values (id, mapvalue) VALUES (?, ?)
			ON CONFLICT DO UPDATE SET mapvalue=?;
		`, []driver.Value{id, value, value})
		if err != nil {
			return fmt.Errorf("error inserting values: %w", err)
		}

		success = true
		return nil
	})

	return success, err
}

func (d *SQLiteConn) GetString(key string) (string, error) {
	var result sql.NullString
	err := d.db.QueryRow(`
	SELECT v.mapvalue
		FROM remap_keys AS k, remap_values AS v
		WHERE k.mapkey=? AND k.maptype=? AND v.id=k.id;
	`, key, typeString).Scan(&result)
	if errors.Is(err, sql.ErrNoRows) {
		return "", remap.ErrNotFound
	} else if !result.Valid {
		return "null", nil
	} else if err != nil {
		return "", fmt.Errorf("error in SQLite driver: %w", err)
	}

	return result.String, nil
}

func (c *SQLiteConn) DelString(key string) error {
	_, err := c.db.Exec(`
	DELETE FROM remap_keys WHERE mapkey=?;
	`, key)
	return err
}

func (d *SQLiteConn) SetString(key string, value string) error {
	tx, err := d.db.Begin()
	if err != nil {
		return fmt.Errorf("error beginning transaction: %w", err)
	}
	defer tx.Rollback()

	var id int64
	err = tx.QueryRow(`
	SELECT id FROM remap_keys WHERE mapkey=?;
	`, key).Scan(&id)
	if err == nil {
		_, err = tx.Exec(`
		UPDATE remap_values SET mapvalue=? WHERE id=?;
		`, value, id)
		if err != nil {
			return fmt.Errorf("error updating value: %w", err)
		}

		if err = tx.Commit(); err != nil {
			return fmt.Errorf("error committing to DB: %w", err)
		}

		return nil
	} else if !errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("error querying db: %w", err)
	}

	res, err := tx.Exec(`
	INSERT INTO remap_keys (mapkey, maptype) VALUES (?, ?);
	`, key, typeString)
	if err != nil {
		return fmt.Errorf("error inserting key: %w", err)
	}

	id, err = res.LastInsertId()
	if err != nil {
		return fmt.Errorf("error getting last insert ID: %w", err)
	}

	_, err = tx.Exec(`
	INSERT INTO remap_values (id, mapvalue) VALUES (?, ?);
	`, id, value)
	if err != nil {
		return fmt.Errorf("error inserting values: %w", err)
	}

	if err = tx.Commit(); err != nil {
		return fmt.Errorf("error committing to DB: %w", err)
	}

	return nil
}

func (d *SQLiteConn) doTransaction(
	fn func(conn *sqlite3.SQLiteConn) error,
) error {
	conn, err := d.db.Conn(context.TODO())
	if err != nil {
		return fmt.Errorf("error getting db connection: %w", err)
	}

	err = conn.Raw(func(driverConn any) error {
		conn, ok := driverConn.(*sqlite3.SQLiteConn)
		if !ok {
			panic(`remap: "sqlite" driver was not an SQLite driver`)
		}

		tx, err := conn.Begin()
		if err != nil {
			return fmt.Errorf("error beginning transaction: %w", err)
		}
		defer tx.Rollback()

		if err = fn(conn); err != nil {
			return err
		}

		return tx.Commit()
	})
	if errors.Is(err, remap.ErrDup) {
		return err
	} else if err != nil {
		return fmt.Errorf("error in SQLite driver: %w", err)
	}

	return nil
}
