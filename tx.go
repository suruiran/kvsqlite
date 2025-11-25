package kvsqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
)

type Tx struct {
	db  *DB
	raw *sql.Tx
}

func (tx *Tx) queryone(ctx context.Context, query string, args []any, dest []any) error {
	row := tx.raw.QueryRowContext(ctx, query, args...)
	if row.Err() != nil {
		return row.Err()
	}
	return row.Scan(dest...)
}

func (tx *Tx) querymany(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	return tx.raw.QueryContext(ctx, query, args...)
}

func (tx *Tx) exec(ctx context.Context, query string, args ...any) (int64, error) {
	result, err := tx.raw.ExecContext(ctx, query, args...)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected()
}

func (tx *Tx) addkey(ctx context.Context, key string, kind KeyKind) error {
	_, err := tx.raw.ExecContext(ctx, `insert into kv_index (key, kind) values (?, ?)`, key, kind)
	return err
}

var ErrNil = errors.New("kvsqlite: nil")

func (tx *Tx) ensurekind(ctx context.Context, expected KeyKind, key string) error {
	kind, err := tx.Kind(ctx, key)
	if err != nil {
		return err
	}
	if kind != expected {
		return fmt.Errorf("kvsqlite: bad key kind, expected %s, but it is a %s", expected.String(), kind.String())
	}
	return nil
}

func (tx *Tx) Kind(ctx context.Context, key string) (KeyKind, error) {
	var kind KeyKind
	err := tx.queryone(ctx, `select kind from kv_index where key = ?`, []any{key}, []any{&kind})
	if err == sql.ErrNoRows {
		err = ErrNil
	}
	return kind, err
}

func (tx *Tx) Exists(ctx context.Context, key string) (bool, error) {
	_, err := tx.Kind(ctx, key)
	if err != nil {
		if err == ErrNil {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func (tx *Tx) delone(ctx context.Context, key string) (int64, error) {
	kind, err := tx.Kind(ctx, key)
	if err != nil {
		return 0, err
	}
	switch kind {
	case KeyKindString:
		{
			return tx.String(key).delone(ctx)
		}
	case KeyKindHash:
		{
			return tx.Hash(key).remove(ctx)
		}
	}
	return 0, fmt.Errorf("kvsqlite: del failed, %s", key)
}

func (tx *Tx) Del(ctx context.Context, keys ...string) (int, []error) {
	if len(keys) < 1 {
		return 0, nil
	}

	c := 0
	var errors []error
	for _, key := range keys {
		if _, err := tx.delone(ctx, key); err != nil {
			if err == sql.ErrNoRows {
				continue
			}
			errors = append(errors, err)
			continue
		}
		_, err := tx.exec(ctx, `delete from kv_index where key = ?`, key)
		if err != nil {
			errors = append(errors, err)
			continue
		}
		c++
	}
	return c, errors
}

//go:generate python ./gen.py
