package kvsqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	_ "github.com/mattn/go-sqlite3"
)

type DB struct {
	raw *sql.DB
}

func OpenDB(ctx context.Context, fp string) (*DB, error) {
	db, err := sql.Open("sqlite3", fp)
	if err != nil {
		return nil, err
	}
	db.ExecContext(ctx, `PRAGMA journal_mode=WAL;`)
	db.ExecContext(ctx, `PRAGMA busy_timeout=3000;`)

	obj := &DB{raw: db}
	err = obj._Init(ctx)
	if err != nil {
		return nil, err
	}
	return obj, nil
}

func (db *DB) SqlDB() *sql.DB {
	return db.raw
}

func (db *DB) _Init(ctx context.Context) error {
	if _, err := db.raw.ExecContext(
		ctx,
		`create table if not exists kv_index (
			key text primary key not null,
			kind int
		)`,
	); err != nil {
		return err
	}
	if _, err := db.raw.ExecContext(
		ctx,
		`create table if not exists kv_string (
			key text primary key not null,
			value blob not null
		)`,
	); err != nil {
		return err
	}
	_, err := db.raw.ExecContext(
		ctx,
		`create table if not exists kv_hash (
			key text not null,
			field text not null,
			value blob not null,
			primary key (key, field)
		)`,
	)
	return err
}

func (db *DB) Close() error {
	return db.raw.Close()
}

func (db *DB) TxScope(ctx context.Context, fnc func(ctx context.Context, tx *Tx) error) (err error) {
	var sqltx *sql.Tx
	sqltx, err = db.raw.Begin()
	if err != nil {
		return err
	}
	defer func() {
		errored := err != nil
		if ra := recover(); ra != nil {
			errored = true
			err = fmt.Errorf("kvsqlite: tx scope recoverd error, %v", ra)
		}
		if errored {
			rollback_err := sqltx.Rollback()
			if rollback_err != nil {
				err = errors.Join(rollback_err, err)
			}
			return
		}
		commit_err := sqltx.Commit()
		if commit_err != nil {
			err = commit_err
		}
	}()
	err = fnc(ctx, &Tx{raw: sqltx, db: db})
	return err
}

func (db *DB) TxScopeWithCtx(ctx context.Context, fnc func(tx *TxWithCtx) error) error {
	return db.TxScope(ctx, func(ctx context.Context, tx *Tx) error {
		tmp := TxWithCtx{Tx: tx, ctx: ctx}
		return fnc(&tmp)
	})
}
