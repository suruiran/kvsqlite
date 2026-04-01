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
	db, err := sql.Open("sqlite3", fmt.Sprintf(`%s?_journal_mode=WAL&busy_timeout=3000`, fp))
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

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
	if _, err := db.raw.ExecContext(
		ctx,
		`create table if not exists kv_hash (
			key text not null,
			field text not null,
			value blob not null,
			primary key (key, field)
		)`,
	); err != nil {
		return err
	}
	if _, err := db.raw.ExecContext(
		ctx,
		`create table if not exists kv_list (
			key text not null,
			idx int not null,
			value blob not null,
			primary key (key, idx)
		)`,
	); err != nil {
		return err
	}
	return nil
}

func (db *DB) Close() error {
	return db.raw.Close()
}

type _CtxKeyType int

const (
	_CtxKeyTx _CtxKeyType = iota
)

func (db *DB) TxScope(ctx context.Context, fnc func(ctx context.Context, tx *Tx) error) (err error) {
	scopetx := ctx.Value(_CtxKeyTx)
	if scopetx != nil {
		return fnc(ctx, scopetx.(*Tx))
	}

	var sqltx *sql.Tx
	sqltx, err = db.raw.BeginTx(ctx, nil)
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

	tx := &Tx{Raw: sqltx, db: db}
	ctx = context.WithValue(ctx, _CtxKeyTx, tx)
	err = fnc(ctx, tx)
	return err
}

func (db *DB) TxScopeWithCtx(ctx context.Context, fnc func(ctx context.Context, tx *TxWithCtx) error) error {
	return db.TxScope(ctx, func(ctx context.Context, tx *Tx) error {
		tmp := TxWithCtx{Tx: tx, ctx: ctx}
		return fnc(ctx, &tmp)
	})
}
