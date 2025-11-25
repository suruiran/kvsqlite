package kvsqlite

import (
	"context"
	"database/sql"
	"fmt"
)

type _StringHandle struct {
	tx  *Tx
	key string
}

func (tx *Tx) String(key string) *_StringHandle {
	return &_StringHandle{tx: tx, key: key}
}

func (handle *_StringHandle) Get(ctx context.Context) (Value, error) {
	if err := handle.tx.ensurekind(ctx, KeyKindString, handle.key); err != nil {
		return Value{}, err
	}
	var val Value
	err := handle.tx.queryone(ctx, `select value from kv_string where key = ?`, []any{handle.key}, []any{&val})
	if err == sql.ErrNoRows {
		err = ErrNil
	}
	return val, err
}

func (handle *_StringHandle) _set(ctx context.Context, val Value) error {
	_, err := handle.tx.exec(ctx, `insert or replace into kv_string (key, value) values (?, ?)`, handle.key, val)
	return err
}

func (handle *_StringHandle) Set(ctx context.Context, val Value) error {
	if err := handle.tx.ensurekind(ctx, KeyKindString, handle.key); err != nil {
		if err != ErrNil {
			return err
		}
		err = handle.tx.addkey(ctx, handle.key, KeyKindString)
		if err != nil {
			return fmt.Errorf("kvsqlite: add key failed, %s", err)
		}
	}
	return handle._set(ctx, val)
}

func (handle *_StringHandle) delone(ctx context.Context) (int64, error) {
	return handle.tx.exec(ctx, `delete from kv_string where key = ?`, handle.key)
}

func (handle *_StringHandle) Incr(ctx context.Context, amount int64) (int64, error) {
	pv, err := handle.Get(ctx)
	if err != nil {
		if err == ErrNil {
			return amount, handle.Set(ctx, Int(amount))
		}
		return 0, err
	}
	iv, err := pv.Int64()
	if err != nil {
		return 0, err
	}
	iv += int64(amount)
	return iv, handle._set(ctx, Int(iv))
}
