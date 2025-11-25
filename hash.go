package kvsqlite

import (
	"context"
	"database/sql"
)

type _HashHandle struct {
	tx  *Tx
	key string
}

func (tx *Tx) Hash(key string) *_HashHandle {
	return &_HashHandle{tx: tx, key: key}
}

func (handle *_HashHandle) Get(ctx context.Context, filed string) (Value, error) {
	if err := handle.tx.ensurekind(ctx, KeyKindHash, handle.key); err != nil {
		return Value{}, err
	}
	var val Value
	err := handle.tx.queryone(ctx, `select value from kv_hash where key = ? and field = ?`, []any{handle.key, filed}, []any{&val})
	if err == sql.ErrNoRows {
		err = ErrNil
	}
	return val, err
}

func (handle *_HashHandle) Incr(ctx context.Context, filed string, amount int64) (int64, error) {
	prev, err := handle.Get(ctx, filed)
	if err != nil {
		if err != ErrNil {
			return 0, err
		}
		return amount, handle.Set(ctx, filed, Int(amount))
	}
	num, err := prev.Int64()
	if err != nil {
		return 0, err
	}
	num += amount
	return num, handle._set(ctx, filed, Int(num))
}

func (handle *_HashHandle) Exists(ctx context.Context, filed string) (bool, error) {
	_, err := handle.Get(ctx, filed)
	if err != nil {
		if err == ErrNil {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func (handle *_HashHandle) Size(ctx context.Context) (int, error) {
	if err := handle.tx.ensurekind(ctx, KeyKindHash, handle.key); err != nil {
		if err == ErrNil {
			return 0, nil
		}
		return 0, err
	}
	var c int
	err := handle.tx.queryone(ctx, `select count(field) from kv_hash where key = ?`, []any{handle.key}, []any{&c})
	if err == sql.ErrNoRows {
		return 0, nil
	}
	return c, err
}

func (handle *_HashHandle) Items(ctx context.Context) (map[string]Value, error) {
	if err := handle.tx.ensurekind(ctx, KeyKindHash, handle.key); err != nil {
		if err == ErrNil {
			return nil, nil
		}
		return nil, err
	}
	rows, err := handle.tx.querymany(ctx, `select field, value from kv_hash where key = ?`, handle.key)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}
	defer rows.Close()

	var vmap = map[string]Value{}

	for rows.Next() {
		var key string
		var val Value
		err = rows.Scan(&key, &val)
		if err != nil {
			return nil, err
		}
		vmap[key] = val
	}
	return vmap, nil
}

func (handle *_HashHandle) Set(ctx context.Context, field string, val Value) error {
	if err := handle.tx.ensurekind(ctx, KeyKindHash, handle.key); err != nil {
		if err != ErrNil {
			return err
		}
		err = handle.tx.addkey(ctx, handle.key, KeyKindHash)
		if err != nil {
			return err
		}
	}
	return handle._set(ctx, field, val)
}

func (handle *_HashHandle) _set(ctx context.Context, field string, val Value) error {
	_, err := handle.tx.exec(ctx, `insert or replace into kv_hash (key, field, value) values (?, ?, ?)`, handle.key, field, val)
	return err
}

func (handle *_HashHandle) SetAll(ctx context.Context, vmap map[string]Value) error {
	if len(vmap) < 1 {
		return nil
	}
	if err := handle.tx.ensurekind(ctx, KeyKindHash, handle.key); err != nil {
		if err != ErrNil {
			return err
		}
		err = handle.tx.addkey(ctx, handle.key, KeyKindHash)
		if err != nil {
			return err
		}
	}
	for field, val := range vmap {
		err := handle._set(ctx, field, val)
		if err != nil {
			return err
		}
	}
	return nil
}

func (handle *_HashHandle) Clear(ctx context.Context) (int64, error) {
	err := handle.tx.ensurekind(ctx, KeyKindHash, handle.key)
	if err != nil {
		if err == ErrNil {
			return 0, nil
		}
		return 0, err
	}
	return handle.tx.exec(ctx, `delete from kv_hash where key=?`, handle.key)
}

func (handle *_HashHandle) Del(ctx context.Context, fields ...string) (int64, error) {
	if len(fields) < 1 {
		return 0, nil
	}
	err := handle.tx.ensurekind(ctx, KeyKindHash, handle.key)
	if err != nil {
		if err == ErrNil {
			return 0, nil
		}
		return 0, err
	}
	var dc int64
	for _, field := range fields {
		rac, err := handle.tx.exec(ctx, `delete from kv_hash where key = ? and field = ?`, handle.key, field)
		if err != nil {
			return 0, err
		}
		dc += rac
	}
	return dc, nil
}

func (handle *_HashHandle) remove(ctx context.Context) (int64, error) {
	return handle.tx.exec(ctx, `delete from kv_hash where key = ?`, handle.key)
}

func (handle *_HashHandle) Keys(ctx context.Context) ([]string, error) {
	err := handle.tx.ensurekind(ctx, KeyKindHash, handle.key)
	if err != nil {
		if err == ErrNil {
			return nil, nil
		}
		return nil, err
	}
	rows, err := handle.tx.querymany(ctx, `select field from kv_hash where key = ?`, handle.key)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}
	defer rows.Close()

	var keys []string
	for rows.Next() {
		var tmp string
		err = rows.Scan(&tmp)
		if err != nil {
			return nil, err
		}
		keys = append(keys, tmp)
	}
	return keys, nil
}
