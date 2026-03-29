package kvsqlite

import (
	"context"
	"database/sql"
	"fmt"
)

type _ListHandle struct {
	tx  *Tx
	key string
}

func (tx *Tx) List(key string) *_ListHandle {
	return &_ListHandle{tx: tx, key: key}
}

func (handle *_ListHandle) Size(ctx context.Context) (int64, error) {
	if err := handle.tx.ensurekind(ctx, KeyKindList, handle.key); err != nil {
		return 0, err
	}
	var val int64
	err := handle.tx.queryone(ctx, `select count(*) from kv_list where key = ?`, []any{handle.key}, []any{&val})
	if err == sql.ErrNoRows {
		return 0, nil
	}
	return val, nil
}

func (handle *_ListHandle) min_or_max(ctx context.Context, fnc string) (sql.Null[int64], error) {
	if err := handle.tx.ensurekind(ctx, KeyKindList, handle.key); err != nil {
		return sql.Null[int64]{}, err
	}
	rawsql := fmt.Sprintf(`select %s(idx) from kv_list where key = ?`, fnc)
	var val sql.Null[int64]
	err := handle.tx.queryone(ctx, rawsql, []any{handle.key}, []any{&val})
	if err != nil {
		return val, err
	}
	return val, nil
}

func (handle *_ListHandle) MaxIdx(ctx context.Context) (sql.Null[int64], error) {
	return handle.min_or_max(ctx, "max")
}

func (handle *_ListHandle) MinIdx(ctx context.Context) (sql.Null[int64], error) {
	return handle.min_or_max(ctx, "min")
}

// slow query
func (handle *_ListHandle) Nth(ctx context.Context, idx int) (Value, error) {
	if err := handle.tx.ensurekind(ctx, KeyKindList, handle.key); err != nil {
		return Value{}, err
	}
	ord := "ASC"
	offset := idx
	if idx < 0 {
		ord = "DESC"
		offset = -idx - 1
	}
	rawsql := fmt.Sprintf(`select value from kv_list where key = ? order by idx %s limit 1 offset ?`, ord)
	var val Value
	err := handle.tx.queryone(ctx, rawsql, []any{handle.key, offset}, []any{&val})
	if err == sql.ErrNoRows {
		err = ErrNil
	}
	return val, err
}

func (handle *_ListHandle) push(ctx context.Context, val Value, incr int64) error {
	fnc := "max"
	if incr < 0 {
		fnc = "min"
	}
	idx, err := handle.min_or_max(ctx, fnc)
	if err != nil {
		if err != ErrNil {
			return err
		}
		err = handle.tx.addkey(ctx, handle.key, KeyKindList)
		if err != nil {
			return err
		}
	}

	idx.V += incr
	_, err = handle.tx.exec(ctx, `insert into kv_list (key, idx, value) values (?, ?, ?)`, handle.key, idx.V, val)
	if err != nil {
		return err
	}
	return nil
}

func (handle *_ListHandle) Push(ctx context.Context, val Value) error {
	return handle.push(ctx, val, 1)
}

func (handle *_ListHandle) LPush(ctx context.Context, val Value) error {
	return handle.push(ctx, val, -1)
}

// cursor is the last idx of the previous page, or nil if the first page is requested
// pagesize < 0 means desc
func (handle *_ListHandle) Page(ctx context.Context, cursor *int64, pagesize int64) ([]Value, int64, error) {
	if err := handle.tx.ensurekind(ctx, KeyKindList, handle.key); err != nil {
		return nil, 0, err
	}

	if pagesize == 0 {
		return nil, 0, nil
	}

	cmp := ">"
	ord := "ASC"
	if pagesize < 0 {
		cmp = "<"
		ord = "DESC"
		pagesize = -pagesize
	}

	rawsql := ""
	args := []any{}
	if cursor == nil {
		rawsql = fmt.Sprintf(`select idx, value from kv_list where key = ? order by idx %s limit ?`, ord)
		args = append(args, handle.key, pagesize)
	} else {
		rawsql = fmt.Sprintf(`select idx, value from kv_list where key = ? and idx %s ? order by idx %s limit ?`, cmp, ord)
		args = append(args, handle.key, *cursor, pagesize)
	}

	var vals = make([]Value, 0, pagesize)
	rows, err := handle.tx.querymany(ctx, rawsql, args...)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close() //nolint:errcheck

	var lidx int64
	for rows.Next() {
		var val Value
		err := rows.Scan(&lidx, &val)
		if err != nil {
			return nil, 0, err
		}
		vals = append(vals, val)
	}
	return vals, lidx, rows.Err()
}

func (handle *_ListHandle) Clear(ctx context.Context) error {
	_, err := handle.tx.exec(ctx, `delete from kv_list where key = ?`, handle.key)
	return err
}
