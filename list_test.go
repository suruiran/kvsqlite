package kvsqlite

import (
	"context"
	"fmt"
	"testing"

	"github.com/suruiran/cube"
)

func TestListHandle(t *testing.T) {
	defer db.Close() //nolint:errcheck

	err := db.TxScope(t.Context(), func(ctx context.Context, tx *Tx) error {
		handle := tx.List("lllv")

		// for i := range 1000 {
		// 	if err := handle.Push(ctx, String(fmt.Sprintf("%d", i))); err != nil {
		// 		return err
		// 	}
		// }

		fmt.Println(cube.Must(handle.Size(ctx)))
		fmt.Println(cube.Must(handle.Nth(ctx, 0)).String())

		var cursor *int64
		for {
			items, idx, err := handle.Page(ctx, cursor, -10)
			if err != nil {
				return err
			}
			if len(items) < 10 {
				break
			}
			fmt.Println(items)
			cursor = new(idx)
		}
		return nil
	})
	fmt.Println(err)
}
