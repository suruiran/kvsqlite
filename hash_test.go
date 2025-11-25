package kvsqlite

import (
	"context"
	"fmt"
	"testing"
)

func TestHashHandle(t *testing.T) {
	defer db.Close()

	db.TxScope(context.Background(), func(ctx context.Context, tx *Tx) error {
		tx.Hash("ccc").Set(ctx, "a", String("121"))
		fmt.Println(tx.Hash("ccc").Get(ctx, "a"))

		fmt.Println(tx.Hash("ccc").Items(ctx))

		tx.Hash("xxxx").SetAll(ctx, map[string]Value{"xxx": String("dd"), "yyy": String("ll")})
		fmt.Println(tx.Hash("xxxx").Items(ctx))
		fmt.Println(tx.Hash("xxxx").Size(ctx))
		fmt.Println(tx.Hash("xxxx").Incr(ctx, "num", -1))
		return nil
	})
}
