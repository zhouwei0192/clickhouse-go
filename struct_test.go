package clickhouse

import (
	"context"
	"fmt"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"log"
	"testing"
	"time"
)

var ClickhouseClient driver.Conn

func InitClickhouse() {
	// 创建连接配置
	conn, err := Open(&Options{
		Addr: []string{"192.168.31.122:9000"}, // ClickHouse 地址
		Auth: Auth{
			Database: "data",
			Username: "zhouwei", // 用户名
			Password: "zhouwei", // 密码
		},
	})
	if err != nil {
		log.Fatal(err)
	}
	ClickhouseClient = conn
}

func TestName(t *testing.T) {
	InitClickhouse()
	tmp := make([]Transaction, 0)
	tmp = append(tmp, Transaction{
		PostBalances: []uint64{1, 2},
		PreBalances:  []uint64{1, 2},
		//PostTokenBalances: []TokenBalance{{
		//	AccountIndex: 1,
		//	Mint:         "2",
		//	Owner:        "3",
		//	ProgramID:    "4",
		//	UITokenAmount: UITokenAmount{
		//		Decimals: 1,
		//		UIAmount: 2,
		//	},
		//}},
		//PreTokenBalances: []TokenBalance{{
		//	AccountIndex: 1,
		//	Mint:         "2",
		//	Owner:        "3",
		//	ProgramID:    "4",
		//	UITokenAmount: UITokenAmount{
		//		Decimals: 1,
		//		UIAmount: 2,
		//	},
		//}},
		LoadedAddresses: LoadedAddress{
			Readonly: []string{"a", "b"},
			Writable: []string{"c", "b"},
		},
		Instructions: []Instructions{{
			Instruction: Instruction{
				Accounts:       []uint16{1, 4},
				Data:           "",
				ProgramIDIndex: 0,
			},
			InnerInstructions: []Instruction{{
				Accounts:       []uint16{54},
				Data:           "21",
				ProgramIDIndex: 0,
			}},
		}},
		AccountKeys: []string{"ds"},
		Index:       21,
		Signature:   "",
		BlockSlot:   0,
		BlockTime:   time.Time{},
	})
	err := NewTransactionDao(ClickhouseClient, context.Background()).Create(tmp)
	if err != nil {
		fmt.Println(err.Error())
	}
}

type TokenBalance struct {
	AccountIndex  uint16        `ch:"account_index"`
	Mint          string        `ch:"mint"`
	Owner         string        `ch:"owner"`
	ProgramID     string        `ch:"programId"`
	UITokenAmount UITokenAmount `ch:"ui_token_amount"`
}
type UITokenAmount struct {
	Decimals uint8   `ch:"decimals"`
	UIAmount float64 `ch:"uiAmount"`
}
type LoadedAddress struct {
	Readonly []string `ch:"readonly"`
	Writable []string `ch:"writable"`
}
type Instruction struct {
	Accounts       []uint16 `ch:"accounts"`
	Data           string   `ch:"data"`
	ProgramIDIndex uint16   `ch:"program_id_index"`
}
type Instructions struct {
	Instruction       Instruction   `ch:"instruction"`
	InnerInstructions []Instruction `ch:"inner_instructions"`
}
type Transaction struct {
	PostBalances      []uint64       `ch:"post_balances"`
	PreBalances       []uint64       `ch:"pre_balances"`
	PostTokenBalances []TokenBalance `ch:"post_token_balances"`
	PreTokenBalances  []TokenBalance `ch:"pre_token_balances"`
	LoadedAddresses   LoadedAddress  `ch:"loaded_addresses"`
	Instructions      []Instructions `ch:"instructions"`
	AccountKeys       []string       `ch:"account_keys"`
	Index             uint16         `ch:"index"`
	Signature         string         `ch:"signature"`
	BlockSlot         int64          `ch:"block_slot"`
	BlockTime         time.Time      `ch:"block_time"`
}

func NewTransactionDao(db driver.Conn, ctx context.Context) *TransactionDao {
	return &TransactionDao{db: db, ctx: ctx}
}

type TransactionDao struct {
	db  driver.Conn
	ctx context.Context
}

func (*TransactionDao) tableName() string {
	return "solana"
}

func (k *TransactionDao) Create(list []Transaction) error {
	if len(list) == 0 {
		return nil
	}
	batch, err := ClickhouseClient.PrepareBatch(k.ctx, "INSERT INTO "+k.tableName())
	if err != nil {
		return err
	}
	for i := 0; i < len(list); i++ {
		err = batch.AppendStruct(&list[i])
		if err != nil {
			return err
		}
	}
	return batch.Send()
}
