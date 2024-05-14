package csvdriver

import (
	"context"
	"database/sql/driver"
	"encoding/csv"
	"fmt"
	"os"
)

type Conn struct {
	name string
	file *os.File
}

// implement driver.Conn interface
var _ driver.Conn = &Conn{}

// implement driver.Tx interface
var _ driver.Tx = &Conn{}

// implement driver.QueryerContext interface
var _ driver.QueryerContext = &Conn{}

// implements driver.Conn interface
func (c *Conn) Prepare(query string) (driver.Stmt, error) {
	return nil, fmt.Errorf("Prepare method not implemented")
}

func (c *Conn) Close() error {
	return c.file.Close()
}

func (c *Conn) Begin() (driver.Tx, error) {
	return nil, fmt.Errorf("Begin method not implemented")
}

// implements driver.Tx interface
func (c *Conn) Commit() error {
	return fmt.Errorf("Commit method not implemented")
}

// implements driver.Tx interface
func (c *Conn) Rollback() error {
	return fmt.Errorf("Rollback method not implemented")
}

// Queryer interface
func (c *Conn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	if query != "SELECT * FROM csv" {
		return nil, fmt.Errorf("Only `SELECT * FROM csv` string is implemented!")
	}

	r := csv.NewReader(c.file)
	columns, err := r.Read()
	if err != nil {
		return nil, err
	}
	r.FieldsPerRecord = len(columns)

	res := &rows{r, columns, c.file}

	return res, nil
}
