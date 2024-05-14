package csvdriver

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/csv"
	"fmt"
	"os"
)

// Register the driver with database/sql
func init() {
	// register csv driver with database/sql
	// database/sql will hold a list of registered drivers and will use the appropriate driver based on the driver we use when opening a database
	sql.Register("csvdriver", &CSVDriver{})
	fmt.Printf("Drivers=%#v\n", sql.Drivers())
}

type CSVDriver struct {
}

type CSVDriverConn struct {
	name string
	file *os.File
}

type results struct {
	reader  *csv.Reader
	columns []string
	file    *os.File
}

// implement driver.Driver interface
var _ driver.Driver = &CSVDriver{}

// implement driver.Conn interface
var _ driver.Conn = &CSVDriverConn{}

// implement driver.QueryerContext interface
var _ driver.QueryerContext = &CSVDriverConn{}

// implement driver.Rows interface
var _ driver.Rows = &results{}

// implements sql.Driver interface
// Open - open file for reading, return the connection
func (d *CSVDriver) Open(name string) (driver.Conn, error) {
	file, err := os.Open(name)
	if err != nil {
		return nil, err
	}

	return &CSVDriverConn{name: name, file: file}, nil
}

func (c *CSVDriverConn) Begin() (driver.Tx, error) {
	return nil, fmt.Errorf("Begin method not implemented")
}

func (c *CSVDriverConn) Close() error {
	return c.file.Close()
}

// implements driver.Conn interface
func (c *CSVDriverConn) Prepare(query string) (driver.Stmt, error) {
	return nil, fmt.Errorf("Prepare method not implemented")
}

// implements driver.Tx interface
func (c *CSVDriverConn) Rollback() error {
	return fmt.Errorf("Rollback method not implemented")
}

// Queryer interface
func (c *CSVDriverConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	if query != "SELECT * FROM csv" {
		return nil, fmt.Errorf("Only `SELECT * FROM csv` string is implemented!")
	}

	r := csv.NewReader(c.file)
	columns, err := r.Read()
	if err != nil {
		return nil, err
	}
	r.FieldsPerRecord = len(columns)

	res := &results{r, columns, c.file}

	return res, nil
}

// driver.Rows interface
func (r *results) Columns() []string {
	return r.columns
}

func (r *results) Close() error {
	_, err := r.file.Seek(0, 0)
	return err
}

func (r *results) Next(dest []driver.Value) error {
	d, err := r.reader.Read()
	if err != nil {
		return err
	}
	for i := 0; i != len(r.columns); i++ {
		dest[i] = driver.Value(d[i])
	}

	return nil
}
