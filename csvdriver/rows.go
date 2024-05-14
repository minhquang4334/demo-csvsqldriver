package csvdriver

import (
	"database/sql/driver"
	"encoding/csv"
	"os"
)

type rows struct {
	reader  *csv.Reader
	columns []string
	file    *os.File
}

// implement driver.Rows interface
var _ driver.Rows = &rows{}

// driver.Rows interface
func (r *rows) Columns() []string {
	return r.columns
}

func (r *rows) Close() error {
	_, err := r.file.Seek(0, 0)
	return err
}

func (r *rows) Next(dest []driver.Value) error {
	d, err := r.reader.Read()
	if err != nil {
		return err
	}
	for i := 0; i != len(r.columns); i++ {
		dest[i] = driver.Value(d[i])
	}

	return nil
}
