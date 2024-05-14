package csvdriver

import (
	"database/sql/driver"
	"os"
)

type Driver struct {
}

// implement driver.Driver interface
var _ driver.Driver = &Driver{}

// implements sql.Driver interface
// Open - open file for reading, return the connection
func (d *Driver) Open(name string) (driver.Conn, error) {
	file, err := os.Open(name)
	if err != nil {
		return nil, err
	}

	return &Conn{name: name, file: file}, nil
}
