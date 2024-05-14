package csvdriver

import (
	"database/sql"
	"fmt"
)

// Register the driver with database/sql
func init() {
	// register csv driver with database/sql
	// database/sql will hold a list of registered drivers and will use the appropriate driver based on the driver we use when opening a database
	sql.Register("csvdriver", &Driver{})
	fmt.Printf("Drivers=%#v\n", sql.Drivers())
}
