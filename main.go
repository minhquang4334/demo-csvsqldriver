package main

import (
	"database/sql"
	"fmt"
	"log"

	_ "github.com/minhquang4334/democsvdriver/csvdriver"
)

func main() {
	db, err := sql.Open("csvdriver", "./testdata/test.csv")
	if err != nil {
		log.Fatalf("Error %s when opening DB\n", err)
	}

	defer db.Close()
	rows, err := db.Query("SELECT * FROM csv")
	if err != nil {
		log.Fatalf("Error %s when querying DB\n", err)
	}

	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		log.Fatalf("Error %s when getting columns\n", err)
	}

	for rows.Next() {
		var f1, f2, f3 string
		err := rows.Scan(&f1, &f2, &f3)
		if err != nil {
			log.Fatalf("Error %s when scanning\n", err)
		}

		fmt.Printf("%s=%s, %s=%s, %s=%s\n", columns[0], f1, columns[1], f2, columns[2], f3)
	}
}
