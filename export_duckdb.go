package main

import (
	"database/sql"
	_ "github.com/marcboeker/go-duckdb"
	"log"
	"flag"
	"fmt"
	"path/filepath"
	"os"
)

func readTables(db *sql.DB) ([]string, error) {
	rows, err := db.Query("SHOW TABLES")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var tables []string
	for rows.Next() {
		var table string
		if err := rows.Scan(&table); err != nil {
			return nil, err
		}
		tables = append(tables, table)
	}

	return tables, nil
}

func exportTables(db *sql.DB, tables []string, outdir string) error {
	for _, tblName := range tables {
		outfile := filepath.Join(outdir, tblName + ".csv")
		fmt.Printf("Exporting table %s to %s\n", tblName, outfile)

		_, err := db.Exec("COPY " + tblName + " TO '" + outfile + "' (HEADER, DELIMITER ',')") // FIXME: may cause problems with certain filenames
		if err != nil {
			return err
		}
	}
	
	return nil
}

func main() {
	var inputFlag = flag.String("input", "", "Input db file")
	var outputFlag = flag.String("output", ".", "Output directory")
	var dryRunFlag = flag.Bool("dry-run", false, "Print tables but do not write output")
	flag.Parse()

	infile, err := filepath.Abs(*inputFlag)
	if err != nil {
		log.Fatal(err)
	}

	outdir, err := filepath.Abs(*outputFlag)
	if err != nil {
		log.Fatal(err)
	}

	outStats, err := os.Stat(outdir)
	if err != nil || !outStats.IsDir() {
		log.Fatal("Output directory does not exist or ist not a directory")
	}

	dryRun := *dryRunFlag

	db, err := sql.Open("duckdb", infile + "?access_mode=READ_ONLY")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	if db.Ping() != nil {
		log.Fatal("Cannot connect to database")
	}

	tables, err := readTables(db)
	if err != nil {
		log.Fatal(err)
	}
	
	fmt.Println("Found database tables:")
	for _, table := range tables {
		fmt.Printf("  - %s\n", table)
	}

	if dryRun {
		fmt.Println("Dry run - no output files were written")
		os.Exit(0)
	}

	err = exportTables(db, tables, outdir)
	if err != nil {
		log.Fatal(err)
	}
}
