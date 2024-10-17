package main

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/apache/arrow/go/v12/arrow"
	"github.com/apache/arrow/go/v12/arrow/array"
	dbsql "github.com/databricks/databricks-sql-go"
	dbsqlrows "github.com/databricks/databricks-sql-go/rows"
	"github.com/joho/godotenv"
)

func main() {
	// Load environment variables from .env file (containing Databricks credentials).
	err := godotenv.Load()
	if err != nil {
		log.Fatal(err.Error())
	}

	// Port configuration for the Databricks SQL connector.
	port := 443
	if err != nil {
		log.Fatal(err.Error())
	}

	// Create a new Databricks SQL connector using the credentials from environment variables.
	connector, err := dbsql.NewConnector(
		dbsql.WithServerHostname(os.Getenv("DATABRICKS_HOST")),
		dbsql.WithPort(port),
		dbsql.WithHTTPPath(os.Getenv("DATABRICKS_HTTP_PATH")),
		dbsql.WithAccessToken(os.Getenv("DATABRICKS_ACCESS_TOKEN")),
		dbsql.WithMaxRows(100000), // Set a maximum number of rows to fetch.
	)

	// Handle any error while creating the connector.
	if err != nil {
		log.Fatal(err)
	}

	// Open the SQL connection using the connector.
	db := sql.OpenDB(connector)
	defer db.Close() // Ensure the connection is closed after operations are complete.

	// Call the function to retrieve and process the data.
	getData(db)
}

// getData retrieves data from the database, processes it in Arrow batches, and prints the result.
func getData(db *sql.DB) {
	// Start the timer
	start := time.Now()

	// Create a context with a 60-second timeout for the query execution.
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// Establish a connection to the database.
	conn, _ := db.Conn(ctx)
	defer conn.Close()

	// SQL query to fetch data from the "nyctaxi.trips" table.
	query := `SELECT * FROM samples.nyctaxi.trips`

	var rows driver.Rows
	var err error

	// Execute the query using the underlying database driver.
	err = conn.Raw(func(d interface{}) error {
		rows, err = d.(driver.QueryerContext).QueryContext(ctx, query, nil)
		return err
	})

	// Handle any error while executing the query.
	if err != nil {
		log.Fatalf("unable to run the query. err: %v", err)
	}
	defer rows.Close() // Ensure the rows are closed after processing.

	// Create a new context for fetching Arrow batches from the result.
	ctx2, cancel2 := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel2()

	// Retrieve Arrow batches from the query result.
	batches, err := rows.(dbsqlrows.Rows).GetArrowBatches(ctx2)
	if err != nil {
		log.Fatalf("unable to get arrow batches. err: %v", err)
	}

	var iBatch, nRows int

	// Loop through the Arrow batches and process each batch.
	for batches.HasNext() {
		b, err := batches.Next()
		if err != nil {
			log.Fatalf("Failure retrieving batch. err: %v", err)
		}

		// Log the number of records in each batch.
		log.Printf("batch %v: nRecords=%v\n", iBatch, b.NumRows())

		// Print the content of the batch.
		printBatch(b)
		iBatch += 1
		nRows += int(b.NumRows())
		b.Release() // Release the batch to free memory.
	}

	// Log the total number of rows processed.
	log.Printf("NRows: %v\n", nRows)

	// Calculate the elapsed time.
	elapsed := time.Since(start)
	log.Printf("Data processing took %s", elapsed)
}

// printBatch prints all the rows and columns of an Arrow Record (batch) in a table format.
func printBatch(record arrow.Record) {
	// Get the schema of the record to print column names.
	schema := record.Schema()

	// Get the list of fields (columns) from the schema.
	fields := schema.Fields()

	// Print the table headers (column names).
	for _, field := range fields {
		fmt.Printf("%s\t", field.Name) // Print column names separated by tabs.
	}
	fmt.Println() // Newline after the column headers.

	// Print a separator line for readability.
	for range fields {
		fmt.Print("--------\t") // Separator line with dashes.
	}
	fmt.Println()

	// Loop through each row in the batch.
	for rowIndex := 0; rowIndex < int(record.NumRows()); rowIndex++ {
		// Loop through each column in the row and print the value.
		for _, col := range record.Columns() {
			// Print the value of the column at the current row.
			printValue(col, rowIndex)
			fmt.Print("\t") // Separate columns with tabs.
		}
		fmt.Println() // Newline after each row.
	}
	fmt.Println() // Extra newline for readability between batches.
}

// printValue prints the value of a column for a specific row.
func printValue(col arrow.Array, index int) {
	// Use type assertion to determine the column's data type and print the value.
	switch col := col.(type) {
	case *array.Int32:
		if col.IsNull(index) {
			fmt.Print("NULL")
		} else {
			fmt.Print(col.Value(index))
		}
	case *array.Int64:
		if col.IsNull(index) {
			fmt.Print("NULL")
		} else {
			fmt.Print(col.Value(index))
		}
	case *array.Float64:
		if col.IsNull(index) {
			fmt.Print("NULL")
		} else {
			fmt.Printf("%.2f", col.Value(index))
		}
	case *array.String:
		if col.IsNull(index) {
			fmt.Print("NULL")
		} else {
			fmt.Printf("%s", col.Value(index))
		}
	case *array.Timestamp:
		if col.IsNull(index) {
			fmt.Print("NULL")
		} else {
			// Convert the timestamp to time.Time for better readability
			ts := col.Value(index).ToTime(arrow.Microsecond)
			fmt.Print(ts.Format(time.RFC3339)) // Format the timestamp as needed
		}
	default:
		// Print a message for unsupported column types.
		fmt.Printf("Unsupported type: %T", col)
	}
}
