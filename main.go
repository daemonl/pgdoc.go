package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"text/template"

	sq "github.com/elgris/sqrl"
	_ "github.com/lib/pq"
	sqrlx "gopkg.daemonl.com/sqrlx"
)

type arrayFlags []string

func (i *arrayFlags) String() string {
	return "my string representation"
}

func (i *arrayFlags) Set(value string) error {
	*i = append(*i, value)
	return nil
}

type Config struct {
	Exclude     []string
	PostgresURL string
}

func main() {
	var exclude arrayFlags
	flag.Var(&exclude, "exclude", "Tables to exclude")
	flag.Parse()
	config := Config{
		Exclude:     []string(exclude),
		PostgresURL: flag.Arg(0),
	}

	if err := do(config); err != nil {
		log.Fatal(err.Error())
	}
}

func do(config Config) error {

	schema := "public"
	ctx := context.Background()
	conn, err := sql.Open("postgres", config.PostgresURL)
	if err != nil {
		return err
	}
	if err := conn.Ping(); err != nil {
		return err
	}

	db, err := sqrlx.New(conn, sq.Dollar)
	if err != nil {
		return err
	}

	tables, err := getTableNames(ctx, db, schema, config.Exclude)
	if err != nil {
		return err
	}

	for idx, table := range tables {
		cols, err := getColumns(ctx, db, schema, table.Name)
		if err != nil {
			return err
		}
		tables[idx].Columns = cols
	}

	enums, err := getEnums(ctx, db, schema)
	if err != nil {
		return err
	}

	fullSchema := Schema{
		Tables: tables,
		Enums:  enums,
	}

	//	dump(tables)
	mdDump(fullSchema)

	return nil

}

func getEnums(ctx context.Context, db *sqrlx.Wrapper, schema string) ([]Enum, error) {

	rows, err := db.QueryRaw(ctx, `
		SELECT t.typname,
			string_agg(e.enumlabel, '|' ORDER BY e.enumsortorder) AS enum_labels,
			COALESCE(obj_description(t.oid, 'pg_type'), '')
		FROM   pg_catalog.pg_type t
		JOIN   pg_catalog.pg_namespace n ON n.oid = t.typnamespace 
		JOIN   pg_catalog.pg_enum e ON t.oid = e.enumtypid
		WHERE n.nspname = $1
		GROUP BY t.oid, t.typname, n.nspname;`, schema)
	if err != nil {
		return nil, fmt.Errorf("Looking up enums %w", err)
	}
	defer rows.Close()
	enums := make([]Enum, 0)
	for rows.Next() {
		name := ""
		description := ""
		valsRaw := ""
		if err := rows.Scan(&name, &valsRaw, &description); err != nil {
			return nil, err
		}
		enums = append(enums, Enum{
			Name:        name,
			Description: description,
			Values:      strings.Split(valsRaw, "|"),
		})
	}
	return enums, nil

}

func getTableNames(ctx context.Context, db *sqrlx.Wrapper, schema string, exclude []string) ([]Table, error) {
	rows, err := db.QueryRaw(ctx, `SELECT relname,
	COALESCE(obj_description(CONCAT('public.', relname)::regclass), '')
	FROM pg_catalog.pg_statio_user_tables WHERE schemaname = $1`, schema)
	if err != nil {
		return nil, err
	}
	tables := make([]Table, 0)
rows:
	for rows.Next() {
		table := Table{}
		if err := rows.Scan(&table.Name, &table.Description); err != nil {
			return nil, err
		}
		for _, exc := range exclude {
			if exc == table.Name {
				continue rows
			}
		}
		tables = append(tables, table)
	}
	return tables, nil
}

type Schema struct {
	Tables []Table
	Enums  []Enum
}

type Table struct {
	Name        string             `json:"name"`
	Description string             `json:"description"`
	Columns     []ColumnDefinition `json:"columns"`
}

type ColumnDefinition struct {
	Name        string `sql:"column_name" json:"name"`
	DataType    string `sql:"data_type" json:"type"`
	CustomType  bool   `sql:"custom_type" json:"custom"`
	Description string `sql:"description" json:"description"`
	IsNullable  bool   `sql:"is_nullable" json:"nullable"`
}

type Enum struct {
	Name        string
	Description string
	Values      []string
}

func getColumns(ctx context.Context, db *sqrlx.Wrapper, schema string, tableName string) ([]ColumnDefinition, error) {

	builder := sq.Select(
		"c.column_name",
		"CASE WHEN c.is_nullable = 'NO' THEN false ELSE true END AS is_nullable",
		"CASE WHEN data_type = 'USER-DEFINED' THEN true ELSE false END AS custom_type",
		"COALESCE(pgd.description, '') AS description",
	).From("pg_catalog.pg_statio_all_tables AS st").
		Join("pg_catalog.pg_description pgd on (pgd.objoid=st.relid)").
		RightJoin("information_schema.columns c on (pgd.objsubid=c.ordinal_position and c.table_schema=st.schemaname and c.table_name=st.relname)").
		Where("table_schema = ?", schema).
		Where("table_name = ?", tableName).
		OrderBy("ordinal_position ASC")

	if stmt, args, err := sq.Case("data_type").
		When("'USER-DEFINED'", "udt_name").
		When("'numeric'", "CONCAT('Number(', numeric_precision, ',', numeric_scale,')')").
		When("'character'", "CONCAT('Char(', character_maximum_length, ')')").
		When("'timestamp with time zone'", "'timestamp'").
		Else("data_type").ToSql(); err != nil {
		return nil, err
	} else {
		builder = builder.Column(stmt+" AS data_type", args...)
	}

	rows, err := db.Query(ctx, builder)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	cols := make([]ColumnDefinition, 0)
	for rows.Next() {
		col := ColumnDefinition{}
		if err := sqrlx.ScanStruct(rows, &col); err != nil {
			return nil, err
		}
		cols = append(cols, col)
	}
	return cols, nil
}

func dump(obj interface{}) {
	b, _ := json.MarshalIndent(obj, "", "  ")
	fmt.Printf("==%T==\n%s\n\n", obj, string(b))
}

func mdDump(schema Schema) {

	tpl, err := template.New("markdown.md").Funcs(template.FuncMap{
		"mdescape": func(val string) string {
			val = strings.ReplaceAll(val, "\n\n", "<br>")
			val = strings.ReplaceAll(val, "\n", " ")
			return val
		},
		"anchor": func(val string) string {
			return strings.ToLower(strings.ReplaceAll(val, "_", "-"))
		},
		"snakeToTitle": func(val string) string {
			words := strings.Split(val, "_")
			for idx, word := range words {
				words[idx] = strings.ToTitle(word[0:1]) + word[1:]
			}
			return strings.Join(words, " ")
		},
	}).ParseFiles("./templates/markdown.md")

	if err != nil {
		panic(err.Error())
	}

	if err := tpl.Execute(os.Stdout, execData{
		Data: schema,
	}); err != nil {
		panic(err.Error())
	}

}

type execData struct {
	Data interface{}
}
