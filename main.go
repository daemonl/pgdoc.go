package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"io"
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
	pgURL := flag.String("postgres", "", "Postgres URL")

	pumlOutFile := flag.String("puml", "", "PUML Output File")
	jsonOutFile := flag.String("json", "", "JSON Output File")
	mdOutFile := flag.String("md", "", "MD Output File")

	pumlNoColumns := flag.Bool("puml-skip-columns", false, "Skip columns in PUML output")
	pumlInclTypes := flag.Bool("puml-include-types", false, "Include data types in PUML")

	flag.Parse()
	config := Config{
		Exclude:     []string(exclude),
		PostgresURL: *pgURL,
	}

	fullSchema, err := getSchema(config)
	if err != nil {
		log.Fatal(err.Error())
	}

	if *pumlOutFile != "" {
		withWriter(*pumlOutFile, func(w io.Writer) error {
			pumlOptions := PUMLOptions{
				IncludeColumns:   !*pumlNoColumns,
				IncludeDataTypes: *pumlInclTypes,
			}
			pumlDump(fullSchema, w, pumlOptions)
			return nil
		})
	}

	if *jsonOutFile != "" {
		withWriter(*jsonOutFile, func(w io.Writer) error {
			bytes, err := json.MarshalIndent(fullSchema, "", "  ")
			if err != nil {
				return err
			}
			if _, err := w.Write(bytes); err != nil {
				return err
			}
			return nil
		})
	}

	if *mdOutFile != "" {
		withWriter(*mdOutFile, func(w io.Writer) error {
			return mdDump(fullSchema, w)
		})
	}
}

func withWriter(filename string, callback func(io.Writer) error) {
	if filename == "-" {
		if err := callback(os.Stdout); err != nil {
			log.Fatal(err.Error())
		}
		return
	}
	out, err := os.Create(filename)
	if err != nil {
		log.Fatal(err.Error())
	}
	defer out.Close()
	if err := callback(out); err != nil {
		log.Fatal(err.Error())
	}
}

func getSchema(config Config) (*Schema, error) {

	schema := "public"
	ctx := context.Background()
	conn, err := sql.Open("postgres", config.PostgresURL)
	if err != nil {
		return nil, err
	}
	if err := conn.Ping(); err != nil {
		return nil, err
	}

	db, err := sqrlx.New(conn, sq.Dollar)
	if err != nil {
		return nil, err
	}

	return getFullSchema(ctx, db, schema, config)

}

func getFullSchema(ctx context.Context, db *sqrlx.Wrapper, schema string, config Config) (*Schema, error) {
	tables, err := getTableNames(ctx, db, schema, config.Exclude)
	if err != nil {
		return nil, err
	}

	for idx, table := range tables {
		cols, err := getColumns(ctx, db, schema, table.Name)
		if err != nil {
			return nil, err
		}

		constraints, err := getConstraints(ctx, db, schema, table.Name)
		if err != nil {
			return nil, err
		}

		pkCols := map[string]ConstraintDefinition{}
		fkCols := []ForeignKeyDefinition{}

		for _, constraint := range constraints {
			switch constraint.ConstraintType {
			case "PRIMARY KEY":
				for _, column := range constraint.LocalColumns {
					if column.Table != table.Name {
						return nil, fmt.Errorf("Table %s had primary key %s in %s", table.Name, constraint.ConstraintName, column.Table)
					}
					pkCols[column.Column] = constraint
				}
			case "FOREIGN KEY":
				if len(constraint.LocalColumns) != 1 || len(constraint.ForeignColumns) != 1 {
					return nil, fmt.Errorf("foreign keys should have 1 local, 1 foreign column. See %s", constraint.ConstraintName)
				}
				localCol := constraint.LocalColumns[0]
				if localCol.Table != table.Name {
					return nil, fmt.Errorf("Table %s had foreign key %s in %s", table.Name, constraint.ConstraintName, localCol.Table)
				}
				foreignCol := constraint.ForeignColumns[0]
				fkCols = append(fkCols, ForeignKeyDefinition{
					Column:    localCol.Column,
					Name:      constraint.ConstraintName,
					RefTable:  foreignCol.Table,
					RefColumn: foreignCol.Column,
				})

			default:
				return nil, fmt.Errorf("Unknown Constraint: %s", constraint.ConstraintType)
			}
		}

		keyColumns := make([]ColumnDefinition, 0, len(pkCols))
		restColumns := make([]ColumnDefinition, 0, len(cols))

		for _, col := range cols {
			if _, ok := pkCols[col.Name]; ok {
				keyColumns = append(keyColumns, col)
			} else {
				restColumns = append(restColumns, col)
			}
		}

		tables[idx].KeyColumns = keyColumns
		tables[idx].Columns = restColumns
		tables[idx].ForeignKeys = fkCols
	}

	enums, err := getEnums(ctx, db, schema)
	if err != nil {
		return nil, err
	}

	return &Schema{

		Tables: tables,
		Enums:  enums,
	}, nil
}

type ForeignKeyDefinition struct {
	Column    string
	Name      string
	RefTable  string
	RefColumn string
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
	Name        string                 `json:"name"`
	Description string                 `json:"description"`
	KeyColumns  []ColumnDefinition     `json:"keyColumns"`
	Columns     []ColumnDefinition     `json:"columns"`
	ForeignKeys []ForeignKeyDefinition `json:"foreignKeys"`
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
		Where("c.table_schema = ?", schema).
		Where("c.table_name = ?", tableName).
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

	rows, err := db.Select(ctx, builder)
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

type ColumnIdentity struct {
	Table  string `json:"table"`
	Column string `json:"column"`
}

type ConstraintDefinition struct {
	LocalColumns   []ColumnIdentity `json:"local_columns"`
	ForeignColumns []ColumnIdentity `json:"foreign_columns"`
	ConstraintName string           `json:"constraint_name"`
	ConstraintType string           `json:"constraint_type"`
}

func getConstraints(ctx context.Context, db *sqrlx.Wrapper, schema string, tableName string) ([]ConstraintDefinition, error) {

	rows, err := db.QueryRaw(ctx, `SELECT row_to_json(root.*) FROM (
SELECT 
kcu_sub.columns AS local_columns,
ccu_sub.columns AS foreign_columns,
tc.constraint_name,
tc.constraint_type 
FROM 
information_schema.table_constraints tc
LEFT JOIN (
        SELECT
        cu.constraint_name,
        cu.constraint_schema,
        array_to_json(array_agg(JSON_BUILD_OBJECT(
                        'table', cu.table_name::text,
                        'column', cu.column_name::text
        ))) AS columns 
        FROM information_schema.constraint_column_usage cu
        GROUP BY cu.constraint_name, cu.constraint_schema
) AS ccu_sub ON
ccu_sub.constraint_name = tc.constraint_name 
AND ccu_sub.constraint_schema = tc.constraint_schema
AND tc.constraint_type = 'FOREIGN KEY'
LEFT JOIN (
        SELECT
        cu.constraint_name,
        cu.constraint_schema,
        cu.table_name,
        cu.table_schema,
        array_to_json(array_agg(JSON_BUILD_OBJECT(
                        'table', cu.table_name::text,
                        'column', cu.column_name::text
        ))) AS columns
        FROM information_schema.key_column_usage cu
        GROUP BY cu.constraint_name, cu.constraint_schema, cu.table_name, cu.table_schema
) AS kcu_sub ON kcu_sub.constraint_name = tc.constraint_name AND kcu_sub.constraint_schema = tc.constraint_schema
WHERE tc.constraint_type IN ('FOREIGN KEY','PRIMARY KEY')
AND kcu_sub.table_schema = $1 AND kcu_sub.table_name = $2) AS root;`,
		schema,
		tableName,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	cols := make([]ConstraintDefinition, 0)
	for rows.Next() {
		colBytes := []byte{}
		if err := rows.Scan(&colBytes); err != nil {
			return nil, err
		}
		col := ConstraintDefinition{}
		if err := json.Unmarshal(colBytes, &col); err != nil {
			return nil, err
		}
		cols = append(cols, col)
	}

	return cols, nil

}

type PUMLWriter struct {
	PUMLOptions
	data string
}

func (c *PUMLWriter) Println(str string) {
	c.data = c.data + str + "\n"
}

func (c *PUMLWriter) Printf(str string, p ...interface{}) {
	c.data = c.data + fmt.Sprintf(str, p...)
}

func (c *PUMLWriter) Column(column ColumnDefinition) {
	prefix := map[bool]string{true: "", false: "* "}[column.IsNullable]
	if c.IncludeDataTypes {
		c.Printf("  %s%s: %s\n", prefix, column.Name, column.DataType)
	} else {
		c.Printf("  %s%s\n", prefix, column.Name)
	}
}

func (c *PUMLWriter) Table(table Table) {
	c.Printf("entity %s {\n", table.Name)
	for _, column := range table.KeyColumns {
		c.Column(column)
	}
	c.Println("--")
	for _, column := range table.Columns {
		c.Column(column)
	}
	c.Println("}")
}

func (c *PUMLWriter) Schema(schema *Schema) {
	c.Println("@startuml")

	if c.IncludeColumns {
		for _, table := range schema.Tables {
			c.Table(table)
		}
	}

	for _, table := range schema.Tables {
		for _, fk := range table.ForeignKeys {
			c.Printf("%s }|--|| %s\n", table.Name, fk.RefTable)
		}
	}

	c.Println("@enduml")

}

type PUMLOptions struct {
	IncludeColumns   bool
	IncludeDataTypes bool
}

func pumlDump(schema *Schema, writer io.Writer, options PUMLOptions) {
	c := &PUMLWriter{
		PUMLOptions: options,
	}
	c.Schema(schema)

	if _, err := writer.Write([]byte(c.data)); err != nil {
		panic(err.Error())
	}

}

func mdDump(schema *Schema, w io.Writer) error {

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
				if len(word) >= 2 {
					words[idx] = strings.ToTitle(word[0:1]) + word[1:]
				}
			}
			return strings.Join(words, " ")
		},
	}).Parse(defaultTemplate)

	if err != nil {
		return err
	}

	return tpl.Execute(w, execData{
		Data: schema,
	})
}

type execData struct {
	Data interface{}
}

var defaultTemplate = `
Tables
======

{{ range .Data.Tables }}
{{ snakeToTitle .Name }}
-----------

{{ .Description }}

| Name | Type | Description |
|------|------|-------------|
{{ range .KeyColumns -}}
| {{ .Name }} (KEY)| {{ if .CustomType }}[{{.DataType}}](#{{anchor .DataType}}){{ else }}{{.DataType}}{{ end }} | {{ mdescape .Description}} |
{{ end -}}
{{ range .Columns -}}
| {{ .Name }} | {{ if .CustomType }}[{{.DataType}}](#{{anchor .DataType}}){{ else }}{{.DataType}}{{ end }} | {{ mdescape .Description}} |
{{ end }}

{{ range .ForeignKeys }}
{{ .Name }}
{{ end }}
{{ end }}


Enums
=====

{{ range .Data.Enums }}
{{ snakeToTitle .Name }}
-------------------------
{{ if .Description }}
{{ .Description }}
{{ else }}
{{ range .Values -}}
- {{ . }}
{{ end }}
{{- end }}
{{ end }}`
