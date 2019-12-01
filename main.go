package main

import (
	"database/sql"
	"flag"
	"fmt"
	"log"
	"strings"
	"time"

	_ "github.com/denisenkom/go-mssqldb"
)

/*
with cte as (
	select a.name FKName, t2.name TableName, c2.name ColumnName, t.name refTableName, c.name refColumnName
	from sys.foreign_keys a
		inner join sys.foreign_key_columns fkc on fkc.constraint_object_id=a.object_id
		inner join sys.tables t on t.object_id=a.referenced_object_id
		inner join sys.columns c on c.object_id=a.referenced_object_id and fkc.referenced_column_id=c.column_id
		inner join sys.tables t2 on t2.object_id=a.parent_object_id
		inner join sys.columns c2 on c2.object_id=a.parent_object_id and fkc.parent_column_id=c2.column_id
	where t.name = 'Table1'
	union all
	select a.name FKName, t2.name TableName, c2.name ColumnName, t.name refTableName, c.name refColumnName
	from sys.foreign_keys a
		inner join sys.foreign_key_columns fkc on fkc.constraint_object_id=a.object_id
		inner join sys.tables t on t.object_id=a.referenced_object_id
		inner join sys.columns c on c.object_id=a.referenced_object_id and fkc.referenced_column_id=c.column_id
		inner join sys.tables t2 on t2.object_id=a.parent_object_id
		inner join sys.columns c2 on c2.object_id=a.parent_object_id and fkc.parent_column_id=c2.column_id
		inner join cte on cte.TableName=t.name
)
select * from cte
*/
var (
	//ParamServer MSSQL Server Address or ip
	ParamServer = flag.String("server", "(local)", "MSSQL Server Addresss or ip")
	//ParamPort MSSQL Server Port
	ParamPort = flag.String("port", "6103", "MSSQL Server Port")
	//ParamUser MSSQL Connect User
	ParamUser = flag.String("user", "sa", "MSSQL Connect User")
	//ParamPass MSSQL Connect Password
	ParamPass = flag.String("pass", "alphapwd", "MSSQL Connect Password")
	//ParamDBName MSSQL Connect DatabaseName
	ParamDBName = flag.String("db", "db01", "MSSQL Connect DatabaseName")
	//ParamSQL SQL
	ParamSQL = flag.String("sql", "select * from Table1 where id in (1,11,21,31)", "SQL")
)

func main() {
	dbconnurl := fmt.Sprintf("server=%s;port=%s;user id=%s;password=%s;database=%s", *ParamServer, *ParamPort, *ParamUser, *ParamPass, *ParamDBName)
	db, err := sql.Open("mssql", dbconnurl)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	var sb strings.Builder

	selectTable(&sb, db, *ParamSQL, "Table1")

	fmt.Print(sb.String())

	// rows, err := db.Query(*ParamSQL)
	// if err != nil {
	// 	log.Fatal(err)
	// }
	// defer rows.Close()

	// cols, err := rows.Columns()
	// if err != nil {
	// 	log.Fatal(err)
	// }
	// vals := make([]interface{}, len(cols))
	// for i := 0; i < len(cols); i++ {

	// }
	// for i, v := range cols {
	// 	vals[i] = new(interface{})
	// 	if i != 0 {
	// 		fmt.Print("\t")
	// 	}
	// 	fmt.Print(v)
	// }
	// fmt.Println()

	// for rows.Next() {
	// 	err = rows.Scan(vals...)
	// 	if err != nil {
	// 		log.Fatal(err)
	// 	}

	// 	for i := 0; i < len(vals); i++ {
	// 		if i != 0 {
	// 			fmt.Print("\t")
	// 		}
	// 		fmt.Print(printValueString(vals[i].(*interface{})))
	// 	}
	// 	fmt.Println()
	// }

}

func selectTable(sb *strings.Builder, db *sql.DB, sqlStr string, tableName string) {
	var sb1 strings.Builder
	rows, err := db.Query(sqlStr)
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		log.Fatal(err)
	}

	vals := make([]interface{}, len(cols))
	for i := range cols {
		vals[i] = new(interface{})
	}

	sb.WriteString("SET IDENTITY_INSERT ")
	sb.WriteString(tableName)
	sb.WriteString(" ON\n")
	insertStr := insertString(tableName, cols)
	for rows.Next() {
		err = rows.Scan(vals...)
		if err != nil {
			log.Fatal(err)
		}
		sb1.WriteString(insertStr)
		sb1.WriteString("(")
		for i := 0; i < len(vals); i++ {
			if i != 0 {
				sb1.WriteString(", ")
			}
			sb1.WriteString(printValueString(vals[i].(*interface{})))
		}
		sb1.WriteString(")\n")
	}

	sb.WriteString(sb1.String())

	sb.WriteString("SET IDENTITY_INSERT ")
	sb.WriteString(tableName)
	sb.WriteString(" OFF\n")

}

func insertString(tableName string, cols []string) string {
	var sb strings.Builder
	sb.WriteString("INSERT INTO ")
	sb.WriteString(tableName)
	sb.WriteString(" (")
	for i, v := range cols {
		if i != 0 {
			sb.WriteString(", ")
		}
		sb.WriteString(v)
	}
	sb.WriteString(") VALUES ")

	return sb.String()
}

func printValueString(pval *interface{}) (rtn string) {
	switch v := (*pval).(type) {
	case nil:
		rtn = fmt.Sprint("NULL")
	case bool:
		if v {
			rtn = fmt.Sprint("1")
		} else {
			rtn = fmt.Sprint("0")
		}
	case []byte:
		rtn = fmt.Sprint(string(v))
	case time.Time:
		rtn = fmt.Sprint("'", v.Format("2006-01-02 15:04:05.999"), "'")
	case int:
		rtn = fmt.Sprint(v)
	case int32:
		rtn = fmt.Sprint(v)
	case int64:
		rtn = fmt.Sprint(v)
	default:
		rtn = fmt.Sprint("'", v, "'")
	}
	return rtn
}
