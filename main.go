package main

import (
	"fmt"
	"gorm.io/gorm"
	"gorm.io/driver/mysql"
	"github.com/spf13/viper"
	"strings"
)

type Config struct {
	Type string `mapstructure:"type"` // 类型，mysql
	Url string `mapstructure:"url"`
	Database string `mapstructure`
}



func main(){
	viper.SetConfigName("config") // name of config file (without extension)
	viper.SetConfigType("yaml") // REQUIRED if the config file does not have the extension in the name
	viper.AddConfigPath(".")               // optionally look for config in the working directory
	err := viper.ReadInConfig() // Find and read the config file
	if err != nil { // Handle errors reading the config file
		panic(fmt.Errorf("Fatal error config file: %w \n", err))
	}
	var cfg Config
	viper.Unmarshal(&cfg)

	// cfg.Type
	db, err := gorm.Open(mysql.Open(cfg.Url), &gorm.Config{})
	if err!=nil{
		fmt.Println(err)
		panic(err)
		return
	}

	rows, err := db.Raw(fmt.Sprintf(`SELECT TABLE_NAME,TABLE_COMMENT FROM information_schema.TABLES WHERE table_schema='%s'`, cfg.Database)).Rows()
	if err!=nil{
		panic(err)
		return
	}
	tables := make([]Table,0)
	for rows.Next() {
		//cols, _ := rows.Columns()

		TABLE_NAME := ""
		TABLE_COMMENT := ""
		rows.Scan(&TABLE_NAME, &TABLE_COMMENT)
		fmt.Printf("table name = %s\n", TABLE_NAME)
		table := Table{
			Name: TABLE_NAME,
			Comment: TABLE_COMMENT,
		}
		columns := make([]Column,0)
		cols, _ := db.Raw(fmt.Sprintf(`SHOW FULL COLUMNS FROM %s`, TABLE_NAME)).Rows()
		for cols.Next() {
			var col Column
			db.ScanRows(cols, &col)
			columns = append(columns, col)
			table.Columns = columns
			fmt.Println(col.ToString())
		}
		tables = append(tables, table)
	}

	fmt.Println("==================================")
	for _,table := range tables {
		//fmt.Printf("%s\r\n", table.ToPgSql())
		table.ToPgSql()
	}


}



type Table struct {
	Name string
	Comment string
	Columns []Column
}

type Column struct {
	Field string
	Type string
	Collation string
	Null string
	Key string
	Default string
	Extra string
	Privileges string
	Comment string
}



func (table Table)ToPgSql()string{
	sql := fmt.Sprintf("CREATE TABLE %s (\r\n", table.Name)
	comments := ""
	size := len(table.Columns)
	for i, col := range table.Columns {
		colstr, commentstr := col.MySQL2Pg(table.Name)
		if size -i == 1 {
			colstr = colstr[:len(colstr)-1]
		}
		sql += colstr+"\r\n"
		if commentstr!=""{
			comments += commentstr +"\r\n"
		}
	}
	sql += "};"
	if table.Comment!=""{
		comments += fmt.Sprintf("COMMENT ON TABLE %s IS '%s';\r\n", table.Name, table.Comment)
	}
	fmt.Println(sql)
	fmt.Println(comments)
	return sql +"\r\n"+ comments;
}

func (col Column)MySQL2Pg(table string)(string,string){
	datatype := ""
	if strings.Index(col.Type, "bigint") >= 0 {
		datatype = "int8"
	}else if strings.Index(col.Type, "datetime") >= 0 {
		datatype = "timestamp without timezone"
	}else {
		datatype = col.Type
	}
	isPrimaryKey := ""
	if col.Key == "PRI"{
		isPrimaryKey = "primary key"
	}
	isnull := "NULL"
	if col.Null == "NO" {
		isnull = "NOT NULL"
	}
	colstr := fmt.Sprintf("%s %s %s %s,", col.Field, datatype, isnull, isPrimaryKey)
	if col.Comment!=""{
		commentstr := fmt.Sprintf("COMMENT ON COLUMN %s.%s IS '%s';", table, col.Field, col.Comment)
		return colstr, commentstr
	}else{
		return colstr, ""
	}
}

func(col Column)ToString()string{
	return fmt.Sprintf("field=%s, type=%s, collcation = %s, null=%s, key=%s, default=%s, extra=%s priviledges=%s comment=%s\n", col.Field,
		col.Type, col.Collation, col.Null, col.Key, col.Default, col.Extra, col.Privileges, col.Comment)
}