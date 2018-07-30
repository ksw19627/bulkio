package dataframe

/*
#cgo darwin LDFLAGS: -lodbc -L/opt/local/lib
#cgo darwin CFLAGS: -I/opt/local/include
#cgo linux LDFLAGS: -lodbc
#cgo dataframe LDFLAGS: -L /root/Workspace/src/coolor/bulkio/dataframe -ldataframe

#include <stdio.h>
#include <stdlib.h>
#include <memory.h>
#include <sql.h>
#include <string.h>
#include <time.h>
#include <sqlext.h>
#include <dataframe.h>

*/
import "C"
import (
	"beji/odbc/api"
	"unsafe"
)

type ColumnInfo struct {
	column_name string
	column_type api.SQLSMALLINT
	column_size api.SQLINTEGER
}

func NewColumnInfo(colname string, column_typ api.SQLSMALLINT, column_siz api.SQLINTEGER) ColumnInfo {
	//fmt.Println("Test_NewColumnInfo")
	return ColumnInfo{column_name: colname, column_type: column_typ, column_size: column_siz}
}

func NewDataFrame(columninfos ...ColumnInfo) *C.DataFrame {
	//fmt.Println("Test_NewDataFrame")
	df := C.DF_init()
	if columninfos != nil {
		for i := 0; i < len(columninfos); i++ {
			C.DF_NewColumnInfo(df, C.CString(columninfos[i].column_name),
				C.SQLSMALLINT(columninfos[i].column_type), C.SQLINTEGER(columninfos[i].column_size))
		}
	}
	return df
}

func (df *C.DataFrame) SetTableName(tableName string) {
	//fmt.Println("Test_NewDataFrame")
	C.DF_SetTableName(df, C.CString(tableName))
}

func (df *C.DataFrame) ReadODBC(Dsn string, tableName string) *C.DataFrame {
	//fmt.Println("Test_ReadODBC")
	df.SetTableName(tableName)
	C.DF_get_columnInfo(df, C.CString(Dsn))

	return df
}

/*
func (df *C.DataFrame) NewColumnInfo(colname string, column_type api.SQLSMALLINT, column_size api.SQLINTEGER) {
	//fmt.Println("Test_NewColumnInfo")
	C.DF_NewColumnInfo(df, C.CString(colname), C.SQLSMALLINT(column_type), C.SQLINTEGER(column_size))
}
*/

func (df *C.DataFrame) CheckColumnInfo() {
	//fmt.Println("Test_NewColumnInfo")
	C.DF_CheckColumnInfo(df)
}

func (df *C.DataFrame) AddRow(args []string) {
	//fmt.Println("Test_AddRow")
	rows := make([]*C.char, len(args))
	for i, _ := range args {
		rows[i] = C.CString(args[i])
		defer C.free(unsafe.Pointer(rows[i]))
	}
	C.DF_Addrow(df, (**C.char)(unsafe.Pointer(&rows[0])), C.int(len(args)))
}

func (df *C.DataFrame) WriteODBC(Dsn string, tableName string) {
	//fmt.Println("Test_WriteODBC")
	df.SetTableName(tableName)
	C.DF_bind_parameter(df, C.CString(Dsn))
}
