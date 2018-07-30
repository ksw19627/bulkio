#include <stdio.h>
#include <stdlib.h>
#include <memory.h>
#include <sql.h>
#include <string.h>
#include <time.h>
#include <sqlext.h>
#include <wchar.h>

#define TRUE 1
#define FALSE 0
#define DESC_LEN 1024

#define CHECK_ERROR(e, s, h, t) ({\
        if (e!=SQL_SUCCESS && e != SQL_SUCCESS_WITH_INFO) {extract_error(s, h, t); goto exit;} \
    })

/* 	Struct columnInfo
 * 	It Contains What transferred from DBMS.
 *  It's made to describe name, type (C_type), scale, sizes per one column
 */
typedef struct columnInfo {
    SQLCHAR column_name[50]; 	//received column_name
    SQLSMALLINT column_type; 	//received types of column (C_type).
    SQLSMALLINT column_scale;	//received column_scale
    SQLINTEGER column_size;		//received column_size
}ColInfo;

/* 	Struct int_buffer
 * 	It Contains (N rows of Column) sets to Column Binding.
 * 	Used to Bind Column sets when the column's type is SQL_C_INTEGER.
 * 	The member 'addcols' is a number of columns which are types of SQL_C_INTEGER.
 */
typedef struct int_buffer{
int addcols;
int ** data;
}int_buf;

typedef struct bigint_buffer{
int addcols;
long long ** data;
}bigint_buf;

typedef struct double_buffer{
int addcols;
double ** data;
}double_buf;

typedef struct bit_buffer{
int addcols;
unsigned char ** data;
}bit_buf;



/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////// Struct char /////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/* 	Struct char_buffer
 * 	It Contains (N rows of Column) sets to Column Binding.
 * 	Used to Bind Column sets when the column's type is SQL_C_CHAR.
 *  In most of cases, char tuples saves string, but there's no string type in clang,
 *  so it needs to char* type to save one string per tuples.
 *  That unhappy facts makes data structure more difficult, so we choosed 3d array to bind each columns of strings.
 * 	The member 'addcols' is a number of columns which are types of SQL_C_CHAR.
 */
typedef struct char_buffer{
int addcols;
char *** data;
}char_buf;

typedef struct wchar_buffer{
int addcols;
wchar_t *** data;
}wchar_buf;

typedef struct binary_buffer{
int addcols;
unsigned char *** data;
}binary_buf;
//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////// Struct DataFrame /////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/* 	Struct DataFrame
 * 	It rules like Headquerter of whole processing.
 * 	each of __ buf is the bind Column sets of SQL_C_ type.
 *  the member Data is 3d pointer. It takes 3d array which contains whole of data to save.
 *  It saves whole of data by row wise method.
 *    'Dsn' = To Connect DBMS with.
 *   	'TableName' = To Insert data in table.
 *   	'Nrow' = Total Row counts of data to insert. (every Column has same Nrows)
 *   	'Ncol'= Total Column counts of data to insert.
 */
typedef struct Dataframe {
	 	  char *Dsn;
	    char *TableName;
	    SQLUSMALLINT Nrow;
	    SQLUSMALLINT Ncol;
			void ***Data;
	    ColInfo *ColumnInfo;


	    wchar_buf *Wchar_buf;
			char_buf *Char_buf;

			bit_buf  *Bit_buf;
	    int_buf  *Int_buf;
	    bigint_buf *BigInt_buf;
	    double_buf *Double_buf;
	    binary_buf *Binary_buf;
	    SQLINTEGER **ind;
}DataFrame;

//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////// Belows are Functions of etc //////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

void parsing_date(char *date, SQL_TIMESTAMP_STRUCT *res);
void extract_error(char *fn, SQLHANDLE handle, SQLSMALLINT type);
void db_connect(char * dsn);

//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////// Belows are Functions of DataFrame /////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

DataFrame* DF_init();
void DF_SetTableName(DataFrame *df, char * tableName);
void DF_Addrow(DataFrame * df, char** row, int rowcols);
void DF_get_columnInfo(DataFrame * df, char * dsn);
void DF_Memfree(DataFrame *df);
void DF_bind_parameter(DataFrame *df, char * dsn);
void DF_NewColumnInfo(DataFrame * df, char * column_name, SQLSMALLINT column_type, SQLINTEGER column_size);
void DF_CheckColumnInfo(DataFrame *df);
