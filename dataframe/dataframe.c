#include <stdio.h>
#include <stdlib.h>
#include <memory.h>
#include <sql.h>
#include <string.h>
#include <time.h>
#include <sqlext.h>
#include <wchar.h>
#include "dataframe.h"

SQLHENV   henv  = SQL_NULL_HENV;   // Environment
SQLHDBC   hdbc  = SQL_NULL_HDBC;   // Connection handle
SQLHSTMT  hstmt = SQL_NULL_HSTMT;  // Statement handle
SQLRETURN retcode;
SQLUSMALLINT *ParamStatusArray;
SQLUINTEGER ParamsProcessed;

int i, j;



void parsing_date(char *date, SQL_TIMESTAMP_STRUCT *res)
{
    struct tm result;
    time_t epoch;
    memset(&result, 0, sizeof(struct tm));

    if(strptime(date, "%Y-%m-%d %H:%M:%S" , &result) == NULL)
        printf("\nstrptime failed: %s\n", date);
    else
    {
        res->year = result.tm_year+1900;
        res->month = result.tm_mon+1;
        res->day = result.tm_mday;
        res->hour = result.tm_hour;
        res->minute = result.tm_min;
        res->second = result.tm_sec;
    }

   return;
}

void db_connect(char * dsn){

  retcode = SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &henv);
  CHECK_ERROR(retcode, "SQLAllocHandle(ENV)",
      henv, SQL_HANDLE_ENV);

  retcode = SQLSetEnvAttr(henv, SQL_ATTR_ODBC_VERSION,
    (SQLPOINTER*)SQL_OV_ODBC3, 0);
  CHECK_ERROR(retcode, "SQLSetEnvAttr(SQL_ATTR_ODBC_VERSION)",
        henv, SQL_HANDLE_ENV);

  retcode = SQLAllocHandle(SQL_HANDLE_DBC, henv, &hdbc);
  CHECK_ERROR(retcode, "SQLAllocHandle(DBC)",
      henv, SQL_HANDLE_ENV);

  retcode = SQLSetConnectAttr(hdbc, SQL_LOGIN_TIMEOUT, (SQLPOINTER)5, 0);
  CHECK_ERROR(retcode, "SQLSetConnectAttr(SQL_LOGIN_TIMEOUT)",
      hdbc, SQL_HANDLE_DBC);

  retcode = SQLConnect(hdbc, (SQLCHAR*)dsn, SQL_NTS,
  (SQLCHAR*) NULL, 0, NULL, 0);
  CHECK_ERROR(retcode, "SQLConnect(SQL_HANDLE_DBC)",
      hdbc, SQL_HANDLE_DBC);

  retcode = SQLSetConnectAttr(hdbc, SQL_ATTR_AUTOCOMMIT,(SQLPOINTER)TRUE, 0);
  CHECK_ERROR(retcode, "SQLSetConnectAttr(SQL_ATTR_AUTOCOMMIT)",
  hdbc, SQL_HANDLE_DBC);

    return;

exit:
    printf ("\nERROR EXIT.\n");
    return;
}

void extract_error(char *fn, SQLHANDLE handle, SQLSMALLINT type)
{
    printf("Error : %s\n", fn);

        SQLRETURN rc;
        SQLSMALLINT sRecordNo;
        SQLCHAR sSQLSTATE[6];
        SQLCHAR sMessage[2048];
        SQLSMALLINT sMessageLength;
        SQLINTEGER sNativeError;
        sRecordNo = 1;

        while ((rc = SQLGetDiagRec(type,
        handle,
        sRecordNo,
        sSQLSTATE,
        &sNativeError,
        sMessage,
        sizeof(sMessage),
        &sMessageLength)) != SQL_NO_DATA)
        {
                printf("Diagnostic Record %d\n", sRecordNo);
                printf(" SQLSTATE : %s\n", sSQLSTATE);
                printf(" Message text : %s\n", sMessage);
                printf(" Message len : %d\n", sMessageLength);
                printf(" Native error : 0x%X\n", sNativeError);

                if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
                {
                        break;
                }

                sRecordNo++;
        }
}

//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////// Belows are Functions of DataFrame /////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

DataFrame* DF_init()
{
	printf("DF malloc:   \t");
	DataFrame *df = (DataFrame*)malloc(sizeof(DataFrame));
	printf("\tSuccess \n");
	printf("Init DF Row:   \t");
	df->Nrow = 0;
	printf("\tSuccess \n");
	printf("Init DF Col:   \t");
	df->Ncol = 0;
	printf("\tSuccess \n");

	printf("Init DF Data: \t");

	df->Bit_buf = NULL;
	df->Int_buf = NULL;
	df->BigInt_buf = NULL;
	df->Char_buf =NULL;
	df->Wchar_buf =NULL;
	df->Double_buf = NULL;
	df->Binary_buf = NULL;
  df->Data = NULL;
	printf("\tSuccess \n");
	return df;
}

void DF_SetTableName(DataFrame *df, char * tableName)
{
  df->TableName = tableName;
}

void DF_Addrow(DataFrame * df, char ** row, int rowcols)
{
  if (df->ColumnInfo == NULL) {
    printf("ERROR :Need To Input ColumnInfo \n");
  }
  if (rowcols != df->Ncol){
    printf("ERROR :Mismatching Row's Columns:(%d). expected number of Columns :(%d) \n", rowcols, df->Ncol);
  }

	int i;

  for(i=0; i < df->Ncol; i++){
    printf("%s \n", row[i]);
  }

  if(df->Data == NULL){
  df->Nrow++;
  df->Data = df->Data = (void***)malloc(sizeof(void**)*df->Nrow);
  df->Data[0] = (void**)malloc(df->Ncol * sizeof(void*));
  memset(df->Data[0], 0x00, df->Ncol * sizeof(void*));
  }else{
  df->Nrow++;
  df->Data = (void***)realloc(df->Data, sizeof(void**)*df->Nrow);
  df->Data[df->Nrow-1] = (void**) malloc(sizeof(void*) * df->Ncol);
  memset(df->Data[df->Nrow-1], 0x00, df->Ncol * sizeof(void*));
  }

	for ( i=0; i<df->Ncol; i++){
	if(df->ColumnInfo[i].column_type== SQL_C_BIT){
		df->Data[(df->Nrow)-1][i] = (char*)malloc(sizeof(char));
		df->Data[(df->Nrow)-1][i] = row[i];
	}else if(df->ColumnInfo[i].column_type== SQL_C_LONG){
		df->Data[(df->Nrow)-1][i] = (int*)malloc(sizeof(int));
		int var = atoi(row[i]);
		df->Data[(df->Nrow)-1][i] = var;
	}else if(df->ColumnInfo[i].column_type== SQL_C_DOUBLE){
		df->Data[(df->Nrow)-1][i] = (double*)malloc(sizeof(double)*1);
		double var = atof(row[i]);
  	df->Data[(df->Nrow)-1][i] = &var;
  }else if(df->ColumnInfo[i].column_type== SQL_C_SBIGINT){
		df->Data[(df->Nrow)-1][i] = (long long*)malloc(sizeof(long long));
		long long var = atoll(row[i]);
		df->Data[(df->Nrow)-1][i] = &var;
		//printf("%d \n", df->Data[(df->Nrow)-1][i]);
	}else if(df->ColumnInfo[i].column_type== SQL_C_BINARY){
  	df->Data[(df->Nrow)-1][i] = (unsigned char*)malloc(sizeof(unsigned char));
  	strcpy(df->Data[(df->Nrow)-1][i], row[i]);
    //printf("%s \n", df->Data[(df->Nrow)-1][i]);
	}else if(df->ColumnInfo[i].column_type== SQL_C_CHAR){
		df->Data[(df->Nrow)-1][i] = (char*)malloc(df->ColumnInfo[i].column_size);
		strcpy(df->Data[(df->Nrow)-1][i], row[i]);
		//printf("%s \n", df->Data[(df->Nrow)-1][i]);
	}else if(df->ColumnInfo[i].column_type== SQL_C_WCHAR){
		df->Data[(df->Nrow)-1][i] = (wchar_t*)malloc(df->ColumnInfo[i].column_size);
		strcpy(df->Data[(df->Nrow)-1][i], row[i]);
		//printf("%s \n", df->Data[(df->Nrow)-1][i]);
	}else if(df->ColumnInfo[i].column_type== SQL_C_TYPE_TIMESTAMP){
		df->Data[(df->Nrow)-1][i] = (SQL_TIMESTAMP_STRUCT*) malloc( sizeof(SQL_TIMESTAMP_STRUCT) );
		parsing_date(row[i],(SQL_TIMESTAMP_STRUCT*)(df->Data[(df->Nrow)-1][i]));
	}
}

}

void DF_NewColumnInfo(DataFrame * df, char * column_name, SQLSMALLINT column_type, SQLINTEGER column_size){
  int i;
  if(df->ColumnInfo ==NULL){
  df->Ncol++;
  df->ColumnInfo = (ColInfo*)malloc(sizeof(ColInfo) * df->Ncol);
  }else{
  df->Ncol++;
  ColInfo *tmp= (ColInfo*)realloc(df->ColumnInfo, sizeof(ColInfo)*df->Ncol);
  if(tmp)
    df->ColumnInfo = tmp;
  else
    printf("Realloc error.\n");
  }

  strcpy(df->ColumnInfo[df->Ncol-1].column_name,(SQLCHAR*)column_name);
  df->ColumnInfo[df->Ncol-1].column_type = column_type;
  df->ColumnInfo[df->Ncol-1].column_size = column_size+1;
  df->ColumnInfo[df->Ncol-1].column_scale= 0;
}


void DF_CheckColumnInfo(DataFrame *df){
  int i;
  for (i=0; i< df->Ncol; i++){
  printf("Col No.:\t %d\t", i);
  printf("Name:\t  %s\t", df->ColumnInfo[i].column_name);
  printf("Type:\t  %d\t", df->ColumnInfo[i].column_type);
  printf("Size:\t %d \t\n", df->ColumnInfo[i].column_size);
  }
}

void DF_get_columnInfo(DataFrame * df, char * dsn){

	int i, j;
	char query[1024];
	SQLINTEGER columnSize;
	SQLCHAR columnName[50];
	SQLSMALLINT columnNum;
	SQLSMALLINT columnNameLength;
	SQLSMALLINT dataType;
	SQLSMALLINT scale;
	SQLSMALLINT nullable;
	df->Dsn = dsn;

  db_connect(df->Dsn);

	retcode = SQLAllocHandle(SQL_HANDLE_STMT, hdbc, &hstmt);
	CHECK_ERROR(retcode, "SQLAllocHandle(SQL_HANDLE_STMT)",
	hstmt, SQL_HANDLE_STMT);
  SQLSetStmtAttr(hstmt, SQL_ATTR_CONCURRENCY,(SQLPOINTER)SQL_CONCUR_LOCK ,0);
	SQLSetStmtAttr(hstmt, SQL_ATTR_CURSOR_TYPE,(SQLPOINTER)SQL_CURSOR_KEYSET_DRIVEN, 0);

	sprintf(query, "SELECT * FROM %s", df->TableName);
	retcode = SQLExecDirect(hstmt, (SQLCHAR*) query , SQL_NTS);
	CHECK_ERROR(retcode, "SQLExecDirect()", hstmt, SQL_HANDLE_STMT);

	SQLNumResultCols(hstmt, &columnNum);

	    df->Ncol = columnNum;
	    df->ColumnInfo = (ColInfo*)malloc(sizeof(ColInfo) * df->Ncol);

	    for (i=0; i<df->Ncol; i++)
	        {
	        SQLDescribeCol(hstmt, i+1,
	        (SQLCHAR *)columnName, sizeof(columnName), &columnNameLength,
	        &dataType, &columnSize, &scale, &nullable);
					strcpy(df->ColumnInfo[i].column_name,columnName);

				  switch (dataType){
	            case SQL_BIT:
	            	df->ColumnInfo[i].column_type = SQL_C_BIT;
	            	df->ColumnInfo[i].column_size = 1;
	            	df->ColumnInfo[i].column_scale = scale;
	           	  break;
							case SQL_TINYINT:
							case SQL_SMALLINT:
							case SQL_INTEGER:
							 	df->ColumnInfo[i].column_type = SQL_C_LONG;
							 	df->ColumnInfo[i].column_size = 4;
							 	df->ColumnInfo[i].column_scale = scale;
							 break;
							case SQL_BIGINT:
								 df->ColumnInfo[i].column_type = SQL_C_SBIGINT;
								 df->ColumnInfo[i].column_size = 8;
								 df->ColumnInfo[i].column_scale = scale;
								 break;
							 case SQL_FLOAT:
							 case SQL_REAL:
							 case SQL_DOUBLE:
 								 df->ColumnInfo[i].column_type = SQL_C_DOUBLE;
 								 df->ColumnInfo[i].column_size = 8;
 								 df->ColumnInfo[i].column_scale = scale;
 								 break;
						  case SQL_NUMERIC:
							case SQL_DECIMAL:
 								 df->ColumnInfo[i].column_type = SQL_C_CHAR;
 								 df->ColumnInfo[i].column_size = columnSize;
 								 df->ColumnInfo[i].column_scale = scale;
 								 break;
						 case SQL_TYPE_TIMESTAMP:
								df->ColumnInfo[i].column_type = SQL_C_TYPE_TIMESTAMP;
								df->ColumnInfo[i].column_size = sizeof(SQL_TIMESTAMP_STRUCT);
							  df->ColumnInfo[i].column_scale = scale;
									 break;
						 case SQL_TYPE_DATE:
						  	 df->ColumnInfo[i].column_type = SQL_C_DATE;
						  	 df->ColumnInfo[i].column_size = sizeof(SQL_DATE_STRUCT);
							 	 df->ColumnInfo[i].column_scale = scale;
						 			break;
						case SQL_TYPE_TIME:
								df->ColumnInfo[i].column_type = SQL_C_TIME;
								df->ColumnInfo[i].column_size = sizeof(SQL_TIME_STRUCT);
								df->ColumnInfo[i].column_scale = scale;
								 break;
	            case SQL_GUID:
						  	df->ColumnInfo[i].column_type = SQL_C_GUID;
	            	df->ColumnInfo[i].column_size = sizeof(SQLGUID);
	            	df->ColumnInfo[i].column_scale = scale;
	                break;
	            case SQL_VARCHAR:
							case SQL_CHAR:
	                df->ColumnInfo[i].column_type = SQL_C_CHAR;
	                df->ColumnInfo[i].column_size = columnSize+1;
	                df->ColumnInfo[i].column_scale = scale;
	                break;
							case SQL_WVARCHAR:
							case SQL_WCHAR:
							    df->ColumnInfo[i].column_type = SQL_C_WCHAR;
							    df->ColumnInfo[i].column_size = (columnSize+1)*2;
							    df->ColumnInfo[i].column_scale = scale;
							    break;
	            case SQL_BINARY:
						  case SQL_VARBINARY:
					        df->ColumnInfo[i].column_type = SQL_C_BINARY;
	                df->ColumnInfo[i].column_size = columnSize;
	                df->ColumnInfo[i].column_scale = scale;
	                break;
	            case SQL_LONGVARCHAR:
	               df->ColumnInfo[i].column_type = SQL_C_CHAR;
	               df->ColumnInfo[i].column_size = 0;
	               break;
	            case SQL_WLONGVARCHAR:
	               df->ColumnInfo[i].column_type = SQL_C_WCHAR;
	               df->ColumnInfo[i].column_size = 0;
	               break;
	            case SQL_LONGVARBINARY:
	               df->ColumnInfo[i].column_type = SQL_C_BINARY;
	               df->ColumnInfo[i].column_size = 0;
	               break;
						 default:
						 printf("unsupported column type %d", dataType);
						 return;
	            }
	        }

					for (i=0; i< df->Ncol; i++){
					printf("Col No.:\t %d\t", i);
					printf("Name:\t  %s\t", df->ColumnInfo[i].column_name);
					printf("Type:\t  %d\t", df->ColumnInfo[i].column_type);
					printf("Size:\t %d \t\n", df->ColumnInfo[i].column_size);
					}
exit:
  SQLFreeHandle(SQL_HANDLE_STMT, hstmt);
}


void DF_Memfree(DataFrame *df){
	int i, j, k;
	printf("Free DF Int_buf: \t");
	if(df->Int_buf != NULL){
	for (i=0; i < df->Int_buf->addcols; i++){
			free(df->Int_buf->data[i]);
	}
	free(df->Int_buf->data);
	free(df->Int_buf);
	}
	printf("Success \n");
	printf("Free DF Char_buf: \t");
	if(df->Char_buf != NULL){
	for (i=0; i < df->Char_buf->addcols; i++){
		for(j=0; j< df->Nrow; j++){
			free(df->Char_buf->data[i][j]);
			//printf("Yes %d \n", j);
		}
	free(df->Char_buf->data[i]);
	}
	free(df->Char_buf->data);
	free(df->Char_buf);
	}
	printf("Success \n");
	printf("Free DF Data: \t");
	for(i=0; i< df->Nrow; i++){
		free(df->Data[i]);
	}
	free(df->Data);
	printf("\tSuccess \n");
	printf("Free DF ColInfo:   \t");
	free(df->ColumnInfo);
	printf("Success \n");
  printf("Free DF Ind: \t");
	if(df->ind != NULL){
  for (i=0; i< df->Ncol; i++){
		free(df->ind[i]);
	}
	free(df->ind);
	}
	printf("\tSuccess \n");
	printf("Free DF struct:   \t");
	free(df);
	printf("Success \n");
}

void DF_bind_parameter(DataFrame *df, char * dsn){

	int i=0, j=0, k=0;
	char Statement[1000] = "Insert Into ";
	char Col_Names[500] = "";
	char Questions[500] = "";
  SQLLEN value_len;
	df->Dsn = dsn;

  db_connect(df->Dsn);

	  retcode = SQLAllocHandle(SQL_HANDLE_STMT, hdbc, &hstmt);
    CHECK_ERROR(retcode, "SQLAllocHandle(SQL_HANDLE_STMT)",
    hstmt, SQL_HANDLE_STMT);
		// Set the SQL_ATTR_PARAM_BIND_TYPE statement attribute to use
		// column-wise binding.

    SQLSetStmtAttr(hstmt, SQL_ATTR_CONCURRENCY,(SQLPOINTER)SQL_CONCUR_LOCK ,0);
    SQLSetStmtAttr(hstmt, SQL_ATTR_CURSOR_TYPE,(SQLPOINTER)SQL_CURSOR_KEYSET_DRIVEN, 0);
    SQLSetStmtAttr(hstmt, SQL_ATTR_PARAM_BIND_TYPE, SQL_PARAM_BIND_BY_COLUMN, 0);
    // Specify the number of elements in each parameter array.
	  SQLSetStmtAttr(hstmt, SQL_ATTR_PARAMSET_SIZE, (SQLPOINTER)df->Nrow, 0);
    // Specify an array in which to return the status of each set of
	   // parameters.
    //SQLSetStmtAttr(hstmt, SQL_ATTR_ROW_BIND_TYPE, SQL_BIND_BY_COLUMN, 0);
     // Specify the number of elements in each parameter array.

    //SQLSetStmtAttr(hstmt, SQL_ATTR_ROW_ARRAY_SIZE, (SQLPOINTER)df->Nrow, 0);
     // Specify an array in which to return the status of each set of
     // parameters.

	  ParamStatusArray = (SQLUSMALLINT*)malloc(sizeof(SQLUSMALLINT)* df->Nrow);
	  SQLSetStmtAttr(hstmt, SQL_ATTR_PARAM_STATUS_PTR, ParamStatusArray, 0);
    // Specify an SQLUINTEGER value in which to return the number of sets of
	   // parameters processed.
	  SQLSetStmtAttr(hstmt, SQL_ATTR_PARAMS_PROCESSED_PTR, &ParamsProcessed, 0);

	  df->ind = (SQLINTEGER**)malloc(sizeof(SQLINTEGER*)*df->Ncol);

		for (i=0; i < df->Ncol; i++){
		strcat(Col_Names,df->ColumnInfo[i].column_name);
		strcat(Questions, "?");
		if(i != (df->Ncol-1)){
		strcat(Col_Names,", ");
		strcat(Questions,", ");
		}
		df->ind[i]	= (SQLINTEGER*)malloc(sizeof(SQLINTEGER)*df->Nrow);

		switch (df->ColumnInfo[i].column_type){
		case SQL_C_BIT:
		if(df->Bit_buf == NULL){
		df->Bit_buf =malloc(sizeof(bit_buf));
		df->Bit_buf->addcols = 1;
		df->Bit_buf->data = (unsigned char**)malloc(sizeof(unsigned char*));
		df->Bit_buf->data[0] = (unsigned char*)malloc(sizeof(unsigned char)* df->Nrow);
	 }else{
			df->Bit_buf->addcols++;
			unsigned char **tmp = (unsigned char**)realloc(df->Bit_buf->data, sizeof *(df->Bit_buf->data) *df->Bit_buf->addcols);
			if(tmp)
			{
				df->Bit_buf->data = tmp;
				df->Bit_buf->data[df->Bit_buf->addcols-1] = (unsigned char*)malloc(sizeof(unsigned char)*df->Nrow);
			}
		}
		for (j=0; j < df->Nrow; j++){
				df->Bit_buf->data[df->Bit_buf->addcols-1][j] = df->Data[j][i];
				df->ind[i][j] = SQL_NTS;
			}
		SQLBindParameter(hstmt, i+1, SQL_PARAM_INPUT, df->ColumnInfo[i].column_type,
		SQL_BIT, df->ColumnInfo[i].column_size, df->ColumnInfo[i].column_scale, (SQLPOINTER)df->Bit_buf->data[df->Bit_buf->addcols-1], sizeof(df->Bit_buf->data[df->Bit_buf->addcols-1]), df->ind[i]);
		printf("SQL_C_BIT SUCCESS \n" );
		break;
		case SQL_C_LONG:
		if(df->Int_buf == NULL){
		df->Int_buf =malloc(sizeof(int_buf));
		df->Int_buf->addcols = 1;
		df->Int_buf->data = (int**)malloc(sizeof(int*));
		df->Int_buf->data[0] = (int*)malloc(sizeof(int)* df->Nrow);
	 }else{
			df->Int_buf->addcols++;
			int **tmp = (int**)realloc(df->Int_buf->data, sizeof *(df->Int_buf->data) *df->Int_buf->addcols);
			if(tmp)
			{
				df->Int_buf->data = tmp;
				df->Int_buf->data[df->Int_buf->addcols-1] = (int*)malloc(sizeof(int)*df->Nrow);
			}
		}
		for (j=0; j < df->Nrow; j++){
				df->Int_buf->data[df->Int_buf->addcols-1][j] = df->Data[j][i];
				df->ind[i][j] = SQL_NTS;
			}
			SQLBindParameter(hstmt, i+1, SQL_PARAM_INPUT, df->ColumnInfo[i].column_type,
			SQL_INTEGER, df->ColumnInfo[i].column_size, df->ColumnInfo[i].column_scale, (SQLPOINTER)df->Int_buf->data[df->Int_buf->addcols-1], sizeof(df->Int_buf->data[df->Int_buf->addcols-1]), df->ind[i]);
			printf("SQL_C_LONG SUCCESS \n" );
			break;
		case SQL_C_DOUBLE:
	//		if(df->Double_buf == NULL){
	//			df->Double_buf =malloc(sizeof(double_buf));
	//			df->Double_buf->addcols = 1;
	//			df->Double_buf->data = (double**)malloc(sizeof(double*));
	//			df->Double_buf->data[0] = (double*)malloc(sizeof(double)* df->Nrow);
  //		 }else{
	//				df->Double_buf->addcols++;
	//				double **tmp = (double**)realloc(df->Double_buf->data, sizeof *(df->Double_buf->data) *df->Double_buf->addcols);
	//				if(tmp)
	//				{
	//					df->Double_buf->data = tmp;
	//					df->Double_buf->data[df->Double_buf->addcols-1] = (double*)malloc(sizeof(double)*df->Nrow);
	//				}
	//			}
  //      		for (j=0; j < df->Nrow; j++){
  //          double d = df->Data[j][i];
  //        //  df->Double_buf->data[df->Double_buf->addcols-1][j] = d;
	//					df->ind[i][j] = SQL_NTS;
	//				}
		SQLBindParameter(hstmt, i+1, SQL_PARAM_INPUT, df->ColumnInfo[i].column_type,
		SQL_FLOAT, df->ColumnInfo[i].column_size, df->ColumnInfo[i].column_scale, (SQLPOINTER)df->Data[0][i], 0, df->ind[i]);
		printf("SQL_C_DOUBLE SUCCESS \n" );
		break;
		case SQL_C_SBIGINT:
				if(df->BigInt_buf == NULL){
				df->BigInt_buf =malloc(sizeof(bigint_buf));
				df->BigInt_buf->addcols = 1;
				df->BigInt_buf->data = (long long**)malloc(sizeof(long long*));
				df->BigInt_buf->data[0] = (long long*)malloc(sizeof(long long)* df->Nrow);
			 }else{
					df->BigInt_buf->addcols++;
					long long **tmp = (long long**)realloc(df->BigInt_buf->data, sizeof *(df->BigInt_buf->data) *df->BigInt_buf->addcols);
					if(tmp)
					{
						df->BigInt_buf->data = tmp;
						df->BigInt_buf->data[df->BigInt_buf->addcols-1] = (long long*)malloc(sizeof(long long)*df->Nrow);
					}
				 }
				for (j=0; j < df->Nrow; j++){
						df->BigInt_buf->data[df->BigInt_buf->addcols-1][j] = df->Data[j][i];
						df->ind[i][j] = SQL_NTS;
					}
		SQLBindParameter(hstmt, i+1, SQL_PARAM_INPUT, df->ColumnInfo[i].column_type,
		SQL_BIGINT, df->ColumnInfo[i].column_size, df->ColumnInfo[i].column_scale, (SQLPOINTER)df->BigInt_buf->data[df->BigInt_buf->addcols-1], sizeof(df->BigInt_buf->data[df->BigInt_buf->addcols-1]), df->ind[i]);
		printf("SQL_C_BIGINT SUCCESS \n" );
		break;
		case SQL_C_BINARY:
			if(df->Binary_buf == NULL){
					df->Binary_buf =(binary_buf*)malloc(sizeof(binary_buf));
					df->Binary_buf->addcols=1;
					int y;
					df->Binary_buf->data = (unsigned char***) malloc(sizeof(unsigned char**) * df->Binary_buf->addcols);
					memset(df->Binary_buf->data, 0x00, sizeof(unsigned char**)*df->Binary_buf->addcols);
					df->Binary_buf->data[0]= (unsigned char**)malloc(df->Nrow * sizeof(unsigned char*));
					memset(df->Binary_buf->data[0], 0x00, df->Nrow * sizeof(unsigned char*));
					for(y=0; y<df->Nrow; y++)
					df->Binary_buf->data[0][y] = (unsigned char*)malloc(sizeof(unsigned char)*df->ColumnInfo[i].column_size);
					}else{
					df->Binary_buf->addcols++;
					int y;
					df->Binary_buf->data = (unsigned char***) realloc(df->Binary_buf->data, sizeof(unsigned char**) * df->Binary_buf->addcols);
					df->Binary_buf->data[df->Char_buf->addcols-1] =(unsigned char**)malloc(df->Nrow * sizeof(unsigned char*));
					memset(df->Binary_buf->data[df->Binary_buf->addcols-1], 0x00, df->Nrow * sizeof(unsigned char*));
					for(y=0; y<df->Nrow; y++)
					df->Binary_buf->data[df->Binary_buf->addcols-1][y] = (unsigned char*)malloc(sizeof(unsigned char)*df->ColumnInfo[i].column_size);
					}

					for (j=0; j <df->Nrow; j++){
						strncpy(df->Binary_buf->data[df->Binary_buf->addcols-1][j], df->Data[j][i], df->ColumnInfo[i].column_size);
						//printf("%s \n", df->Char_buf->data[df->Char_buf->addcols-1][j]);
						//printf("%d \n", strlen(df->Char_buf->data[df->Char_buf->addcols-1][j]));
						df->ind[i][j] = strlen(df->Binary_buf->data[df->Binary_buf->addcols-1][j]);
					}

					SQLBindParameter(hstmt, i+1, SQL_PARAM_INPUT, df->ColumnInfo[i].column_type,
					SQL_BINARY, df->ColumnInfo[i].column_size, df->ColumnInfo[i].column_scale, (SQLPOINTER)df->Binary_buf->data[df->Binary_buf->addcols-1][0], 0, df->ind[i]);
					printf("SQL_C_BINARY SUCCESS \n" );
					break;
		break;
		case SQL_C_CHAR:
			if(df->Char_buf == NULL){
			df->Char_buf =(char_buf*)malloc(sizeof(char_buf));
			df->Char_buf->addcols=1;
			int y;
			df->Char_buf->data = (char***) malloc(sizeof(char**) * df->Char_buf->addcols);
			memset(df->Char_buf->data, 0x00, sizeof(char**)*df->Char_buf->addcols);
			df->Char_buf->data[0]= (char**)malloc(df->Nrow * sizeof(char*));
			memset(df->Char_buf->data[0], 0x00, df->Nrow * sizeof(char*));
			for(y=0; y<df->Nrow; y++)
			df->Char_buf->data[0][y] = (char*)malloc(sizeof(char)*df->ColumnInfo[i].column_size);
			}else{
			df->Char_buf->addcols++;
			int y;
			df->Char_buf->data = (char***) realloc(df->Char_buf->data, sizeof(char**) * df->Char_buf->addcols);
			df->Char_buf->data[df->Char_buf->addcols-1] =(char**)malloc(df->Nrow * sizeof(char*));
			memset(df->Char_buf->data[df->Char_buf->addcols-1], 0x00, df->Nrow * sizeof(char*));
			for(y=0; y<df->Nrow; y++)
			df->Char_buf->data[df->Char_buf->addcols-1][y] = (char*)malloc(sizeof(char)*df->ColumnInfo[i].column_size);

			}

			for (j=0; j <df->Nrow; j++){
        //printf("%s \n", df->Data[j][i]);
        memset(df->Char_buf->data[df->Char_buf->addcols-1][j], 0x00, sizeof(char)*df->ColumnInfo[i].column_size);
  			strcpy(df->Char_buf->data[df->Char_buf->addcols-1][j], df->Data[j][i]);
    //    for (k=strlen(df->Data[j][i]); k<df->ColumnInfo[i].column_size; k++){
    //    df->Char_buf->data[df->Char_buf->addcols-1][j][k]= " ";
    //    }
    //    df->Char_buf->data[df->Char_buf->addcols-1][j][df->ColumnInfo[i].column_size-1] = "\0";
		//		printf("%d \n", strlen(df->Char_buf->data[df->Char_buf->addcols-1][j]));
			//df->ind[i][j] = strlen(df->Char_buf->data[df->Char_buf->addcols-1][j]);
      //df->ind[i][j] = strlen(df->Char_buf->data[df->Char_buf->addcols-1][j]);
      //printf("%d \n", df->ColumnInfo[i].column_size);
      //printf("%s \n", df->Char_buf->data[df->Char_buf->addcols-1][j]);
      //printf("%d \n", strlen(df->Char_buf->data[df->Char_buf->addcols-1][j]));
      //df->ind[i][j] = df->ColumnInfo[i].column_size;
      //df->ind[i][j] = strlen(df->Char_buf->data[df->Char_buf->addcols-1][j]);
        value_len =strlen(df->Char_buf->data[df->Char_buf->addcols-1][j]);
        df->ind[i][j] = strlen(df->Char_buf->data[df->Char_buf->addcols-1][j]);
      }


      //SQLBindParameter(hstmt, i+1, SQL_PARAM_INPUT, df->ColumnInfo[i].column_type,
      //SQL_CHAR, df->ColumnInfo[i].column_size, df->ColumnInfo[i].column_scale, (SQLPOINTER)df->Char_buf->data[df->Char_buf->addcols-1][0], 0, df->ind[i]);
      SQLBindParameter(hstmt, i+1, SQL_PARAM_INPUT, df->ColumnInfo[i].column_type,
      SQL_CHAR, df->ColumnInfo[i].column_size, df->ColumnInfo[i].column_scale, (SQLPOINTER)df->Char_buf->data[df->Char_buf->addcols-1][1], value_len+1, df->ind[i]);
      printf("SQL_C_CHAR SUCCESS \n" );
			break;

		case SQL_C_WCHAR:
					if(df->Wchar_buf == NULL){
					df->Wchar_buf =(wchar_buf*)malloc(sizeof(wchar_buf));
					df->Wchar_buf->addcols=1;
					int y;
					df->Wchar_buf->data = (wchar_t***) malloc(sizeof(wchar_t**) * df->Wchar_buf->addcols);
					memset(df->Wchar_buf->data, 0x00, sizeof(wchar_t**)*df->Wchar_buf->addcols);
					df->Wchar_buf->data[0]= (wchar_t**)malloc(df->Nrow * sizeof(wchar_t*));
					memset(df->Wchar_buf->data[0], 0x00, df->Nrow * sizeof(wchar_t*));
					for(y=0; y<df->Nrow; y++)
					df->Wchar_buf->data[0][y] = (wchar_t*)malloc(sizeof(wchar_t)*df->ColumnInfo[i].column_size);
					}else{
					df->Wchar_buf->addcols++;
					int y;
					df->Wchar_buf->data = (wchar_t***) realloc(df->Wchar_buf->data, sizeof(wchar_t**) * df->Wchar_buf->addcols);
					df->Wchar_buf->data[df->Wchar_buf->addcols-1] =(wchar_t**)malloc(df->Nrow * sizeof(wchar_t*));
					memset(df->Wchar_buf->data[df->Wchar_buf->addcols-1], 0x00, df->Nrow * sizeof(wchar_t*));
					for(y=0; y<df->Nrow; y++)
					df->Wchar_buf->data[df->Wchar_buf->addcols-1][y] = (wchar_t*)malloc(sizeof(wchar_t)*df->ColumnInfo[i].column_size);
					}

					for (j=0; j <df->Nrow; j++){
						wcsncpy(df->Wchar_buf->data[df->Wchar_buf->addcols-1][j], df->Data[j][i], df->ColumnInfo[i].column_size);
						//printf("%s \n", df->Char_buf->data[df->Char_buf->addcols-1][j]);
						//printf("%d \n", wcslen(df->Char_buf->data[df->Char_buf->addcols-1][j]));
						df->ind[i][j] = wcslen(df->Wchar_buf->data[df->Wchar_buf->addcols-1][j]);
					}

					SQLBindParameter(hstmt, i+1, SQL_PARAM_INPUT, df->ColumnInfo[i].column_type,
					SQL_WCHAR, df->ColumnInfo[i].column_size, df->ColumnInfo[i].column_scale, (SQLPOINTER)df->Wchar_buf->data[df->Wchar_buf->addcols-1][0], 0, df->ind[i]);
					printf("SQL_C_WCHAR SUCCESS \n" );
					break;
    //case SQL_C_TYPE_TIMESTAMP:
    //case SQL_C_DATE:
    //case SQL_C_TIME:
			}
		}
		strcat(Statement,df->TableName);
		strcat(Statement," (");
		strcat(Statement,Col_Names);
		strcat(Statement,") VALUES (");
		strcat(Statement,Questions);
		strcat(Statement,")");

		printf("%s \n", Statement);
		retcode = SQLExecDirect(hstmt, (SQLCHAR*)Statement, SQL_NTS);
		printf("Success Bulk Operations \n");
		DF_Memfree(df);

    exit:
        SQLCloseCursor(hstmt);
        SQLFreeHandle(SQL_HANDLE_STMT, hstmt);
        return;
	}
