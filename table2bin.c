/* 
 *      Export a table to a binary file 
 */
#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <string.h>
#include "libpq-fe.h"

static void exit_nicely(PGconn *conn) {
    PQfinish(conn);
    exit(1);
}

int main(int argc, char **argv) {
    const char *conninfo;
    PGconn     *conn;
    PGresult   *res;
    int         nFields;
    int         i, 
                j;
    char *tableName;

    /*     
     Need to pass tableName as an argument, databasename is an optional argument
    */  
    
    switch(argc) {
        case 2:
            conninfo = "dbname = postgres";
            tableName = argv[1];
            break;
        case 3:
            conninfo = argv[2];
            tableName = argv[1];
            break;
        default:
            printf("Default db: postgres, default table: dataset\n");
            conninfo = "dbname = huy";
            tableName = "dataset";
            break;
    }
    /* Make a connection to the database */
    conn = PQconnectdb(conninfo);

    /* Check to see that the backend connection was successfully made */
    if (PQstatus(conn) != CONNECTION_OK) {
        fprintf(stderr, "Connection to database failed: %s",
                PQerrorMessage(conn));
        exit_nicely(conn);
    }

    // clock_t start = clock();
    
    /* Start a transaction block */
    res = PQexec(conn, "BEGIN");
    if (PQresultStatus(res) != PGRES_COMMAND_OK) {
        fprintf(stderr, "BEGIN command failed: %s", PQerrorMessage(conn));
        PQclear(res);
        exit_nicely(conn);
    }
    PQclear(res);
    time_t start = time(NULL);  
    //Fetching rows 
    char querryStr[100];    
    strcat(querryStr, "DECLARE test BINARY CURSOR FOR SELECT * FROM ");
    strcat(querryStr, tableName);
    // strcat(querryStr, " LIMIT(20)");
    res = PQexec(conn, querryStr);
    if (PQresultStatus(res) != PGRES_COMMAND_OK) {
        fprintf(stderr, "DECLARE CURSOR FAIL: %s", PQerrorMessage(conn));
        PQclear(res);
        exit_nicely(conn);
    }
    PQclear(res);

    res = PQexec(conn, "FETCH ALL in test");
    if (PQresultStatus(res) != PGRES_TUPLES_OK) {
        fprintf(stderr, "FETCH ALL failed: %s", PQerrorMessage(conn));
        PQclear(res);
        exit_nicely(conn);
    }
    time_t end = time(NULL);
    int timeOfExe = end - start;
    printf("Time of fetching data to memory: %i seconds\n", timeOfExe);
    /* first, print out the attribute names */
    nFields = PQnfields(res);
    // for (i = 0; i < nFields; i++)
        // printf("%-15s", PQfname(res, i));
    // printf("\n");

    // Then the size in bytes of each column
    // printf("Num of columns: %d\n", nFields);
    // printf("Size of nFields: %lu\n", sizeof(nFields));
    
    int *sizeArr = (int*)malloc(nFields*sizeof(int));
    int currentSize;
    for (i = 0; i < nFields; i++) {
        // printf("%-15d", PQfsize(res, i));
        currentSize = PQfsize(res, i);
        sizeArr[i] = currentSize;        
    }
    
    unsigned long numRows = PQntuples(res);    
    // printf("%lu\n", numRows);
    FILE *fout = fopen("out.bin", "w+");
    
    // Printing HEADER
    // This part write to the header in following order:
    // number of rows, number of columns, length of each column (the loop)
    // fwrite(&numRows, sizeof(numRows), 1, fout);
    // fwrite(&nFields, sizeof(nFields), 1, fout);
    // int currentColumnSize;
    // for (i = 0; i < nFields; i++) {
    //     currentColumnSize = PQfsize(res, i);
    //     fwrite(&currentColumnSize, sizeof(currentColumnSize), 1, fout);
    // }
    start = time(NULL); // Timing for exprting binary file
    // Then write the values
    for (i = 0; i < numRows; i++) {
        for (j = 0; j < nFields; j++) {
            // printf("%-15s", PQgetvalue(res, i, j));
            // currentValue = PQgetvalue(res, i, j);
            // printf("%-15s", PQgetvalue(res, i, j));
            fwrite(PQgetvalue(res, i, j), sizeArr[j], 1, fout);
        }
        // printf("\n");
    }
    end = time(NULL);

    PQclear(res);

    //close the portal ... 
    res = PQexec(conn, "CLOSE myportal");
    PQclear(res);

    /* end the transaction */
    res = PQexec(conn, "END");
    PQclear(res);

    // clock_t end = clock();
    
    // int timediff = end - start;
    // int timeOfExe = timediff * 1000 / CLOCKS_PER_SEC;
    timeOfExe = end - start;
    // printf("Time of exe: %f seconds\n", timeOfExe/1000.0);
    printf("Time of export to file: %i seconds\n", timeOfExe);
    /* close the connection to the database and cleanup */
    PQfinish(conn);

    return 0;
}
