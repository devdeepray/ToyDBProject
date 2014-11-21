/* testpf.c */
#include <stdio.h>
#include "pf.h"
#include "pftypes.h"
#define FILE1	"file1"
#define FILE2	"file2"

writeTableHeader(int n_tuples, int n_attributes, int *attribute_lengths, int *buf){
    buf[0] = n_tuples;
//    buf[1] = n_attributes;
//    int i=0;
//    for(;i<n_attributes; i++){
//        buf[i + 2] = attribute_lengths[i];
//    }
}

writeRecord(char* record, int offset, int length, char* buf){
    int pos = 0;
    for(pos = 0;pos < length; ++pos){
        buf[offset + pos] = record[pos];
    }
}

readRecord(char* record, int offset, int length, char* buf){
    int pos = 0;
    for(pos = 0;pos < length; ++pos){
        record[pos] = buf[offset + pos];
    }
}




struct Record
{
    char name[20];
    char phone[10];
    int dd;
    int mm;
    int yy;
};




main()
{

int bufmode;
scanf("%d", &bufmode);
setBufMode(bufmode);
int error;
int i;
int pagenum,*buf;
int *buf1,*buf2;
int fd1,fd2;

    printf("1: Create table 2: select from table 3: join two tables\n");
    int choice;
    char filename[20];
    scanf("%d", &choice);
    printf("Enter file name\n");
    scanf("%s", filename);

    switch(choice)
    {
    case 1:
        if ((error=PF_CreateFile(filename))!= PFE_OK){
            PF_PrintError("asfsad");
            exit(1);
        }
        printf("Enter number of pages\n");
        scanf("%d", &choice);
        writefile(filename, choice);
        break;
    case 2:
        selectByName(filename, "DBInternals");
        break;
    case 3:
        break;
    default:
        break;
    }

PF_printcounts();

}


/************************************************************
Open the File.
allocate as many pages in the file as the buffer
manager would allow, and write the page number
into the data.
then, close file.
******************************************************************/
writefile(char *fname, int n_pages)
{

    struct Record r;
    strcpy(r.name, "DBInternals");
    strcpy(r.phone, "9831929192");
    r.dd = 10;
    r.mm = 11;
    r.yy = 12;
    int page_hdr = 4;
    int n_tuppp = (PF_PAGE_SIZE - page_hdr) / sizeof(r);
    int i;
    int fd,pagenum;
    int *buf;
    int error;

	/* open file1, and allocate a few pages in there */
	if ((fd=PF_OpenFile(fname))<0){
		PF_PrintError("open file1");
		exit(1);
	}
	printf("opened %s\n",fname);

	for (i=0; i < n_pages; i++){
		if ((error=PF_AllocPage(fd,&pagenum,&buf))!= PFE_OK){
			PF_PrintError("first buffer\n");
			exit(1);
		}
        buf[0] = n_tuppp;
        int j;
        for( j = 0; j < n_tuppp; ++j){
            writeRecord(&r, page_hdr + j * sizeof(r), sizeof(r), buf);
        }


		printf("allocated page %d\n",pagenum);
		if ((error=PF_UnfixPage(fd,i,TRUE))!= PFE_OK){
			PF_PrintError("unfix buffer\n");
			exit(1);
		}
		PF_PrintError("unfixed page successfully\n");
	}
	/* close the file */
	if ((error=PF_CloseFile(fd))!= PFE_OK){
		PF_PrintError("close file1\n");
		exit(1);
	}

}

/**************************************************************
print the content of file
*************************************************************/

selectByName(char* fname, char* name)
{


    int page_hdr = 4;
    int i;
    int fd,pagenum;
    int *buf;
    int error;



    printf("opening %s\n",fname);
	if ((fd=PF_OpenFile(fname))<0){
		PF_PrintError("open file");
		exit(1);
	}

	pagenum = -1;
	while ((error=PF_GetNextPage(fd,&pagenum,&buf))== PFE_OK){

        struct Record r;
        int n_tuppp = buf[0];
        int i = 0;
        for(; i < n_tuppp; ++i)
        {
            readRecord(&r, page_hdr + i * sizeof(r), sizeof(r), buf);
            if(strcmp(r.name, name)){
                // Do something
                PF_PrintError(":)");
            }
        }
		if ((error=PF_UnfixPage(fd,pagenum,FALSE))!= PFE_OK){
			PF_PrintError("unfix");
			exit(1);
		}
	}



	if ((error=PF_CloseFile(fd))!= PFE_OK){
		PF_PrintError("close file");
		exit(1);
	}

}


readfile(fname)
char *fname;
{
int error;
int *buf;
int pagenum;
int fd;

	printf("opening %s\n",fname);
	if ((fd=PF_OpenFile(fname))<0){
		PF_PrintError("open file");
		exit(1);
	}
	printfile(fd);
	if ((error=PF_CloseFile(fd))!= PFE_OK){
		PF_PrintError("close file");
		exit(1);
	}
}

printfile(fd)
int fd;
{
int error;
int *buf;
int pagenum;

	printf("reading file\n");
	pagenum = -1;
	while ((error=PF_GetNextPage(fd,&pagenum,&buf))== PFE_OK){
		printf("got page %d, %d\n",pagenum,*buf);
		if ((error=PF_UnfixPage(fd,pagenum,FALSE))!= PFE_OK){
			PF_PrintError("unfix");
			exit(1);
		}
	}
	if (error != PFE_EOF){
		PF_PrintError("not eof\n");
		exit(1);
	}
	printf("eof reached\n");

}

