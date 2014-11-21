

writeRecord(char* record, int offset, int length, char* buf){
    int pos = 0;
    for(pos = 0;pos < length; ++pos){
        buf[offset + pos] = record[pos];
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
